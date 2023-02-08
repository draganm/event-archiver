package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/draganm/event-buffer/client"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/gofrs/uuid"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

var eventsPath = dbpath.ToPath("events")

func main() {
	logger, _ := zap.Config{
		Encoding:    "json",
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths: []string{"stdout"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:   "message",
			LevelKey:     "level",
			EncodeLevel:  zapcore.CapitalLevelEncoder,
			TimeKey:      "time",
			EncodeTime:   zapcore.ISO8601TimeEncoder,
			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}.Build()

	defer logger.Sync()
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:    "batch-duration",
				EnvVars: []string{"BATCH_DURATION"},
				Value:   6 * time.Hour,
			},
			&cli.StringFlag{
				Name:     "event-buffer-base-url",
				EnvVars:  []string{"EVENT_BUFFER_BASE_URL"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "state-file",
				Value:   "state",
				EnvVars: []string{"STATE_FILE"},
			},
			&cli.StringFlag{
				Name:    "archive-dir",
				Value:   "archive",
				EnvVars: []string{"ARCHIVE_DIR"},
			},
			&cli.StringFlag{
				Name:     "s3-bucket-name",
				EnvVars:  []string{"S3_BUCKET_NAME"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "s3-key-prefix",
				EnvVars: []string{"S3_KEY_PREFIX"},
				Value:   "events",
			},
		},
		Action: func(c *cli.Context) error {
			log := zapr.NewLogger(logger)
			defer log.Info("archiver exiting")

			archiveDir := c.String("archive-dir")
			log = log.WithValues("archiveDir", archiveDir)

			// create archive dir if it doesn't exist
			_, err := os.Stat(archiveDir)
			switch {
			case os.IsNotExist(err):
				err = os.MkdirAll(archiveDir, 0700)
				if err != nil {
					return fmt.Errorf("could not crate archive dir: %w", err)
				}
			case err != nil:
				return fmt.Errorf("could not stat archive dir: %w", err)
			}

			cl, err := client.New(c.String("event-buffer-base-url"))
			if err != nil {
				return fmt.Errorf("could not open event buffer client: %w", err)
			}

			awsSession := session.Must(session.NewSession())
			uploader := s3manager.NewUploader(awsSession)
			s3BucketName := c.String("s3-bucket-name")
			s3KeyPrefix := c.String("s3-key-prefix")

			// archiveFiles := []string{}
			s3Client := s3.New(awsSession)

			lastIDFromS3 := ""

			hasMorePages := true
			var continuationToken *string

			for hasMorePages {

				list, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
					Bucket:            aws.String(s3BucketName),
					Prefix:            &s3KeyPrefix,
					ContinuationToken: continuationToken,
					// Delimiter:         aws.String("/"),
				})

				if err != nil {
					return fmt.Errorf("could not list s3 objects")
				}

				for _, c := range list.Contents {
					// archiveFiles = append(archiveFiles, aws.StringValue(c.Key))

					key := aws.StringValue(c.Key)

					lastIDFromKey, err := objectKeyToLastID(key)
					if err != nil {
						return fmt.Errorf("could not parse object name %s: %w", key, err)
					}

					if lastIDFromS3 < lastIDFromKey {
						lastIDFromS3 = lastIDFromKey
					}

				}

				hasMorePages = aws.BoolValue(list.IsTruncated)

				if hasMorePages {
					continuationToken = list.ContinuationToken
				}
			}

			log.Info("archiveFiles", "lastID", lastIDFromS3)

			db, err := embedded.Open(c.String("state-file"), 0700, embedded.Options{})
			if err != nil {
				return fmt.Errorf("could not open state file: %w", err)
			}

			err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
				if !tx.Exists(eventsPath) {
					tx.CreateMap(eventsPath)
				}
				return nil
			})

			if err != nil {
				return fmt.Errorf("could not initialize the database: %w", err)
			}

			if lastIDFromS3 != "" {
				err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
					it := tx.Iterator(eventsPath)
					it.Seek(lastIDFromS3)
					if it.IsDone() {
						return nil
					}

					toDelete := []string{}

					for ; !it.IsDone(); it.Prev() {
						toDelete = append(toDelete, it.GetKey())
					}

					for _, id := range toDelete {
						tx.Delete(eventsPath.Append(id))
					}

					return nil
				})

			}

			if err != nil {
				return fmt.Errorf("could not initialize the database: %w", err)
			}

			batchDuration := c.Duration("batch-duration")

			eg, ctx := errgroup.WithContext(context.Background())

			// signal handler
			eg.Go(func() error {
				sigChan := make(chan os.Signal, 1)
				signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
				select {
				case sig := <-sigChan:
					log.Info("received signal", "signal", sig.String())
					return fmt.Errorf("received signal %s", sig.String())
				case <-ctx.Done():
					return nil
				}
			})

			eg.Go(func() error {
				for ctx.Err() == nil {
					var firstID, lastID string

					err := bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
						it := tx.Iterator(eventsPath)
						if it.IsDone() {
							return nil
						}

						firstID = it.GetKey()

						it.Last()
						if it.IsDone() {
							return nil
						}

						lastID = it.GetKey()
						return nil
					})

					if err != nil {
						return fmt.Errorf("could not determine first and last id: %w", err)
					}

					// TODO: if either first or last ID is empty, we need more events

					if firstID != "" || lastID != "" {
						firsTime, err := timeOfID(firstID)
						if err != nil {
							return fmt.Errorf("could not get time from id %s: %w", firstID, err)
						}

						lastTime, err := timeOfID(lastID)
						if err != nil {
							return fmt.Errorf("could not get time from id %s: %w", lastID, err)
						}

						if lastTime.Sub(firsTime) > batchDuration {
							log.Info("batch full, archiving")
							endTime := firsTime.Add(batchDuration)

							err = archiveToS3(
								ctx,
								log,
								db,
								endTime,
								filepath.Join(archiveDir, fmt.Sprintf("%s.json.gz", firstID)),
								uploader,
								s3BucketName,
								s3KeyPrefix,
							)
							if err != nil {
								log.Error(err, "failed to archive")
								time.Sleep(1 * time.Second)
							}
						}

						// TODO: see if the archive file should be created
					}

					var events = []json.RawMessage{}
					ids, err := cl.PollForEvents(ctx, lastID, 1000, &events)
					if err != nil {
						log.Error(err, "failed to poll for events, backing off")
						time.Sleep(1 * time.Second)
						continue
					}

					err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
						for i, id := range ids {
							tx.Put(eventsPath.Append(id), events[i])
						}
						return nil
					})

					if err != nil {
						return fmt.Errorf("could not store events: %w", err)
					}

				}
				return nil
			})

			return eg.Wait()

		},
	}
	app.RunAndExitOnError()
}

func objectKeyToLastID(key string) (string, error) {
	parts := strings.Split(key, "/")
	if len(parts) < 1 {
		return "", fmt.Errorf("split found no parts")
	}

	lastPart := parts[len(parts)-1]

	if !strings.HasSuffix(lastPart, ".json.gz") {
		return "", fmt.Errorf("key %s has no .json.gz suffix", key)
	}

	withoutPrefix := strings.TrimSuffix(lastPart, ".json.gz")

	ids := strings.Split(withoutPrefix, ".")

	if len(ids) != 2 {
		return "", fmt.Errorf("expected 2 IDs, found %d", len(ids))
	}

	return ids[1], nil

}

func archiveToS3(
	ctx context.Context,
	log logr.Logger,
	db bolted.Database,
	endTime time.Time,
	archiveFile string,
	uploader *s3manager.Uploader,
	bucketName, keyPrefix string,
) (err error) {

	log = log.WithValues("archiveFile", archiveFile)

	defer func() {
		if err != nil {
			os.Remove(archiveFile)
		}
	}()
	f, err := os.OpenFile(archiveFile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("could not open archive file: %w", err)
	}
	defer f.Close()

	gw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return fmt.Errorf("could not create gzip writer: %w", err)
	}

	defer func() {
		if err == nil {
			err = gw.Close()
		}
	}()

	enc := json.NewEncoder(gw)

	written := []string{}

	err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {

		for it := tx.Iterator(eventsPath); !it.IsDone(); it.Next() {
			evTime, err := timeOfID(it.GetKey())
			if err != nil {
				return fmt.Errorf("could not get time of id %s: %w", it.GetKey(), err)
			}
			if evTime.After(endTime) {
				break
			}
			ev := []any{it.GetKey(), json.RawMessage(it.GetValue())}
			enc.Encode(ev)
			written = append(written, it.GetKey())
		}

		log.Info("archived events to file", "count", len(written))
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed writing events to file: %w", err)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("could not sync archive file: %w", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("could not seek archive file to start: %w", err)
	}

	firsID := written[0]
	lastID := written[len(written)-1]

	// Upload the file to S3.
	result, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path.Join(keyPrefix, fmt.Sprintf("%s.%s.json.gz", firsID, lastID))),
		Body:   f,
	})

	if err != nil {
		return fmt.Errorf("could not upload archive to s3: %w", err)
	}

	log.Info("archived events", "until", endTime, "s3Location", result.Location, "s3Version", result.VersionID)

	err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {

		for _, k := range written {
			tx.Delete(eventsPath.Append(k))
		}

		log.Info("deleted events from file", "count", len(written))
		return nil
	})

	return nil
}

func timeOfID(idString string) (time.Time, error) {
	id, err := uuid.FromString(idString)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse uuid: %w", err)
	}

	ts, err := uuid.TimestampFromV6(id)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not get uuidv6 timestamp: %w", err)
	}

	t, err := ts.Time()
	if err != nil {
		return time.Time{}, fmt.Errorf("could not get error from timestamp: %w", err)
	}
	return t, nil

}
