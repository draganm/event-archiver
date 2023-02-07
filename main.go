package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/draganm/event-buffer/client"
	"github.com/go-logr/zapr"
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
		},
		Action: func(c *cli.Context) error {
			log := zapr.NewLogger(logger)
			defer log.Info("archiver exiting")

			cl, err := client.New(c.String("event-buffer-base-url"))
			if err != nil {
				return fmt.Errorf("could not open event buffer client: %w", err)
			}

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
						// TODO: see if the archive file should be created
					}

					var events = []json.RawMessage{}
					ids, err := cl.PollForEvents(ctx, lastID, 2000, &events)
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
