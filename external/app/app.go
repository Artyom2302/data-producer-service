package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"data-producer-service/external/adapter/kafka"
	"data-producer-service/external/adapter/postgres"
	"data-producer-service/external/adapter/producer"
	"data-producer-service/external/application/schedule"
	"data-producer-service/external/application/send"
	"data-producer-service/external/config"
	"data-producer-service/external/platform"
	"data-producer-service/migrations"
)

func Run(ctx context.Context, cfg *config.Config) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if err := postgres.Migrate(ctx, cfg.DB.Dsn, migrations.MigrationFiles); err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}
	storage, err := postgres.NewPgStorage(ctx, cfg.DB.Dsn)
	if err != nil {
		return fmt.Errorf("create postgres storage: %w", err)
	}
	defer storage.Close()
	sender, err := kafka.NewRawEventSender(ctx, cfg.Kafka.Url, cfg.Kafka.Topic)
	if err != nil {
		return fmt.Errorf("create raw event sender: %w", err)
	}
	defer sender.Close()

	packageProducer, err := producer.NewRawPackageProducer(cfg.Scheduler.RandomNumberCount)
	if err != nil {
		return fmt.Errorf("create raw package producer: %w", err)
	}
	scheduleUC := schedule.NewUseCase(packageProducer, storage)
	sendUC, err := send.NewUseCase(cfg.Scheduler.SendLimit, sender, storage)
	if err != nil {
		return fmt.Errorf("create send use case: %w", err)
	}

	sendJob := func(ctx context.Context) error {

		for {
			n, err := sendUC.Send(ctx)
			if n > 0 {
				logger.Info("packages sent", "count", n)
			}
			if err != nil {
				return err
			}
			if n < cfg.Scheduler.SendLimit {
				return nil
			}
		}
	}

	logger.Info("service started")
	var wg sync.WaitGroup
	wg.Go(func() {
		platform.RunScheduler(ctx, "schedule", cfg.Scheduler.ScheduleInterval, cfg.Scheduler.IterationTimeout, logger, scheduleUC.Schedule)
	})
	wg.Go(func() {
		platform.RunScheduler(ctx, "send", cfg.Scheduler.SendInterval, cfg.Scheduler.IterationTimeout, logger, sendJob)
	})
	wg.Wait()
	logger.Info("service stopped")
	return nil
}
