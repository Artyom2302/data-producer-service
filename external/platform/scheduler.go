package platform

import (
	"context"
	"log/slog"
	"time"
)

func RunScheduler(ctx context.Context, name string,
	interval time.Duration,
	timeout time.Duration,
	log *slog.Logger,
	fn func(ctx context.Context) error,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("job stopped", "job", name)
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				log.Info("job stopped", "job", name)
				return
			}
			iterCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			if err := fn(iterCtx); err != nil {
				log.Error("job failed", "job", name, "error", err)
			}
			cancel()
		}
	}
}
