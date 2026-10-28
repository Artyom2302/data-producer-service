package platform

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunSchedulerCallsFnUntilCancelled(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	var calls atomic.Int32
	fn := func(context.Context) error {
		calls.Add(1)
		return errors.New("boom")
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		RunScheduler(ctx, "test", 10*time.Millisecond, time.Second, log, fn)
		close(done)
	}()

	time.Sleep(55 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunScheduler did not return after ctx was cancelled")
	}

	if n := calls.Load(); n < 3 {
		t.Errorf("fn called %d times, want at least 3", n)
	}
	if !strings.Contains(buf.String(), `msg="job failed" job=test error=boom`) {
		t.Errorf("log does not contain the job error:\n%s", buf.String())
	}
}

func TestRunSchedulerLimitsIterationByTimeout(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	iterErr := make(chan error, 1)
	fn := func(ctx context.Context) error {
		<-ctx.Done()
		select {
		case iterErr <- ctx.Err():
		default:
		}
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go RunScheduler(ctx, "test", 10*time.Millisecond, 20*time.Millisecond, log, fn)

	select {
	case err := <-iterErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("iteration ctx.Err() = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("iteration was not stopped by the timeout")
	}
}

func TestRunSchedulerLetsStartedIterationFinish(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	started := make(chan struct{}, 1)
	iterErr := make(chan error, 1)
	var calls atomic.Int32
	fn := func(ctx context.Context) error {
		if calls.Add(1) > 1 {
			t.Error("a new iteration started after ctx was cancelled")
			return nil
		}
		started <- struct{}{}
		time.Sleep(50 * time.Millisecond)
		iterErr <- ctx.Err()
		return nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		RunScheduler(ctx, "test", 10*time.Millisecond, time.Second, log, fn)
		close(done)
	}()

	<-started
	cancel()

	select {
	case err := <-iterErr:
		if err != nil {
			t.Errorf("iteration ctx.Err() = %v after the parent was cancelled, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("iteration did not finish")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunScheduler did not return after the iteration finished")
	}
}
