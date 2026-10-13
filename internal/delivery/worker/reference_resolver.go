package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jlucaslds-dev/backend-challenge-go/internal/usecase"
	"go.uber.org/fx"
)

type ReferenceResolver struct {
	uc     *usecase.WageringUseCase
	cancel context.CancelFunc
	done   chan struct{}
}

func NewReferenceResolver(lc fx.Lifecycle, uc *usecase.WageringUseCase) *ReferenceResolver {
	resolver := &ReferenceResolver{uc: uc}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			resolver.cancel = cancel
			resolver.done = make(chan struct{})
			go resolver.run(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if resolver.cancel == nil {
				return nil
			}
			resolver.cancel()
			select {
			case <-resolver.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return resolver
}

func (r *ReferenceResolver) run(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := r.uc.ResolvePendingReferences(ctx); err != nil && ctx.Err() == nil {
			slog.Error("pending reference resolution failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
