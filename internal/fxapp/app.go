package fxapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	deliveryhttp "github.com/jlucaslds-dev/backend-challenge-go/internal/delivery/http"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/delivery/worker"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/observability"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/usecase"
	"go.uber.org/fx"
)

func NewRouter(handler *deliveryhttp.Handler, auth *deliveryhttp.Authenticator) *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/health/live", handler.Live)
	r.Get("/health/ready", handler.Ready)
	r.HandleFunc("/metrics", observability.Handler)

	r.Group(func(internal chi.Router) {
		internal.Use(auth.Authenticate)
		internal.Use(auth.RequireInternal)
		internal.Post("/wallets", handler.CreateWallet)
		internal.Get("/wallets/{walletId}", handler.GetWallet)
		internal.Get("/wallets/{walletId}/ledger", handler.GetLedger)
		internal.Post("/wallets/{walletId}/reconciliation", handler.ReconcileWallet)
	})
	r.Group(func(provider chi.Router) {
		provider.Use(auth.Authenticate)
		provider.Use(auth.RequireProvider)
		provider.Post("/wagering/transactions", handler.ProcessWager)
		provider.Get("/wagering/transactions/{transactionId}", handler.GetTransaction)
		provider.Get("/providers/{providerId}/wagering/transactions/{externalTransactionId}", handler.GetProviderTransaction)
	})
	return r
}

func NewHTTPServer(lc fx.Lifecycle, router *chi.Mux) *http.Server {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{
		Addr:              net.JoinHostPort("", port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	var listener net.Listener
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var err error
			listener, err = net.Listen("tcp", srv.Addr)
			if err != nil {
				return fmt.Errorf("listen HTTP on %s: %w", srv.Addr, err)
			}
			go func() {
				if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
					slog.Error("HTTP server stopped unexpectedly", "error", err)
				}
			}()
			slog.Info("HTTP server listening", "address", srv.Addr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
	return srv
}

func ProvideDB(lc fx.Lifecycle, ctx context.Context) (*postgres.DB, error) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/jungle_gaming?sslmode=disable"
	}
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	db, err := postgres.NewDB(startupCtx, connStr)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		db.Close()
		return nil
	}})
	return db, nil
}

func ProvideSQSReadiness(client *worker.Client) deliveryhttp.DependencyChecker {
	return client
}

func CreateApp() *fx.App {
	return fx.New(AppOptions())
}

func AppOptions() fx.Option {
	return fx.Options(
		fx.Provide(context.Background),
		fx.Module("infrastructure",
			fx.Provide(ProvideDB, worker.NewClient, ProvideSQSReadiness, deliveryhttp.NewAuthenticator),
		),
		fx.Module("postgres",
			fx.Provide(
				postgres.NewTxManager,
				postgres.NewWalletRepository,
				postgres.NewTransactionRepository,
				postgres.NewLedgerRepository,
				postgres.NewOutboxRepository,
				postgres.NewInboxRepository,
			),
		),
		fx.Module("usecases",
			fx.Provide(usecase.NewWalletUseCase, usecase.NewWageringUseCase),
		),
		fx.Module("delivery",
			fx.Provide(
				deliveryhttp.NewHandler,
				NewRouter,
				NewHTTPServer,
				worker.NewWagerConsumer,
				worker.NewOutboxPublisher,
				worker.NewReferenceResolver,
			),
		),
		fx.Invoke(func(*http.Server, *worker.WagerConsumer, *worker.OutboxPublisher, *worker.ReferenceResolver) {}),
	)
}
