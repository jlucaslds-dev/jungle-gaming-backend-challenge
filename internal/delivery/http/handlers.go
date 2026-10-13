package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/usecase"
)

type Handler struct {
	walletUC   *usecase.WalletUseCase
	wageringUC *usecase.WageringUseCase
	db         *postgres.DB
	sqsReady   DependencyChecker
}

type DependencyChecker interface {
	Check(context.Context) error
}

func NewHandler(walletUC *usecase.WalletUseCase, wageringUC *usecase.WageringUseCase, db *postgres.DB, sqsReady DependencyChecker) *Handler {
	return &Handler{walletUC: walletUC, wageringUC: wageringUC, db: db, sqsReady: sqsReady}
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type walletRequest struct {
	PlayerID       string       `json:"playerId"`
	InitialBalance domain.Money `json:"initialBalance"`
}

type walletResponse struct {
	ID       string       `json:"id"`
	PlayerID string       `json:"playerId"`
	Balance  domain.Money `json:"balance"`
	Version  int          `json:"version"`
}

type wagerRequest struct {
	ProviderID  string       `json:"providerId"`
	ExternalID  string       `json:"externalTransactionId"`
	PlayerID    string       `json:"playerId"`
	WalletID    string       `json:"walletId"`
	RoundID     string       `json:"roundId"`
	GameID      string       `json:"gameId"`
	Type        string       `json:"kind"`
	Money       domain.Money `json:"money"`
	ReferenceID string       `json:"referenceExternalTransactionId,omitempty"`
}

type wagerResponse struct {
	TransactionID    string                   `json:"transactionId"`
	Status           domain.TransactionStatus `json:"status"`
	Balance          domain.Money             `json:"balance"`
	IdempotentReplay bool                     `json:"idempotentReplay"`
	FailureCode      string                   `json:"failureCode,omitempty"`
}

type ledgerEntryResponse struct {
	ID            string       `json:"id"`
	TransactionID string       `json:"transactionId"`
	Direction     string       `json:"direction"`
	Amount        domain.Money `json:"amount"`
	BalanceBefore domain.Money `json:"balanceBefore"`
	BalanceAfter  domain.Money `json:"balanceAfter"`
	CreatedAt     time.Time    `json:"createdAt"`
}

type ledgerResponse struct {
	WalletID string                `json:"walletId"`
	Entries  []ledgerEntryResponse `json:"entries"`
	Next     string                `json:"nextCursor,omitempty"`
}

type reconciliationResponse struct {
	WalletID          string       `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

func (h *Handler) CreateWallet(w http.ResponseWriter, r *http.Request) {
	var req walletRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	if req.PlayerID == "" || req.InitialBalance.Currency() == "" || req.InitialBalance.IsNegative() {
		writeAPIError(w, http.StatusBadRequest, "INVALID_REQUEST", "playerId e initialBalance são obrigatórios")
		return
	}
	wallet, err := h.walletUC.CreateWallet(r.Context(), req.PlayerID, req.InitialBalance)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			writeAPIError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", "já existe carteira para este jogador e moeda")
			return
		}
		slog.Error("wallet creation failed", "requestId", middleware.GetReqID(r.Context()), "error", err)
		writeAPIError(w, http.StatusServiceUnavailable, "PERSISTENCE_UNAVAILABLE", "não foi possível criar a carteira")
		return
	}
	writeJSON(w, http.StatusCreated, walletResponse{
		ID: wallet.ID(), PlayerID: wallet.PlayerID(), Balance: wallet.Balance(), Version: wallet.Version(),
	})
}

func (h *Handler) ProcessWager(w http.ResponseWriter, r *http.Request) {
	var req wagerRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	principal, ok := principalFromContext(r.Context())
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "autenticação obrigatória")
		return
	}
	if req.ProviderID != "" && req.ProviderID != principal.ProviderID {
		writeAPIError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId não corresponde à identidade autenticada")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if strings.TrimSpace(key) == "" {
		writeAPIError(w, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "header Idempotency-Key obrigatório")
		return
	}
	result, err := h.wageringUC.ProcessWager(r.Context(), usecase.ProcessWagerInput{
		ProviderID: principal.ProviderID, IdempotencyKey: key, PlayerID: req.PlayerID,
		WalletID: req.WalletID, ExternalID: req.ExternalID, RoundID: req.RoundID,
		GameID: req.GameID, Amount: req.Money.String(), Currency: req.Money.Currency(),
		Type: domain.TransactionType(req.Type), ReferenceID: req.ReferenceID,
	})
	if err != nil {
		switch {
		case errors.Is(err, usecase.ErrIdempotencyConflict):
			writeAPIError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "chave ou transação externa já utilizada com conteúdo diferente")
		case errors.Is(err, usecase.ErrInvalidWager), errors.Is(err, domain.ErrInvalidMoney):
			writeAPIError(w, http.StatusBadRequest, "INVALID_WAGER", err.Error())
		case errors.Is(err, postgres.ErrWalletNotFound):
			writeAPIError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "carteira não encontrada")
		default:
			slog.Error("wager processing failed", "requestId", middleware.GetReqID(r.Context()),
				"providerId", principal.ProviderID, "externalTransactionId", req.ExternalID, "walletId", req.WalletID, "error", err)
			writeAPIError(w, http.StatusServiceUnavailable, "PROCESSING_UNAVAILABLE", "falha transitória ao processar a operação")
		}
		return
	}
	statusCode := http.StatusOK
	switch result.Transaction.Status() {
	case domain.StatusPendingReference:
		statusCode = http.StatusAccepted
	case domain.StatusRejected:
		statusCode = http.StatusUnprocessableEntity
	case domain.StatusFailed:
		statusCode = http.StatusServiceUnavailable
	}
	writeJSON(w, statusCode, wagerResponse{
		TransactionID: result.Transaction.ID(), Status: result.Transaction.Status(),
		Balance: result.Balance, IdempotentReplay: result.IdempotentReplay,
		FailureCode: result.Transaction.FailureCode(),
	})
}

func (h *Handler) GetWallet(w http.ResponseWriter, r *http.Request) {
	wallet, err := h.walletUC.GetWallet(r.Context(), chi.URLParam(r, "walletId"))
	if err != nil {
		if errors.Is(err, postgres.ErrWalletNotFound) {
			writeAPIError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "carteira não encontrada")
			return
		}
		slog.Error("wallet lookup failed", "requestId", middleware.GetReqID(r.Context()), "walletId", chi.URLParam(r, "walletId"), "error", err)
		writeAPIError(w, http.StatusServiceUnavailable, "PERSISTENCE_UNAVAILABLE", "consulta indisponível")
		return
	}
	writeJSON(w, http.StatusOK, walletResponse{
		ID: wallet.ID(), PlayerID: wallet.PlayerID(), Balance: wallet.Balance(), Version: wallet.Version(),
	})
}

func (h *Handler) GetLedger(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 100 {
			writeAPIError(w, http.StatusBadRequest, "INVALID_LIMIT", "limit deve estar entre 1 e 100")
			return
		}
		limit = parsed
	}
	var beforeTime *time.Time
	var beforeID string
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "INVALID_CURSOR", "cursor inválido")
			return
		}
		parts := strings.SplitN(string(decoded), "|", 2)
		if len(parts) != 2 {
			writeAPIError(w, http.StatusBadRequest, "INVALID_CURSOR", "cursor inválido")
			return
		}
		timestamp, err := time.Parse(time.RFC3339Nano, parts[0])
		if err != nil || parts[1] == "" {
			writeAPIError(w, http.StatusBadRequest, "INVALID_CURSOR", "cursor inválido")
			return
		}
		beforeTime, beforeID = &timestamp, parts[1]
	}
	walletID := chi.URLParam(r, "walletId")
	if _, err := h.walletUC.GetWallet(r.Context(), walletID); err != nil {
		if errors.Is(err, postgres.ErrWalletNotFound) {
			writeAPIError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "carteira não encontrada")
			return
		}
		writeAPIError(w, http.StatusServiceUnavailable, "PERSISTENCE_UNAVAILABLE", "consulta indisponível")
		return
	}
	entries, err := h.walletUC.ListLedger(r.Context(), walletID, beforeTime, beforeID, limit+1)
	if err != nil {
		slog.Error("ledger lookup failed", "requestId", middleware.GetReqID(r.Context()), "walletId", walletID, "error", err)
		writeAPIError(w, http.StatusServiceUnavailable, "PERSISTENCE_UNAVAILABLE", "consulta indisponível")
		return
	}
	response := ledgerResponse{WalletID: walletID, Entries: make([]ledgerEntryResponse, 0, limit)}
	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}
	for _, entry := range entries {
		direction := "CREDIT"
		if entry.Amount().IsNegative() {
			direction = "DEBIT"
		}
		response.Entries = append(response.Entries, ledgerEntryResponse{
			ID: entry.ID(), TransactionID: entry.TransactionID(), Direction: direction, Amount: entry.Amount(),
			BalanceBefore: entry.BalanceBefore(), BalanceAfter: entry.BalanceAfter(), CreatedAt: entry.CreatedAt(),
		})
	}
	if hasMore {
		last := entries[len(entries)-1]
		response.Next = base64.RawURLEncoding.EncodeToString([]byte(last.CreatedAt().UTC().Format(time.RFC3339Nano) + "|" + last.ID()))
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) ReconcileWallet(w http.ResponseWriter, r *http.Request) {
	result, err := h.walletUC.Reconcile(r.Context(), chi.URLParam(r, "walletId"))
	if err != nil {
		if errors.Is(err, postgres.ErrWalletNotFound) {
			writeAPIError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "carteira não encontrada")
			return
		}
		slog.Error("wallet reconciliation failed", "requestId", middleware.GetReqID(r.Context()),
			"walletId", chi.URLParam(r, "walletId"), "error", err)
		writeAPIError(w, http.StatusServiceUnavailable, "PERSISTENCE_UNAVAILABLE", "reconciliação indisponível")
		return
	}
	if !result.Consistent {
		slog.Error("wallet ledger mismatch", "requestId", middleware.GetReqID(r.Context()),
			"walletId", result.WalletID, "difference", result.Difference.String())
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: result.WalletID, StoredBalance: result.StoredBalance,
		CalculatedBalance: result.CalculatedBalance, Difference: result.Difference,
		Consistent: result.Consistent, CheckedEntries: result.CheckedEntries,
	})
}

func (h *Handler) GetTransaction(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalFromContext(r.Context())
	transaction, err := h.wageringUC.GetTransaction(r.Context(), chi.URLParam(r, "transactionId"))
	if err != nil {
		writeLookupError(w, r, err)
		return
	}
	if transaction.ProviderID() != principal.ProviderID {
		writeAPIError(w, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transação não encontrada")
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse(transaction))
}

func (h *Handler) GetProviderTransaction(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalFromContext(r.Context())
	providerID := chi.URLParam(r, "providerId")
	if providerID != principal.ProviderID {
		writeAPIError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "provedor não autorizado")
		return
	}
	transaction, err := h.wageringUC.GetProviderTransaction(r.Context(), providerID, chi.URLParam(r, "externalTransactionId"))
	if err != nil {
		writeLookupError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse(transaction))
}

func transactionResponse(transaction domain.WagerTransaction) wagerResponse {
	return wagerResponse{
		TransactionID: transaction.ID(), Status: transaction.Status(),
		Balance: transaction.ResultBalance(), FailureCode: transaction.FailureCode(),
	}
}

func writeLookupError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		writeAPIError(w, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transação não encontrada")
		return
	}
	slog.Error("transaction lookup failed", "requestId", middleware.GetReqID(r.Context()),
		"transactionId", chi.URLParam(r, "transactionId"),
		"externalTransactionId", chi.URLParam(r, "externalTransactionId"), "error", err)
	writeAPIError(w, http.StatusServiceUnavailable, "PERSISTENCE_UNAVAILABLE", "consulta indisponível")
}

func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
}

func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Pool.Ping(r.Context()); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "NOT_READY", "PostgreSQL indisponível")
		return
	}
	if err := h.sqsReady.Check(r.Context()); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "NOT_READY", "SQS indisponível")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("corpo da requisição inválido: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("corpo deve conter exatamente um objeto JSON")
	}
	return nil
}

func writeAPIError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{Code: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("failed to encode HTTP response", "error", err)
	}
}
