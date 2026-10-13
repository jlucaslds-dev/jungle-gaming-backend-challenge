package domain

import (
	"errors"
	"time"
)

type TransactionType string

const (
	TypeOpening  TransactionType = "OPENING"
	TypeBet      TransactionType = "BET"
	TypeWin      TransactionType = "WIN"
	TypeLoss     TransactionType = "LOSS"
	TypeRefund   TransactionType = "REFUND"
	TypeRollback TransactionType = "ROLLBACK"
)

type TransactionStatus string

const (
	StatusPending          TransactionStatus = "PENDING"
	StatusPendingReference TransactionStatus = "PENDING_REFERENCE"
	StatusProcessed        TransactionStatus = "PROCESSED"
	StatusRejected         TransactionStatus = "REJECTED"
	StatusFailed           TransactionStatus = "FAILED"
)

type WagerTransaction struct {
	id             string
	walletID       string
	externalID     string
	providerID     string
	idempotencyKey string
	payloadHash    string
	playerID       string
	roundID        string
	gameID         string
	referenceID    string
	failureCode    string
	resultBalance  Money
	amount         Money
	txType         TransactionType
	status         TransactionStatus
	createdAt      time.Time
	updatedAt      time.Time
}

func NewWagerTransaction(id, walletID, externalID string, amount Money, txType TransactionType) (WagerTransaction, error) {
	if id == "" {
		return WagerTransaction{}, errors.New("o id da transação é obrigatório")
	}
	if walletID == "" {
		return WagerTransaction{}, errors.New("o id da carteira é obrigatório")
	}
	if externalID == "" {
		return WagerTransaction{}, errors.New("o id externo é obrigatório")
	}
	if err := amount.Validate(); err != nil {
		return WagerTransaction{}, err
	}

	// LOSS exige obrigatoriamente montante zero; as restantes exigem montante positivo
	switch txType {
	case TypeLoss:
		if !amount.IsZero() {
			return WagerTransaction{}, errors.New("o montante da transação LOSS deve ser zero")
		}
	case TypeBet, TypeWin, TypeRefund, TypeRollback, TypeOpening:
		if amount.IsZero() || amount.IsNegative() {
			return WagerTransaction{}, errors.New("o montante da transação deve ser positivo")
		}
	default:
		return WagerTransaction{}, ErrInvalidTransaction
	}

	now := time.Now().UTC()
	return WagerTransaction{
		id:         id,
		walletID:   walletID,
		externalID: externalID,
		amount:     amount,
		txType:     txType,
		status:     StatusPending,
		createdAt:  now,
		updatedAt:  now,
	}, nil
}

func NewOpeningTransaction(id, walletID, playerID string, amount Money) (WagerTransaction, error) {
	if id == "" || walletID == "" || playerID == "" || amount.IsNegative() || amount.IsZero() || amount.Validate() != nil {
		return WagerTransaction{}, ErrInvalidTransaction
	}
	now := time.Now().UTC()
	return WagerTransaction{
		id: id, walletID: walletID, playerID: playerID, amount: amount,
		txType: TypeOpening, status: StatusPending, createdAt: now, updatedAt: now,
	}, nil
}

func RehydrateWagerTransaction(id, walletID, externalID string, amount Money, txType TransactionType, status TransactionStatus, createdAt, updatedAt time.Time) WagerTransaction {
	return WagerTransaction{
		id:         id,
		walletID:   walletID,
		externalID: externalID,
		amount:     amount,
		txType:     txType,
		status:     status,
		createdAt:  createdAt,
		updatedAt:  updatedAt,
	}
}

type WagerMetadata struct {
	ProviderID     string
	IdempotencyKey string
	PayloadHash    string
	PlayerID       string
	RoundID        string
	GameID         string
	ReferenceID    string
}

func NewExternalWagerTransaction(id, walletID, externalID string, amount Money, txType TransactionType, metadata WagerMetadata) (WagerTransaction, error) {
	t, err := NewWagerTransaction(id, walletID, externalID, amount, txType)
	if err != nil {
		return WagerTransaction{}, err
	}
	if metadata.ProviderID == "" || metadata.IdempotencyKey == "" || len(metadata.PayloadHash) != 64 ||
		metadata.PlayerID == "" || metadata.RoundID == "" || metadata.GameID == "" {
		return WagerTransaction{}, errors.New("metadados externos obrigatórios ausentes")
	}
	t.providerID = metadata.ProviderID
	t.idempotencyKey = metadata.IdempotencyKey
	t.payloadHash = metadata.PayloadHash
	t.playerID = metadata.PlayerID
	t.roundID = metadata.RoundID
	t.gameID = metadata.GameID
	t.referenceID = metadata.ReferenceID
	return t, nil
}

func RehydrateExternalWagerTransaction(id, walletID, externalID string, amount Money, txType TransactionType, status TransactionStatus, metadata WagerMetadata, failureCode string, resultBalance Money, createdAt, updatedAt time.Time) (WagerTransaction, error) {
	t, err := NewExternalWagerTransaction(id, walletID, externalID, amount, txType, metadata)
	if err != nil {
		return WagerTransaction{}, err
	}
	t.status = status
	t.failureCode = failureCode
	t.resultBalance = resultBalance
	t.createdAt = createdAt
	t.updatedAt = updatedAt
	return t, nil
}

func (t *WagerTransaction) ID() string                     { return t.id }
func (t *WagerTransaction) WalletID() string               { return t.walletID }
func (t *WagerTransaction) ExternalID() string             { return t.externalID }
func (t *WagerTransaction) ProviderID() string             { return t.providerID }
func (t *WagerTransaction) IdempotencyKey() string         { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string            { return t.payloadHash }
func (t *WagerTransaction) PlayerID() string               { return t.playerID }
func (t *WagerTransaction) RoundID() string                { return t.roundID }
func (t *WagerTransaction) GameID() string                 { return t.gameID }
func (t *WagerTransaction) ReferenceID() string            { return t.referenceID }
func (t *WagerTransaction) FailureCode() string            { return t.failureCode }
func (t *WagerTransaction) SetFailureCode(code string)     { t.failureCode = code }
func (t *WagerTransaction) ResultBalance() Money           { return t.resultBalance }
func (t *WagerTransaction) SetResultBalance(balance Money) { t.resultBalance = balance }
func (t *WagerTransaction) Amount() Money                  { return t.amount }
func (t *WagerTransaction) Type() TransactionType          { return t.txType }
func (t *WagerTransaction) Status() TransactionStatus      { return t.status }
func (t *WagerTransaction) CreatedAt() time.Time           { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time           { return t.updatedAt }

func (t *WagerTransaction) Complete() error {
	if t.status != StatusPending && t.status != StatusPendingReference {
		return errors.New("apenas transações pendentes podem ser completadas")
	}
	t.status = StatusProcessed
	t.updatedAt = time.Now().UTC()
	return nil
}

func (t *WagerTransaction) MarkPendingReference() error {
	if t.status != StatusPending {
		return errors.New("apenas transações pendentes podem aguardar referência")
	}
	t.status = StatusPendingReference
	t.updatedAt = time.Now().UTC()
	return nil
}

func (t *WagerTransaction) Reject(code string) error {
	if t.status != StatusPending && t.status != StatusPendingReference {
		return errors.New("apenas transações pendentes podem ser rejeitadas")
	}
	if code == "" {
		return errors.New("código de rejeição obrigatório")
	}
	t.status = StatusRejected
	t.failureCode = code
	t.updatedAt = time.Now().UTC()
	return nil
}

func (t *WagerTransaction) Fail(code string) error {
	if t.status != StatusPending && t.status != StatusPendingReference {
		return errors.New("apenas transações pendentes podem falhar permanentemente")
	}
	if code == "" {
		return errors.New("código de falha obrigatório")
	}
	t.status = StatusFailed
	t.failureCode = code
	t.updatedAt = time.Now().UTC()
	return nil
}
