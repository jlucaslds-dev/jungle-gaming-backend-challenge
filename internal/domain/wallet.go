package domain

import (
	"errors"
	"time"
)

type Wallet struct {
	id        string
	playerId  string
	balance   Money
	version   int
	createdAt time.Time
	updatedAt time.Time
}

// NewWallet cria uma nova carteira com saldo inicial e versão 1
func NewWallet(id, playerId string, initialBalance Money) (Wallet, error) {
	if id == "" {
		return Wallet{}, errors.New("id da carteira não pode ser vazio")
	}
	if playerId == "" {
		return Wallet{}, errors.New("id do jogador não pode ser vazio")
	}
	if len(playerId) > 100 {
		return Wallet{}, errors.New("id do jogador excede o limite de 100 caracteres")
	}
	if initialBalance.Validate() != nil || initialBalance.IsNegative() {
		return Wallet{}, errors.New("saldo inicial inválido")
	}

	now := time.Now().UTC()
	return Wallet{
		id:        id,
		playerId:  playerId,
		balance:   initialBalance,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// RehydrateWallet reconstrói a carteira a partir do banco de dados sem alterar versão ou emitir eventos
func RehydrateWallet(id, playerId string, balance Money, version int, createdAt, updatedAt time.Time) Wallet {
	return Wallet{
		id:        id,
		playerId:  playerId,
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}
}

func (w *Wallet) ID() string {
	return w.id
}

func (w *Wallet) PlayerID() string {
	return w.playerId
}

func (w *Wallet) Balance() Money {
	return w.balance
}

func (w *Wallet) Version() int {
	return w.version
}

// Debit subtrai um valor da carteira, garantindo saldo não negativo e incrementando a versão
func (w *Wallet) Debit(amount Money) error {
	if amount.currency == "" || amount.IsNegative() || amount.IsZero() {
		return errors.New("o débito deve ser positivo e possuir moeda")
	}
	if amount.Currency() != w.balance.Currency() {
		return ErrCurrencyMismatch
	}
	if amount.Amount() > w.balance.Amount() {
		return ErrInsufficientFunds
	}
	newBalance, err := w.balance.Subtract(amount)
	if err != nil {
		return err
	}
	w.balance = newBalance
	w.version++
	w.updatedAt = time.Now().UTC()
	return nil
}

// Credit adiciona um valor à carteira e incrementa a versão
func (w *Wallet) Credit(amount Money) error {
	if amount.currency == "" || amount.IsNegative() || amount.IsZero() {
		return errors.New("o crédito deve ser positivo e possuir moeda")
	}
	if amount.Currency() != w.balance.Currency() {
		return ErrCurrencyMismatch
	}
	newBalance, err := w.balance.Add(amount)
	if err != nil {
		return err
	}

	w.balance = newBalance
	w.version++
	w.updatedAt = time.Now().UTC()
	return nil
}
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }
