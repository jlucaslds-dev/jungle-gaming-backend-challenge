package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	textcurrency "golang.org/x/text/currency"
)

var (
	ErrInvalidMoney       = errors.New("invalid money")
	ErrCurrencyMismatch   = errors.New("currency mismatch")
	ErrMoneyOverflow      = errors.New("money overflow")
	ErrInsufficientFunds  = errors.New("insufficient funds")
	ErrInvalidTransaction = errors.New("invalid transaction")
)

// Money stores minor units (two decimal places) without floating-point arithmetic.
type Money struct {
	amount   int64
	currency string
}

func NewMoney(amount string, currency string) (Money, error) {
	if err := ValidateCurrency(currency); err != nil {
		return Money{}, err
	}
	if len(amount) < 4 || amount[0] == '-' || amount[0] == '+' {
		return Money{}, fmt.Errorf("%w: amount must be a non-negative decimal with two fractional digits", ErrInvalidMoney)
	}
	whole, fraction, ok := strings.Cut(amount, ".")
	if !ok || whole == "" || len(fraction) != 2 || strings.ContainsAny(whole, "+-eE") {
		return Money{}, fmt.Errorf("%w: amount must use fixed two-decimal notation", ErrInvalidMoney)
	}
	for _, part := range []string{whole, fraction} {
		for _, c := range part {
			if c < '0' || c > '9' {
				return Money{}, fmt.Errorf("%w: amount contains a non-digit", ErrInvalidMoney)
			}
		}
	}
	wholeAmount, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || wholeAmount > math.MaxInt64/100 {
		return Money{}, fmt.Errorf("%w: amount is out of range", ErrInvalidMoney)
	}
	minor := wholeAmount * 100
	fractionAmount := int64(fraction[0]-'0')*10 + int64(fraction[1]-'0')
	if minor > math.MaxInt64-fractionAmount {
		return Money{}, fmt.Errorf("%w: amount is out of range", ErrInvalidMoney)
	}
	return Money{amount: minor + fractionAmount, currency: currency}, nil
}

func ValidateCurrency(currency string) error {
	if len(currency) != 3 || strings.ToUpper(currency) != currency {
		return fmt.Errorf("%w: currency must be a three-letter uppercase ISO code", ErrInvalidMoney)
	}
	for _, c := range currency {
		if c < 'A' || c > 'Z' {
			return fmt.Errorf("%w: currency must be a three-letter uppercase ISO code", ErrInvalidMoney)
		}
	}
	if _, err := textcurrency.ParseISO(currency); err != nil {
		return fmt.Errorf("%w: unknown ISO 4217 currency", ErrInvalidMoney)
	}
	return nil
}

func (m Money) Validate() error {
	if err := ValidateCurrency(m.currency); err != nil {
		return err
	}
	return nil
}

func NewMoneyFromInt64(amount int64, currency string) Money {
	return Money{amount: amount, currency: currency}
}

func (m Money) Amount() int64    { return m.amount }
func (m Money) Currency() string { return m.currency }
func (m Money) IsZero() bool     { return m.amount == 0 }
func (m Money) IsNegative() bool { return m.amount < 0 }

func (m Money) Add(other Money) (Money, error) {
	if err := m.checkCurrency(other); err != nil {
		return Money{}, err
	}
	if (other.amount > 0 && m.amount > math.MaxInt64-other.amount) ||
		(other.amount < 0 && m.amount < math.MinInt64-other.amount) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amount: m.amount + other.amount, currency: m.currency}, nil
}

func (m Money) Subtract(other Money) (Money, error) {
	if err := m.checkCurrency(other); err != nil {
		return Money{}, err
	}
	if (other.amount < 0 && m.amount > math.MaxInt64+other.amount) ||
		(other.amount > 0 && m.amount < math.MinInt64+other.amount) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amount: m.amount - other.amount, currency: m.currency}, nil
}

func (m Money) Negate() (Money, error) {
	if m.amount == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amount: -m.amount, currency: m.currency}, nil
}

func (m Money) Compare(other Money) (int, error) {
	if err := m.checkCurrency(other); err != nil {
		return 0, err
	}
	switch {
	case m.amount < other.amount:
		return -1, nil
	case m.amount > other.amount:
		return 1, nil
	default:
		return 0, nil
	}
}

func (m Money) String() string {
	sign := ""
	magnitude := uint64(m.amount)
	if m.amount < 0 {
		sign = "-"
		magnitude = uint64(-(m.amount + 1)) + 1
	}
	return fmt.Sprintf("%s%d.%02d", sign, magnitude/100, magnitude%100)
}

func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{Amount: m.String(), Currency: m.currency})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	var wire struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMoney, err)
	}
	if wire.Amount == "" || wire.Currency == "" {
		return fmt.Errorf("%w: amount and currency are required", ErrInvalidMoney)
	}
	if wire.Amount[0] == '-' {
		parsed, err := parseSignedMoney(wire.Amount, wire.Currency)
		if err != nil {
			return err
		}
		*m = parsed
		return nil
	}
	parsed, err := NewMoney(wire.Amount, wire.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

func parseSignedMoney(amount, currency string) (Money, error) {
	positive, err := NewMoney(strings.TrimPrefix(amount, "-"), currency)
	if err != nil {
		return Money{}, err
	}
	return positive.Negate()
}

func (m Money) checkCurrency(other Money) error {
	if m.currency == "" || other.currency == "" || m.currency != other.currency {
		return ErrCurrencyMismatch
	}
	return nil
}
