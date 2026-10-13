package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestNewMoneyParsesFixedPrecision(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{input: "0.00", want: 0},
		{input: "25.08", want: 2508},
		{input: "92233720368547758.07", want: math.MaxInt64},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := NewMoney(test.input, "BRL")
			if err != nil {
				t.Fatalf("NewMoney() error = %v", err)
			}
			if got.Amount() != test.want {
				t.Fatalf("amount = %d, want %d", got.Amount(), test.want)
			}
			if got.String() != test.input {
				t.Fatalf("String() = %q, want %q", got.String(), test.input)
			}
		})
	}
}

func TestNewMoneyRejectsInvalidInputs(t *testing.T) {
	for _, input := range []string{"", "1", "1.0", "1.000", "-1.00", "1e2", "NaN", "92233720368547758.08"} {
		t.Run(input, func(t *testing.T) {
			if _, err := NewMoney(input, "BRL"); !errors.Is(err, ErrInvalidMoney) {
				t.Fatalf("NewMoney(%q) error = %v, want ErrInvalidMoney", input, err)
			}
		})
	}
	if _, err := NewMoney("1.00", "brl"); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("lowercase currency error = %v, want ErrInvalidMoney", err)
	}
	if _, err := NewMoney("1.00", "ZZZ"); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("unknown currency error = %v, want ErrInvalidMoney", err)
	}
}

func TestMoneyArithmetic(t *testing.T) {
	a := NewMoneyFromInt64(math.MaxInt64, "BRL")
	if _, err := a.Add(NewMoneyFromInt64(1, "BRL")); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("Add overflow error = %v", err)
	}
	if _, err := NewMoneyFromInt64(math.MinInt64, "BRL").Negate(); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("Negate overflow error = %v", err)
	}
	if _, err := NewMoneyFromInt64(math.MaxInt64, "BRL").Subtract(NewMoneyFromInt64(-1, "BRL")); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("Subtract overflow error = %v", err)
	}
	if _, err := NewMoneyFromInt64(math.MinInt64, "BRL").Add(NewMoneyFromInt64(-1, "BRL")); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("Add underflow error = %v", err)
	}
	if _, err := NewMoneyFromInt64(100, "BRL").Add(NewMoneyFromInt64(100, "USD")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency mismatch error = %v", err)
	}
	got, err := NewMoneyFromInt64(-123, "BRL").Add(NewMoneyFromInt64(23, "BRL"))
	if err != nil || got.String() != "-1.00" {
		t.Fatalf("negative arithmetic = %s, %v; want -1.00", got, err)
	}
	comparison, err := NewMoneyFromInt64(200, "BRL").Compare(NewMoneyFromInt64(100, "BRL"))
	if err != nil || comparison != 1 {
		t.Fatalf("Compare() = %d, %v; want 1", comparison, err)
	}
}

func TestMoneyJSONUsesDecimalStrings(t *testing.T) {
	var money Money
	if err := json.Unmarshal([]byte(`{"amount":"25.08","currency":"BRL"}`), &money); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	data, err := json.Marshal(money)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if string(data) != `{"amount":"25.08","currency":"BRL"}` {
		t.Fatalf("JSON = %s", data)
	}
	minimum := NewMoneyFromInt64(math.MinInt64, "BRL")
	if minimum.String() != "-92233720368547758.08" {
		t.Fatalf("minimum int64 serialized as %q", minimum.String())
	}
}
