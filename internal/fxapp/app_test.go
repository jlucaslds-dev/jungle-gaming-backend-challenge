package fxapp

import (
	"testing"

	"go.uber.org/fx"
)

func TestFxCompositionGraph(t *testing.T) {
	if err := fx.ValidateApp(AppOptions()); err != nil {
		t.Fatalf("Fx graph is invalid: %v", err)
	}
}
