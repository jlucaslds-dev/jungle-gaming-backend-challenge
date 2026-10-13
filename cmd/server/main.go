package main

import (
	"log/slog"
	"os"

	"github.com/jlucaslds-dev/backend-challenge-go/internal/fxapp"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	app := fxapp.CreateApp()
	app.Run()
}
