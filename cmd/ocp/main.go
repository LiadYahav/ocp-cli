package main

import (
	"fmt"
	"os"

	"github.com/liadyahav/ocp-cli/internal/cli"
)

func main() {
	app := cli.New()

	if err := app.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

