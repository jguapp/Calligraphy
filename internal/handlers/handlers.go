// Package handlers assembles Calligraphy's built-in handler set. The API
// imports this too -- not to execute anything, but so submission can
// validate job types against the same registry the workers run.
package handlers

import (
	"github.com/jguapp/calligraphy/internal/handler"
	"github.com/jguapp/calligraphy/internal/handlers/articleanalysis"
	"github.com/jguapp/calligraphy/internal/handlers/bench"
	"github.com/jguapp/calligraphy/internal/handlers/httpcallback"
)

// Config carries the pieces individual handlers need injected.
type Config struct {
	CallbackSecret       string
	CallbackAllowedHosts []string
}

// NewRegistry builds the full built-in registry.
func NewRegistry(cfg Config) *handler.Registry {
	r := handler.NewRegistry()
	r.MustRegister(articleanalysis.New())
	r.MustRegister(httpcallback.New(cfg.CallbackSecret, cfg.CallbackAllowedHosts))
	bench.RegisterAll(r)
	return r
}
