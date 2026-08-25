package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"log/slog"
	"mqtt/internal/auth"
)

func storeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

//nolint:govet

func bgCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = cancel
	return ctx
}

func loadTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
	}
	// preload to validate
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, err
	}
	if caFile != "" {
		caData, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("failed to parse CA %s", caFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func buildAuthenticator(cfg Config) auth.Authenticator {
	var chain []auth.Authenticator
	if cfg.JWTSecret != "" {
		chain = append(chain, &auth.JWTAuth{Secret: cfg.JWTSecret})
	}
	if cfg.ACLFile != "" {
		if acl, err := auth.NewFileACL(cfg.ACLFile); err == nil {
			chain = append(chain, acl)
		} else {
			slog.Warn("acl file load failed", "file", cfg.ACLFile, "err", err)
		}
	}
	if len(chain) == 0 {
		if cfg.AllowAnonymous {
			return &auth.AllowAll{}
		}
		return &auth.DenyAll{}
	}
	if len(chain) == 1 {
		return chain[0]
	}
	return &auth.Chain{Auths: chain}
}
