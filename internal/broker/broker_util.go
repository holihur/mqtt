package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"mqtt/internal/auth"
)

func storeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

func bgCtx() context.Context {
	return context.Background()
}

func loadTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	cfg.GetCertificate = func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		return &cfg.Certificates[0], nil
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

// authorizeOnly adapts an authorization component (e.g. FileACL) for AND
// composition: it contributes only its Authorize decision and is neutral on
// Authenticate, so a Chain of [credentials..., authorizeOnly(acl)] means
// "valid credentials AND ACL-allowed topic".
type authorizeOnly struct{ inner auth.Authenticator }

func (a authorizeOnly) Authenticate(_, _ string, _ []byte) bool { return true }
func (a authorizeOnly) Authorize(c, t string, p bool) bool      { return a.inner.Authorize(c, t, p) }

func buildAuthenticator(cfg Config) (auth.Authenticator, error) {
	var chain []auth.Authenticator
	if cfg.JWTSecret != "" {
		chain = append(chain, &auth.JWTAuth{Secret: cfg.JWTSecret})
	}
	if cfg.ACLFile != "" {
		acl, err := auth.NewFileACL(cfg.ACLFile)
		if err != nil {
			// fail closed: a requested ACL that cannot be loaded must abort
			// startup instead of silently degrading to an open broker
			return nil, fmt.Errorf("load acl file %s: %w", cfg.ACLFile, err)
		}
		chain = append(chain, authorizeOnly{acl})
	}
	if len(chain) == 0 {
		if cfg.AllowAnonymous {
			return &auth.AllowAll{}, nil
		}
		return &auth.DenyAll{}, nil
	}
	if cfg.JWTSecret == "" {
		// an ACL file alone is authorization-only: without an explicit
		// AllowAnonymous opt-in there is no way to authenticate, so fail closed
		if !cfg.AllowAnonymous {
			return &auth.DenyAll{}, nil
		}
		return &auth.Chain{Auths: append([]auth.Authenticator{&auth.AllowAll{}}, chain...)}, nil
	}
	if len(chain) == 1 {
		return chain[0], nil
	}
	return &auth.Chain{Auths: chain}, nil
}
