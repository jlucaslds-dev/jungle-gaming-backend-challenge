package http

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

type principalKey struct{}

type Principal struct {
	ProviderID string
	ClientID   string
}

type Authenticator struct {
	verifier     *oidc.IDTokenVerifier
	internalApps map[string]struct{}
}

type tokenClaims struct {
	AuthorizedParty string `json:"azp"`
	ClientID        string `json:"client_id"`
	ProviderID      string `json:"provider_id"`
}

func NewAuthenticator(ctx context.Context) (*Authenticator, error) {
	issuer := strings.TrimSpace(os.Getenv("OIDC_ISSUER_URL"))
	audience := strings.TrimSpace(os.Getenv("OIDC_AUDIENCE"))
	internalClients := strings.TrimSpace(os.Getenv("INTERNAL_CLIENT_IDS"))
	if issuer == "" || audience == "" || internalClients == "" {
		return nil, &configurationError{message: "OIDC_ISSUER_URL, OIDC_AUDIENCE e INTERNAL_CLIENT_IDS são obrigatórios"}
	}
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	provider, err := oidc.NewProvider(startupCtx, issuer)
	if err != nil {
		return nil, err
	}
	internal := make(map[string]struct{})
	for _, client := range strings.Split(internalClients, ",") {
		if client = strings.TrimSpace(client); client != "" {
			internal[client] = struct{}{}
		}
	}
	if len(internal) == 0 {
		return nil, &configurationError{message: "INTERNAL_CLIENT_IDS deve conter ao menos um client id"}
	}
	return &Authenticator{
		verifier:     provider.Verifier(&oidc.Config{ClientID: audience}),
		internalApps: internal,
	}, nil
}

type configurationError struct{ message string }

func (e *configurationError) Error() string { return e.message }

func (a *Authenticator) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := strings.Fields(r.Header.Get("Authorization"))
		if len(auth) != 2 || !strings.EqualFold(auth[0], "Bearer") {
			writeAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "bearer token obrigatório")
			return
		}
		token, err := a.verifier.Verify(r.Context(), auth[1])
		if err != nil {
			writeAPIError(w, http.StatusUnauthorized, "INVALID_TOKEN", "token inválido ou expirado")
			return
		}
		var claims tokenClaims
		if err := token.Claims(&claims); err != nil {
			writeAPIError(w, http.StatusUnauthorized, "INVALID_TOKEN", "claims do token inválidos")
			return
		}
		clientID := claims.AuthorizedParty
		if clientID == "" {
			clientID = claims.ClientID
		}
		p := Principal{ProviderID: claims.ProviderID, ClientID: clientID}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

func (a *Authenticator) RequireProvider(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := principalFromContext(r.Context())
		if !ok || p.ProviderID == "" {
			writeAPIError(w, http.StatusForbidden, "PROVIDER_IDENTITY_REQUIRED", "o token não está associado a um provedor")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *Authenticator) RequireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := principalFromContext(r.Context())
		if !ok {
			writeAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "autenticação obrigatória")
			return
		}
		if _, allowed := a.internalApps[p.ClientID]; !allowed {
			writeAPIError(w, http.StatusForbidden, "FORBIDDEN", "operação reservada a serviços internos")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func principalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
