package auth

import (
	"bufio"
	"crypto/subtle"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Authenticator interface {
	Authenticate(clientID, username string, password []byte) bool
	Authorize(clientID, topic string, isPublish bool) bool
}

type AllowAll struct{}

func (a *AllowAll) Authenticate(_, _ string, _ []byte) bool { return true }
func (a *AllowAll) Authorize(_, _ string, _ bool) bool      { return true }

type DenyAll struct{}

func (d *DenyAll) Authenticate(_, _ string, _ []byte) bool { return false }
func (d *DenyAll) Authorize(_, _ string, _ bool) bool      { return false }

type SimpleAuth struct {
	Users map[string]string   // username -> password
	ACL   map[string][]string // clientID -> allowed topic prefixes
}

func (s *SimpleAuth) Authenticate(_, username string, password []byte) bool {
	if len(s.Users) == 0 {
		// no users configured = nobody may authenticate (fail-closed);
		// open deployments must opt in explicitly via AllowAll
		return false
	}
	expect, ok := s.Users[username]
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expect), password) == 1
}
func (s *SimpleAuth) Authorize(clientID, topic string, _ bool) bool {
	if len(s.ACL) == 0 {
		return true
	}
	allowed, ok := s.ACL[clientID]
	if !ok {
		return true
	}
	for _, p := range allowed {
		if p == "#" || p == topic || (len(p) > 0 && topicHasPrefix(topic, p)) {
			return true
		}
	}
	return false
}

type JWTAuth struct {
	Secret string
	AllowAll
}

func (j *JWTAuth) Authenticate(clientID, username string, password []byte) bool {
	if j.Secret == "" {
		return false
	}
	tokenStr := string(password)
	if tokenStr == "" {
		tokenStr = username
	}
	if tokenStr == "" {
		return false
	}
	// try parse as JWT, if not JWT try fallback to AllowAll
	if !strings.Contains(tokenStr, ".") {
		return false
	}
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"HS256", "HS384", "HS512"}), jwt.WithExpirationRequired())
	token, err := parser.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return []byte(j.Secret), nil
	})
	if err != nil || !token.Valid {
		return false
	}
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if exp, ok := claims["exp"].(float64); ok {
			if int64(exp) < time.Now().Unix() {
				return false
			}
		}
		if cid, ok := claims["client_id"].(string); ok && cid != "" && cid != clientID {
			return false
		}
	}
	return true
}

type FileACL struct {
	mu    sync.RWMutex
	rules []aclRule
	path  string
	mtime time.Time
}

// Authenticate always fails: an ACL file grants topic permissions, it is not
// a credential source. Composing code must pair it with a real authenticator
// (or an explicit AllowAnonymous opt-in).
func (f *FileACL) Authenticate(_, _ string, _ []byte) bool { return false }

type aclRule struct {
	Username string
	ClientID string
	Topic    string
	Access   string // read, write, readwrite
}

func NewFileACL(path string) (*FileACL, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rules []aclRule
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		var r aclRule
		for i := 0; i < len(parts); i++ {
			switch parts[i] {
			case "user":
				if i+1 < len(parts) {
					r.Username = parts[i+1]
					i++
				}
			case "client":
				if i+1 < len(parts) {
					r.ClientID = parts[i+1]
					i++
				}
			case "topic":
				if i+1 < len(parts) {
					r.Topic = parts[i+1]
					i++
				}
			case "read", "write", "readwrite":
				r.Access = parts[i]
			}
		}
		if r.Topic != "" {
			if r.Access == "" {
				r.Access = "readwrite"
			}
			rules = append(rules, r)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	info, _ := os.Stat(path)
	var mtime time.Time
	if info != nil {
		mtime = info.ModTime()
	}
	return &FileACL{rules: rules, path: path, mtime: mtime}, nil
}

func (f *FileACL) Reload() (bool, error) {
	if f.path == "" {
		return false, nil
	}
	info, err := os.Stat(f.path)
	if err != nil {
		return false, err
	}
	if !info.ModTime().After(f.mtime) {
		return false, nil
	}
	newACL, err := NewFileACL(f.path)
	if err != nil {
		return false, err
	}
	f.mu.Lock()
	f.rules = newACL.rules
	f.mtime = newACL.mtime
	n := len(f.rules)
	f.mu.Unlock()
	// caller logs INFO
	_ = n
	return true, nil
}

func (f *FileACL) Authorize(clientID, topic string, isPublish bool) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.rules) == 0 {
		return true
	}
	for _, r := range f.rules {
		if r.ClientID != "" && r.ClientID != clientID {
			continue
		}
		// username check would need context, skip for now
		need := "read"
		if isPublish {
			need = "write"
		}
		if r.Access != "readwrite" && r.Access != need {
			continue
		}
		if r.Topic == "#" || r.Topic == topic || topicHasPrefix(topic, r.Topic) || matchMqttFilter(topic, r.Topic) {
			return true
		}
	}
	return false
}

func matchMqttFilter(topic, filter string) bool {
	// reuse topic matching via simple split
	tParts := strings.Split(topic, "/")
	fParts := strings.Split(filter, "/")
	for i, fp := range fParts {
		if fp == "#" {
			return i == len(fParts)-1
		}
		if fp == "+" {
			if i >= len(tParts) {
				return false
			}
			continue
		}
		if i >= len(tParts) || tParts[i] != fp {
			return false
		}
	}
	return len(tParts) == len(fParts)
}

func topicHasPrefix(topic, prefix string) bool {
	if prefix == "#" {
		return true
	}
	if len(prefix) >= 2 && prefix[len(prefix)-2:] == "/#" {
		base := prefix[:len(prefix)-2]
		return topic == base || (len(topic) > len(base) && topic[:len(base)+1] == base+"/")
	}
	return topic == prefix
}

type Chain struct {
	Auths []Authenticator
}

func (c *Chain) Authenticate(clientID, username string, password []byte) bool {
	for _, a := range c.Auths {
		if !a.Authenticate(clientID, username, password) {
			return false
		}
	}
	return true
}
func (c *Chain) Authorize(clientID, topic string, isPublish bool) bool {
	for _, a := range c.Auths {
		if !a.Authorize(clientID, topic, isPublish) {
			return false
		}
	}
	return true
}
