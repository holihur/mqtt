package auth

import (
	"bufio"
	"os"
	"strings"
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

type SimpleAuth struct {
	Users map[string]string   // username -> password
	ACL   map[string][]string // clientID -> allowed topic prefixes
}

func (s *SimpleAuth) Authenticate(_, username string, password []byte) bool {
	if len(s.Users) == 0 {
		return true
	}
	expect, ok := s.Users[username]
	if !ok {
		return false
	}
	return expect == string(password)
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
		return true
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
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"HS256", "HS384", "HS512"}))
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
	AllowAll
	rules []aclRule
}

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
		// format: user <username> topic <topic> <access>
		// or: client <clientID> topic <topic> <access>
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
	return &FileACL{rules: rules}, sc.Err()
}

func (f *FileACL) Authorize(clientID, topic string, isPublish bool) bool {
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
