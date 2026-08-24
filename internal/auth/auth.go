package auth

// Authenticator decides if client can connect and if topic operation is allowed.

type Authenticator interface {
	Authenticate(clientID, username string, password []byte) bool
	Authorize(clientID, topic string, isPublish bool) bool
}

type AllowAll struct{}

func (a *AllowAll) Authenticate(_, _ string, _ []byte) bool { return true }
func (a *AllowAll) Authorize(_, _ string, _ bool) bool      { return true }

// Simple map-based for testing
type SimpleAuth struct {
	Users map[string]string   // username -> password
	ACL   map[string][]string // clientID -> allowed topic prefixes (empty means allow all)
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
		return true // no ACL for client => allow
	}
	for _, p := range allowed {
		if p == "#" || p == topic || (len(p) > 0 && topicHasPrefix(topic, p)) {
			return true
		}
	}
	return false
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
