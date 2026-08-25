package auth

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestAllowAll(t *testing.T) {
	t.Parallel()
	a := &AllowAll{}
	if !a.Authenticate("c", "u", []byte("p")) {
		t.Fatal("AllowAll.Authenticate should return true")
	}
	if !a.Authorize("c", "t", true) {
		t.Fatal("AllowAll.Authorize should return true")
	}
}

func TestDenyAll(t *testing.T) {
	t.Parallel()
	d := &DenyAll{}
	if d.Authenticate("c", "u", []byte("p")) {
		t.Fatal("DenyAll.Authenticate should return false")
	}
	if d.Authorize("c", "t", false) {
		t.Fatal("DenyAll.Authorize should return false")
	}
}

func TestSimpleAuth_Authenticate(t *testing.T) {
	t.Parallel()

	t.Run("empty_users_allows_all", func(t *testing.T) {
		s := &SimpleAuth{Users: map[string]string{}}
		if !s.Authenticate("c", "anyone", []byte("x")) {
			t.Fatal("empty users map should allow all")
		}
	})

	t.Run("valid_user_correct_password", func(t *testing.T) {
		s := &SimpleAuth{Users: map[string]string{"admin": "secret"}}
		if !s.Authenticate("c", "admin", []byte("secret")) {
			t.Fatal("correct password should pass")
		}
	})

	t.Run("valid_user_wrong_password", func(t *testing.T) {
		s := &SimpleAuth{Users: map[string]string{"admin": "secret"}}
		if s.Authenticate("c", "admin", []byte("wrong")) {
			t.Fatal("wrong password should fail")
		}
	})

	t.Run("unknown_user", func(t *testing.T) {
		s := &SimpleAuth{Users: map[string]string{"admin": "secret"}}
		if s.Authenticate("c", "nobody", []byte("secret")) {
			t.Fatal("unknown user should fail")
		}
	})

	t.Run("empty_password", func(t *testing.T) {
		s := &SimpleAuth{Users: map[string]string{"admin": ""}}
		if !s.Authenticate("c", "admin", []byte("")) {
			t.Fatal("empty password match should pass")
		}
	})
}

func TestSimpleAuth_Authorize(t *testing.T) {
	t.Parallel()

	t.Run("empty_acl_allows_all", func(t *testing.T) {
		s := &SimpleAuth{ACL: map[string][]string{}}
		if !s.Authorize("c", "any/topic", true) {
			t.Fatal("empty ACL should allow all")
		}
	})

	t.Run("client_not_in_acl_allows", func(t *testing.T) {
		s := &SimpleAuth{ACL: map[string][]string{"c1": {"t1"}}}
		if !s.Authorize("other", "t1", false) {
			t.Fatal("client not in ACL should allow")
		}
	})

	t.Run("exact_topic_match", func(t *testing.T) {
		s := &SimpleAuth{ACL: map[string][]string{"c1": {"a/b"}}}
		if !s.Authorize("c1", "a/b", false) {
			t.Fatal("exact topic match should allow")
		}
	})

	t.Run("hash_suffix_match", func(t *testing.T) {
		s := &SimpleAuth{ACL: map[string][]string{"c1": {"a/#"}}}
		if !s.Authorize("c1", "a/b", false) {
			t.Fatal("a/# should match a/b")
		}
	})

	t.Run("wildcard_hash", func(t *testing.T) {
		s := &SimpleAuth{ACL: map[string][]string{"c1": {"#"}}}
		if !s.Authorize("c1", "anything/here", true) {
			t.Fatal("# wildcard should allow all")
		}
	})

	t.Run("topic_not_allowed", func(t *testing.T) {
		s := &SimpleAuth{ACL: map[string][]string{"c1": {"a/b"}}}
		if s.Authorize("c1", "x/y", false) {
			t.Fatal("non-matching topic should deny")
		}
	})
}

func TestJWTAuth(t *testing.T) {
	secret := "test-secret-123"
	j := &JWTAuth{Secret: secret}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"client_id": "c1", "exp": time.Now().Add(time.Hour).Unix()})
	signed, _ := token.SignedString([]byte(secret))
	if !j.Authenticate("c1", "any", []byte(signed)) {
		t.Fatalf("valid jwt should pass")
	}
	if j.Authenticate("c2", "any", []byte(signed)) {
		t.Fatalf("client_id mismatch should fail")
	}
	token2 := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"client_id": "c1", "exp": time.Now().Add(-time.Hour).Unix()})
	signed2, _ := token2.SignedString([]byte(secret))
	if j.Authenticate("c1", "any", []byte(signed2)) {
		t.Fatalf("expired should fail")
	}
	if j.Authenticate("c1", "any", []byte("not-a-jwt")) {
		t.Fatalf("invalid should fail")
	}
}

func TestJWTAuth_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty_secret_rejects", func(t *testing.T) {
		j := &JWTAuth{Secret: ""}
		if j.Authenticate("c", "u", []byte("token")) {
			t.Fatal("empty secret should reject")
		}
	})

	t.Run("empty_password_and_username_rejects", func(t *testing.T) {
		j := &JWTAuth{Secret: "s"}
		if j.Authenticate("c", "", []byte("")) {
			t.Fatal("empty password and username should reject")
		}
	})

	t.Run("token_from_username_when_password_empty", func(t *testing.T) {
		secret := "my-secret"
		j := &JWTAuth{Secret: secret}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(time.Hour).Unix()})
		signed, _ := token.SignedString([]byte(secret))
		if !j.Authenticate("c", signed, []byte("")) {
			t.Fatal("token in username with empty password should work")
		}
	})

	t.Run("no_claims_token_passes", func(t *testing.T) {
		secret := "my-secret"
		j := &JWTAuth{Secret: secret}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{})
		signed, _ := token.SignedString([]byte(secret))
		if !j.Authenticate("c", "u", []byte(signed)) {
			t.Fatal("token without exp/client_id claims should pass")
		}
	})

	t.Run("wrong_secret_rejects", func(t *testing.T) {
		j := &JWTAuth{Secret: "correct"}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(time.Hour).Unix()})
		signed, _ := token.SignedString([]byte("wrong"))
		if j.Authenticate("c", "u", []byte(signed)) {
			t.Fatal("wrong signing secret should reject")
		}
	})

	t.Run("non_jwt_string_with_dot_rejects", func(t *testing.T) {
		j := &JWTAuth{Secret: "s"}
		if j.Authenticate("c", "u", []byte("has.dot")) {
			t.Fatal("non-JWT string with dot should reject")
		}
	})

	t.Run("client_id_empty_in_claims_passes", func(t *testing.T) {
		secret := "s"
		j := &JWTAuth{Secret: secret}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"client_id": "", "exp": time.Now().Add(time.Hour).Unix()})
		signed, _ := token.SignedString([]byte(secret))
		if !j.Authenticate("c", "u", []byte(signed)) {
			t.Fatal("empty client_id in claims should not block")
		}
	})
}

func TestFileACL(t *testing.T) {
	content := `
client client1 topic a/b read
client client1 topic a/# readwrite
`
	tmp, _ := os.CreateTemp("", "acl")
	_, _ = tmp.WriteString(content)
	_ = tmp.Close()
	defer os.Remove(tmp.Name())
	acl, err := NewFileACL(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Authorize("client1", "a/b", false) {
		t.Fatalf("client1 a/b should allow")
	}
	if acl.Authorize("client1", "x/y", false) {
		t.Fatalf("client1 x/y should deny")
	}
	if !acl.Authorize("client1", "a/c/d", true) {
		t.Fatalf("client1 a/# write should allow")
	}
	if acl.Authorize("other", "a/b", false) {
		t.Fatalf("other should deny")
	}
	// no rules file should allow all
	acl2 := &FileACL{}
	if !acl2.Authorize("any", "any", false) {
		t.Fatalf("empty acl should allow")
	}
}

func TestNewFileACL_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nonexistent_file", func(t *testing.T) {
		_, err := NewFileACL("/tmp/nonexistent_acl_file_12345.txt")
		if err == nil {
			t.Fatal("expected error for nonexistent file")
		}
	})

	t.Run("comments_and_blank_lines", func(t *testing.T) {
		content := "# comment\n\n   \nclient c1 topic t1 read\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if !acl.Authorize("c1", "t1", false) {
			t.Fatal("should parse rule after comments/blanks")
		}
	})

	t.Run("default_access_is_readwrite", func(t *testing.T) {
		content := "topic sensors/temp\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if !acl.Authorize("any", "sensors/temp", false) {
			t.Fatal("default readwrite should allow read")
		}
		if !acl.Authorize("any", "sensors/temp", true) {
			t.Fatal("default readwrite should allow write")
		}
	})

	t.Run("write_only_access", func(t *testing.T) {
		content := "client c1 topic logs write\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if acl.Authorize("c1", "logs", false) {
			t.Fatal("write-only should deny read")
		}
		if !acl.Authorize("c1", "logs", true) {
			t.Fatal("write-only should allow write")
		}
	})

	t.Run("read_only_access", func(t *testing.T) {
		content := "client c1 topic data read\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if !acl.Authorize("c1", "data", false) {
			t.Fatal("read-only should allow read")
		}
		if acl.Authorize("c1", "data", true) {
			t.Fatal("read-only should deny write")
		}
	})

	t.Run("mqtt_plus_wildcard", func(t *testing.T) {
		content := "client c1 topic sensor/+/temp read\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if !acl.Authorize("c1", "sensor/1/temp", false) {
			t.Fatal("+ wildcard should match single level")
		}
		if acl.Authorize("c1", "sensor/1/2/temp", false) {
			t.Fatal("+ wildcard should not match multiple levels")
		}
	})

	t.Run("mqtt_hash_wildcard", func(t *testing.T) {
		content := "client c1 topic home/# read\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if !acl.Authorize("c1", "home/living/temp", false) {
			t.Fatal("# wildcard should match multi-level")
		}
	})

	t.Run("no_client_filter_matches_any", func(t *testing.T) {
		content := "topic public read\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if !acl.Authorize("anyone", "public", false) {
			t.Fatal("no client filter should match any client")
		}
	})

	t.Run("line_without_topic_skipped", func(t *testing.T) {
		content := "client c1 read\nclient c2 topic t2 write\n"
		tmp := writeTempACL(t, content)
		acl, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if !acl.Authorize("c2", "t2", true) {
			t.Fatal("second rule should still work")
		}
	})
}

func TestFileACL_Reload(t *testing.T) {
	t.Parallel()

	t.Run("no_path_returns_false", func(t *testing.T) {
		f := &FileACL{}
		reloaded, err := f.Reload()
		if err != nil {
			t.Fatal(err)
		}
		if reloaded {
			t.Fatal("empty path should return false")
		}
	})

	t.Run("same_mtime_no_reload", func(t *testing.T) {
		content := "topic t1 read\n"
		tmp := writeTempACL(t, content)
		f, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		reloaded, err := f.Reload()
		if err != nil {
			t.Fatal(err)
		}
		if reloaded {
			t.Fatal("same mtime should not reload")
		}
	})

	t.Run("newer_mtime_reloads", func(t *testing.T) {
		content := "topic t1 read\n"
		tmp := writeTempACL(t, content)
		f, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		f.mtime = time.Now().Add(-time.Hour)
		reloaded, err := f.Reload()
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded {
			t.Fatal("newer mtime should trigger reload")
		}
	})

	t.Run("nonexistent_file_on_reload", func(t *testing.T) {
		tmp := writeTempACL(t, "topic t1 read\n")
		f, err := NewFileACL(tmp)
		if err != nil {
			t.Fatal(err)
		}
		os.Remove(tmp)
		f.mtime = time.Now().Add(-time.Hour)
		_, err = f.Reload()
		if err == nil {
			t.Fatal("expected error when file removed")
		}
	})
}

func TestMatchMqttFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		topic, filter string
		want          bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/b/c", "a/b", false},
		{"a/b", "a/b/c", false},
		{"a/b", "+/b", true},
		{"a/b", "a/+", true},
		{"a/b/c", "+/+/+", true},
		{"a/b", "+/+/+", false},
		{"a/b", "#", true},
		{"a/b/c", "#", true},
		{"a/b", "+", false},
		{"sensor/1/temp", "+/+/temp", true},
		{"a", "a", true},
		{"a", "b", false},
	}

	for _, tc := range tests {
		got := matchMqttFilter(tc.topic, tc.filter)
		if got != tc.want {
			t.Errorf("matchMqttFilter(%q, %q) = %v, want %v", tc.topic, tc.filter, got, tc.want)
		}
	}
}

func TestTopicHasPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		topic, prefix string
		want          bool
	}{
		{"a/b", "#", true},
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/b/c", "a/#", true},
		{"a", "a/#", true},
		{"b/c", "a/#", false},
		{"a/b", "a/b/#", true},
		{"a/b/c", "a/b/#", true},
	}

	for _, tc := range tests {
		got := topicHasPrefix(tc.topic, tc.prefix)
		if got != tc.want {
			t.Errorf("topicHasPrefix(%q, %q) = %v, want %v", tc.topic, tc.prefix, got, tc.want)
		}
	}
}

func TestChain(t *testing.T) {
	t.Parallel()

	t.Run("all_pass", func(t *testing.T) {
		c := &Chain{Auths: []Authenticator{&AllowAll{}, &AllowAll{}}}
		if !c.Authenticate("c", "u", []byte("p")) {
			t.Fatal("all pass should return true")
		}
		if !c.Authorize("c", "t", false) {
			t.Fatal("all pass authorize should return true")
		}
	})

	t.Run("first_fails_short_circuits", func(t *testing.T) {
		c := &Chain{Auths: []Authenticator{&DenyAll{}, &AllowAll{}}}
		if c.Authenticate("c", "u", []byte("p")) {
			t.Fatal("first deny should short-circuit authenticate")
		}
		if c.Authorize("c", "t", false) {
			t.Fatal("first deny should short-circuit authorize")
		}
	})

	t.Run("second_fails", func(t *testing.T) {
		c := &Chain{Auths: []Authenticator{&AllowAll{}, &DenyAll{}}}
		if c.Authenticate("c", "u", []byte("p")) {
			t.Fatal("second deny should fail authenticate")
		}
		if c.Authorize("c", "t", false) {
			t.Fatal("second deny should fail authorize")
		}
	})

	t.Run("empty_chain_passes", func(t *testing.T) {
		c := &Chain{Auths: []Authenticator{}}
		if !c.Authenticate("c", "u", []byte("p")) {
			t.Fatal("empty chain should pass")
		}
		if !c.Authorize("c", "t", false) {
			t.Fatal("empty chain authorize should pass")
		}
	})
}

func writeTempACL(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "acl.txt")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
