package hook

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"golang.org/x/crypto/bcrypt"
)

func TestBcryptHasher(t *testing.T) {
	h := BcryptHasher{}
	pw := []byte("s3cr3t")
	hash, _ := bcrypt.GenerateFromPassword(pw, bcrypt.MinCost)
	if !h.Verify(pw, string(hash)) {
		t.Fatalf("bcrypt verify should pass")
	}
	if h.Verify([]byte("wrong"), string(hash)) {
		t.Fatalf("bcrypt verify should fail")
	}
}

func TestPlainHasher(t *testing.T) {
	h := PlainHasher{}
	if !h.Verify([]byte("abc"), "abc") {
		t.Fatalf("plain should pass")
	}
	if h.Verify([]byte("abc"), "xyz") {
		t.Fatalf("plain should fail")
	}
}

func TestNewDBAuthHookValidation(t *testing.T) {
	if _, err := NewDBAuthHook(nil, DBAuthConfig{UsersQuery: "SELECT 1"}); err == nil {
		t.Fatalf("should fail nil db")
	}
	db, _, _ := sqlmock.New()
	defer db.Close()
	if _, err := NewDBAuthHook(db, DBAuthConfig{}); err == nil {
		t.Fatalf("should fail empty UsersQuery")
	}
	if _, err := NewDBAuthHook(db, DBAuthConfig{UsersQuery: "SELECT 1", Hasher: PlainHasher{}}); err != nil {
		t.Fatalf("should pass with plain hasher: %v", err)
	}
}

func TestDBAuthOnAuthSuccess(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	rows := sqlmock.NewRows([]string{"password_hash", "status"}).AddRow(string(hash), "active")
	mock.ExpectQuery("SELECT password_hash").WithArgs("alice").WillReturnRows(rows)
	h, _ := NewDBAuthHook(db, DBAuthConfig{UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?", Hasher: BcryptHasher{}})
	if err := h.OnAuth("c1", "alice", []byte("pass")); err != nil {
		t.Fatalf("auth should pass: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDBAuthOnAuthUserNotFound(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SELECT password_hash").WithArgs("bob").WillReturnRows(sqlmock.NewRows([]string{"password_hash", "status"}))
	h, _ := NewDBAuthHook(db, DBAuthConfig{UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?", Hasher: PlainHasher{}})
	if err := h.OnAuth("c1", "bob", []byte("any")); !isDenied(err) {
		t.Fatalf("should deny not found, got %v", err)
	}
}

func TestDBAuthOnAuthDisabled(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	rows := sqlmock.NewRows([]string{"password_hash", "status"}).AddRow("hash", "disabled")
	mock.ExpectQuery("SELECT password_hash").WithArgs("bob").WillReturnRows(rows)
	h, _ := NewDBAuthHook(db, DBAuthConfig{UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?", Hasher: PlainHasher{}})
	if err := h.OnAuth("c1", "bob", []byte("hash")); !isDenied(err) {
		t.Fatalf("should deny disabled")
	}
}

func TestDBAuthOnAuthWrongPassword(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("correct"), bcrypt.MinCost)
	rows := sqlmock.NewRows([]string{"password_hash", "status"}).AddRow(string(hash), "active")
	mock.ExpectQuery("SELECT password_hash").WithArgs("alice").WillReturnRows(rows)
	h, _ := NewDBAuthHook(db, DBAuthConfig{UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?", Hasher: BcryptHasher{}})
	if err := h.OnAuth("c1", "alice", []byte("wrong")); !isDenied(err) {
		t.Fatalf("should deny wrong password")
	}
}

func TestDBAuthOnAuthOneColumn(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	rows := sqlmock.NewRows([]string{"password_hash"}).AddRow("plainpass")
	mock.ExpectQuery("SELECT password_hash").WithArgs("alice").WillReturnRows(rows)
	h, _ := NewDBAuthHook(db, DBAuthConfig{UsersQuery: "SELECT password_hash FROM users WHERE username = ?", Hasher: PlainHasher{}})
	if err := h.OnAuth("c1", "alice", []byte("plainpass")); err != nil {
		t.Fatalf("one column should pass: %v", err)
	}
}

func TestDBAuthACLAllowAndDeny(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	// auth success to store mapping
	rows := sqlmock.NewRows([]string{"password_hash", "status"}).AddRow(string(hash), "active")
	mock.ExpectQuery("SELECT password_hash").WithArgs("alice").WillReturnRows(rows)
	h, _ := NewDBAuthHook(db, DBAuthConfig{
		UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?",
		ACLQuery:   "SELECT topic_pattern FROM acl WHERE username = ?",
		Hasher:     BcryptHasher{},
	})
	if err := h.OnAuth("c1", "alice", []byte("pass")); err != nil {
		t.Fatalf("auth fail: %v", err)
	}
	// ACL allow exact
	mock.ExpectQuery("SELECT topic_pattern").WithArgs("alice").WillReturnRows(sqlmock.NewRows([]string{"topic_pattern"}).AddRow("sensor/+/temp"))
	if err := h.OnPublish("c1", "sensor/a/temp", nil, 0, false); err != nil {
		t.Fatalf("acl should allow wildcard: %v", err)
	}
	// ACL deny
	mock.ExpectQuery("SELECT topic_pattern").WithArgs("alice").WillReturnRows(sqlmock.NewRows([]string{"topic_pattern"}).AddRow("sensor/+/temp"))
	if err := h.OnPublish("c1", "sensor/a/humidity", nil, 0, false); !isDenied(err) {
		t.Fatalf("acl should deny")
	}
	// ACL allow via exact
	mock.ExpectQuery("SELECT topic_pattern").WithArgs("alice").WillReturnRows(sqlmock.NewRows([]string{"topic_pattern"}).AddRow("tenant/t42/#"))
	if err := h.OnSubscribe("c1", "tenant/t42/data", 0); err != nil {
		t.Fatalf("acl should allow prefix: %v", err)
	}
	// No ACL rows -> deny
	mock.ExpectQuery("SELECT topic_pattern").WithArgs("alice").WillReturnRows(sqlmock.NewRows([]string{"topic_pattern"}))
	if err := h.OnSubscribe("c1", "any/topic", 0); !isDenied(err) {
		t.Fatalf("empty acl should deny")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Regression: requesting a filter BROADER than the granted pattern must be denied.
// The ACL pattern authorizes topics; the requested subscription filter must be
// covered by it. Subscribing "#" with only "tenant/t42/#" granted leaks the
// whole broker and must fail.
func TestDBAuthACLDenyBroaderFilterThanPattern(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	rows := sqlmock.NewRows([]string{"password_hash", "status"}).AddRow(string(hash), "active")
	mock.ExpectQuery("SELECT password_hash").WithArgs("alice").WillReturnRows(rows)
	h, _ := NewDBAuthHook(db, DBAuthConfig{
		UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?",
		ACLQuery:   "SELECT topic_pattern FROM acl WHERE username = ?",
		Hasher:     BcryptHasher{},
	})
	if err := h.OnAuth("c1", "alice", []byte("pass")); err != nil {
		t.Fatalf("auth fail: %v", err)
	}
	// exact pattern filter itself is allowed
	mock.ExpectQuery("SELECT topic_pattern").WithArgs("alice").WillReturnRows(sqlmock.NewRows([]string{"topic_pattern"}).AddRow("tenant/t42/#"))
	if err := h.OnSubscribe("c1", "tenant/t42/#", 0); err != nil {
		t.Fatalf("exact pattern filter should be allowed: %v", err)
	}
	// broader filter "#" must be denied
	mock.ExpectQuery("SELECT topic_pattern").WithArgs("alice").WillReturnRows(sqlmock.NewRows([]string{"topic_pattern"}).AddRow("tenant/t42/#"))
	if err := h.OnSubscribe("c1", "#", 0); !isDenied(err) {
		t.Fatalf("subscribe # with only tenant/t42/# granted should be denied, got %v", err)
	}
	// broader wildcard filter must be denied too
	mock.ExpectQuery("SELECT topic_pattern").WithArgs("alice").WillReturnRows(sqlmock.NewRows([]string{"topic_pattern"}).AddRow("sensor/+/temp"))
	if err := h.OnSubscribe("c1", "sensor/#", 0); !isDenied(err) {
		t.Fatalf("subscribe sensor/# with only sensor/+/temp granted should be denied, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDBAuthACLNoQueryAllows(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	h, _ := NewDBAuthHook(db, DBAuthConfig{UsersQuery: "SELECT 1", ACLQuery: ""})
	if err := h.OnPublish("c1", "any/topic", nil, 0, false); err != nil {
		t.Fatalf("no acl query should allow")
	}
	if err := h.OnSubscribe("c1", "any/#", 0); err != nil {
		t.Fatalf("no acl query should allow")
	}
}

// Regression: empty username must NOT bypass DB authentication.
func TestDBAuthAnonDenied(t *testing.T) {
	h := &DBAuthHook{queryTimeout: 0}
	if err := h.OnAuth("c1", "", []byte("")); !isDenied(err) {
		t.Fatalf("empty username should be denied, got %v", err)
	}
}

func isDenied(err error) bool {
	return err != nil && err.Error() == ErrDenied.Error()
}
