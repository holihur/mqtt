package main

import "testing"

func TestResolveSQLDSN(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantDriver string
		wantDSN    string
	}{
		{
			name:       "postgres url",
			raw:        "postgres://user:pass@localhost/mqtt?sslmode=disable",
			wantDriver: "postgres",
			wantDSN:    "postgres://user:pass@localhost/mqtt?sslmode=disable",
		},
		{
			name:       "postgresql url",
			raw:        "postgresql://user:pass@localhost/mqtt",
			wantDriver: "postgres",
			wantDSN:    "postgresql://user:pass@localhost/mqtt",
		},
		{
			name:       "mysql url with port",
			raw:        "mysql://user:pass@db:3307/mqtt?charset=utf8mb4",
			wantDriver: "mysql",
			wantDSN:    "user:pass@tcp(db:3307)/mqtt?charset=utf8mb4",
		},
		{
			name:       "mysql url without port",
			raw:        "mysql://user:pass@localhost/mqtt",
			wantDriver: "mysql",
			wantDSN:    "user:pass@tcp(localhost:3306)/mqtt",
		},
		{
			name:       "mariadb url",
			raw:        "mariadb://user:pass@localhost/mqtt",
			wantDriver: "mysql",
			wantDSN:    "user:pass@tcp(localhost:3306)/mqtt",
		},
		{
			name:       "native mysql dsn",
			raw:        "user:pass@tcp(db:3306)/mqtt",
			wantDriver: "mysql",
			wantDSN:    "user:pass@tcp(db:3306)/mqtt",
		},
		{
			name:       "mysql unix socket",
			raw:        "user:pass@unix(/tmp/mysql.sock)/mqtt",
			wantDriver: "mysql",
			wantDSN:    "user:pass@unix(/tmp/mysql.sock)/mqtt",
		},
		{
			name:       "sqlite file",
			raw:        "./data/mqtt_messages.db",
			wantDriver: "sqlite",
			wantDSN:    "./data/mqtt_messages.db",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, dsn := resolveSQLDSN(tt.raw)
			if driver != tt.wantDriver || dsn != tt.wantDSN {
				t.Fatalf("resolveSQLDSN(%q) = (%q, %q), want (%q, %q)", tt.raw, driver, dsn, tt.wantDriver, tt.wantDSN)
			}
		})
	}
}

func TestMysqlDSNFromURLIPv6(t *testing.T) {
	got := mysqlDSNFromURL("mysql://user:pass@[::1]/mqtt")
	want := "user:pass@tcp([::1]:3306)/mqtt"
	if got != want {
		t.Fatalf("mysqlDSNFromURL = %q, want %q", got, want)
	}
}

func TestMongoDatabaseFromDSN(t *testing.T) {
	tests := []map[string]string{
		{"dsn": "mongodb://user:pass@localhost:27017/mqtt?authSource=admin", "want": "mqtt"},
		{"dsn": "mongodb://localhost:27017/audit", "want": "audit"},
		{"dsn": "mongodb+srv://user:pass@cluster.example.com/telemetry", "want": "telemetry"},
		{"dsn": "mongodb://localhost:27017", "want": "mqtt"},
		{"dsn": "mongodb://localhost:27017/", "want": "mqtt"},
	}
	for _, tt := range tests {
		if got := mongoDatabaseFromDSN(tt["dsn"]); got != tt["want"] {
			t.Fatalf("mongoDatabaseFromDSN(%q) = %q, want %q", tt["dsn"], got, tt["want"])
		}
	}
}

func TestIsMongoDSN(t *testing.T) {
	if !isMongoDSN("mongodb://localhost:27017/mqtt") {
		t.Fatal("mongodb:// must be detected")
	}
	if !isMongoDSN("mongodb+srv://cluster.example.com/mqtt") {
		t.Fatal("mongodb+srv:// must be detected")
	}
	if isMongoDSN("postgres://localhost/mqtt") {
		t.Fatal("postgres DSN must not be detected as MongoDB")
	}
}
