# Auth Hooks

## 内置认证

- `AllowAll` / `DenyAll` / `SimpleAuth` / `JWT (HS256)` / `FileACL`
- 通过 `hook.Manager` 链式调用，任一 `OnAuth` 返回 `ErrDenied` 即拒绝

## DB 认证 Hook（中型内置版）

`internal/hook/db_auth.go` 提供可插拔的数据库认证，支持 `PostgreSQL / MySQL / SQLite`，使用 `database/sql` + `BcryptHasher`。

### 最小配置

```go
import (
    "database/sql"
    "mqtt/internal/hook"
    _ "github.com/mattn/go-sqlite3" // 或 pq / go-sql-driver/mysql
)

db, _ := sql.Open("sqlite3", "./mqtt.db")
h, _ := hook.NewDBAuthHook(db, hook.DBAuthConfig{
    UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?",
    ACLQuery:   "SELECT topic_pattern FROM acl WHERE username = ?",
    Hasher:     hook.BcryptHasher{},
})
broker.RegisterHook(h)
```

`UsersQuery` 需返回 `password_hash`（必选）和 `status`（可选，`active` 放行，其他拒绝）。`ACLQuery` 为空则默认放行；返回的 `topic_pattern` 支持 MQTT 通配 `+`/`#`，通过 `topic.MatchFilter` 判定。

### Schema

```sql
CREATE TABLE users (username VARCHAR(256) PRIMARY KEY, password_hash VARCHAR(255), status VARCHAR(20) DEFAULT 'active');
CREATE TABLE acl (username VARCHAR(256), topic_pattern VARCHAR(512));
```

详见 `examples/hook/db_auth/schema.sql` 与 `examples/hook/db_auth/main.go`（内存 SQLite 完整可跑示例，`go run ./examples/hook/db_auth`）。

### 行为

- `username == ""` 视为匿名，直接放行（由 `AllowAnonymous` 决定）
- `user not found / disabled / password mismatch / DB 超时` 均返回 `ErrDenied` 并 `slog` 记录
- `OnSubscribe/OnPublish` 查询 `ACLQuery`，无记录则拒绝，任一 `pattern` 匹配 `requested` 即放行
- `clientID -> username` 映射在 `OnAuth` 成功后缓存，`OnDisconnect` 清理

### 生产建议

- 连接池：`db.SetMaxOpenConns(10)` 等由调用方配置
- 超时：默认 5s，可 via `DBAuthConfig.QueryTimeout` 调整
- 哈希：默认 `BcryptHasher`，测试可用 `PlainHasher`
- 占位符：`?` 适用于 MySQL/SQLite，PostgreSQL 请用 `$1`（按驱动要求自行在 `UsersQuery/ACLQuery` 中写对）
