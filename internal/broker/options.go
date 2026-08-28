package broker

import (
	"crypto/tls"
	"net"

	"mqtt/internal/auth"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"
)

// Option 采用函数式选项，允许嵌入式调用方以声明式覆盖 Config / 依赖。
type Option func(*Broker) error

// DefaultConfig 返回带合理默认值的 Config，供独立与嵌入式共用。
func DefaultConfig() Config {
	return Config{
		TCPAddr:                   ":1883",
		WSAddr:                    ":8083",
		MaxPacketSize:             1 << 20,
		MaxConnections:            20000,
		MaxPublishPerSec:          100,
		MaxSubscribePerSec:        20,
		AllowAnonymous:            false,
		MaxRetainedMessages:       10000,
		MaxRetainedSize:           1 << 30,
		MaxRetainPerTopic:         1000,
		MaxRetainSizePerTopic:     100 << 20,
		MaxInflightWindow:         100,
		MaxSubscriptionsPerClient: 128,
		WalDir:                    "./data/wal",
	}
}

// ApplyDefaults 仅在零值时填充默认值，不覆盖已有显式配置。
func (c *Config) ApplyDefaults() {
	if c.MaxPacketSize == 0 {
		c.MaxPacketSize = 1 << 20
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 20000
	}
	if c.MaxPublishPerSec == 0 {
		c.MaxPublishPerSec = 100
	}
	if c.MaxSubscribePerSec == 0 {
		c.MaxSubscribePerSec = 20
	}
	if c.MaxRetainedMessages == 0 {
		c.MaxRetainedMessages = 10000
	}
	if c.MaxRetainedSize == 0 {
		c.MaxRetainedSize = 1 << 30
	}
	if c.MaxRetainPerTopic == 0 {
		c.MaxRetainPerTopic = 1000
	}
	if c.MaxInflightWindow == 0 {
		c.MaxInflightWindow = 100
	}
	if c.MaxSubscriptionsPerClient == 0 {
		c.MaxSubscriptionsPerClient = 128
	}
	if c.MaxRetainSizePerTopic == 0 {
		c.MaxRetainSizePerTopic = 100 << 20
	}
	if c.WalDir == "" {
		c.WalDir = "./data/wal"
	}
}

// WithStore 注入持久化实现。传 nil 时 Start 阶段回落到 MemoryStore。
func WithStore(s persistence.Store) Option {
	return func(b *Broker) error {
		b.store = s
		return nil
	}
}

// WithVersion 注入构建期版本信息 (version/commit/date), 管理 API /api/v1/info 展示。
func WithVersion(version, commit, date string) Option {
	return func(b *Broker) error {
		b.versionInfo = brokerVersion{version: version, commit: commit, date: date}
		return nil
	}
}

// WithAuthenticator 注入认证器，会同步注册为 hook。
func WithAuthenticator(a auth.Authenticator) Option {
	return func(b *Broker) error {
		b.auth = a
		return nil
	}
}

// WithHook 注册一个 hook（可在 New 之前或之后多次调用）。
func WithHook(h hook.Hook) Option {
	return func(b *Broker) error {
		if h != nil {
			b.hooks.Register(h)
		}
		return nil
	}
}

// WithHooks 批量注册。
func WithHooks(hooks ...hook.Hook) Option {
	return func(b *Broker) error {
		for _, h := range hooks {
			if h != nil {
				b.hooks.Register(h)
			}
		}
		return nil
	}
}

// WithTLSConfig 直接注入 *tls.Config，优先级高于文件路径。
func WithTLSConfig(tc *tls.Config) Option {
	return func(b *Broker) error {
		b.cfg.TLSConfig = tc
		return nil
	}
}

// WithTCPAddr 覆盖 TCP 监听地址；传 "" 表示嵌入式禁用 TCP 监听。
func WithTCPAddr(addr string) Option {
	return func(b *Broker) error {
		b.cfg.TCPAddr = addr
		return nil
	}
}

// WithWSAddr 覆盖 WebSocket 监听地址；传 "" 禁用 WS。
func WithWSAddr(addr string) Option {
	return func(b *Broker) error {
		b.cfg.WSAddr = addr
		return nil
	}
}

// WithNodeID 显式指定节点 ID。
func WithNodeID(id string) Option {
	return func(b *Broker) error {
		b.cfg.NodeID = id
		if id != "" {
			b.nodeID = id
		}
		return nil
	}
}

// WithAllowAnonymous 是否允许匿名。
func WithAllowAnonymous(allow bool) Option {
	return func(b *Broker) error {
		b.cfg.AllowAnonymous = allow
		return nil
	}
}

// WithCustomListener 注入已建立的 net.Listener（用于嵌入式复用已有端口或 Unix Socket）。
// 注入后 TCPAddr 将被忽略，Start 不再自行 net.Listen。
func WithCustomListener(ln net.Listener) Option {
	return func(b *Broker) error {
		b.customListener = ln
		return nil
	}
}

// WithRedisAddr 覆盖 Redis 地址（空则禁用集群）。
func WithRedisAddr(addr string) Option {
	return func(b *Broker) error {
		b.cfg.RedisAddr = addr
		return nil
	}
}

// WithWALDir 覆盖 WAL 目录；传 "" 或 "-" 表示禁用 WAL。WAL 默认开启 "./data/wal"。
func WithWALDir(dir string) Option {
	return func(b *Broker) error {
		b.cfg.WalDir = dir
		return nil
	}
}

// WithWALStore 注入任意 Store 作为 WAL 实现（接口可替换：Pebble/Badger/Memory 等）。
// 若同时通过 WithStore 注入主 Store，则主 Store 优先，WAL 作为备用；若仅注入 WAL，则其即为主 Store。
func WithWALStore(s persistence.Store) Option {
	return func(b *Broker) error {
		if s != nil {
			b.cfg.WalDir = "injected"
			b.store = s
		}
		return nil
	}
}

// WithWsAllowOrigins 设置 WebSocket 允许的 Origin 白名单；空表示仅同源+空 Origin，含 "*" 表示放行全部。
func WithWsAllowOrigins(origins []string) Option {
	return func(b *Broker) error {
		b.cfg.WsAllowOrigins = origins
		return nil
	}
}

// WithConfig 合并一个完整 Config（非零字段覆盖）。
func WithConfig(cfg Config) Option {
	return func(b *Broker) error {
		if cfg.NodeID != "" {
			b.cfg.NodeID = cfg.NodeID
			b.nodeID = cfg.NodeID
		}
		if cfg.TCPAddr != "" {
			b.cfg.TCPAddr = cfg.TCPAddr
		}
		// WSAddr 允许显式设为空以禁用，需特殊处理：若调用方明确传入 WithConfig 且 WSAddr 为空且原值非空，
		// 这里无法区分零值与显式清空。建议嵌入式用 WithWSAddr("") 显式禁用。
		if cfg.WSAddr != "" {
			b.cfg.WSAddr = cfg.WSAddr
		}
		if cfg.RedisAddr != "" {
			b.cfg.RedisAddr = cfg.RedisAddr
		}
		if cfg.PprofAddr != "" {
			b.cfg.PprofAddr = cfg.PprofAddr
		}
		if cfg.ACLFile != "" {
			b.cfg.ACLFile = cfg.ACLFile
		}
		if cfg.JWTSecret != "" {
			b.cfg.JWTSecret = cfg.JWTSecret
		}
		if cfg.TLSCertFile != "" {
			b.cfg.TLSCertFile = cfg.TLSCertFile
		}
		if cfg.TLSKeyFile != "" {
			b.cfg.TLSKeyFile = cfg.TLSKeyFile
		}
		if cfg.TLSCAFile != "" {
			b.cfg.TLSCAFile = cfg.TLSCAFile
		}
		if cfg.TLSConfig != nil {
			b.cfg.TLSConfig = cfg.TLSConfig
		}
		if cfg.MaxPacketSize != 0 {
			b.cfg.MaxPacketSize = cfg.MaxPacketSize
		}
		if cfg.MaxConnections != 0 {
			b.cfg.MaxConnections = cfg.MaxConnections
		}
		if cfg.MaxPublishPerSec != 0 {
			b.cfg.MaxPublishPerSec = cfg.MaxPublishPerSec
		}
		if cfg.MaxSubscribePerSec != 0 {
			b.cfg.MaxSubscribePerSec = cfg.MaxSubscribePerSec
		}
		if cfg.MaxRetainedMessages != 0 {
			b.cfg.MaxRetainedMessages = cfg.MaxRetainedMessages
		}
		if cfg.MaxRetainedSize != 0 {
			b.cfg.MaxRetainedSize = cfg.MaxRetainedSize
		}
		if cfg.MaxRetainPerTopic != 0 {
			b.cfg.MaxRetainPerTopic = cfg.MaxRetainPerTopic
		}
		if cfg.MaxRetainSizePerTopic != 0 {
			b.cfg.MaxRetainSizePerTopic = cfg.MaxRetainSizePerTopic
		}
		if cfg.WalDir != "" {
			if cfg.WalDir == "-" {
				b.cfg.WalDir = ""
			} else {
				b.cfg.WalDir = cfg.WalDir
			}
		}
		if len(cfg.WsAllowOrigins) > 0 {
			b.cfg.WsAllowOrigins = cfg.WsAllowOrigins
		}
		// AllowAnonymous 为 bool，WithConfig 视为显式覆盖需调用方自行用 WithAllowAnonymous
		return nil
	}
}
