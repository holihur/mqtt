//! Authentication / authorization. Port of `internal/auth/auth.go`.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha384, Sha512};
use tokio::sync::Mutex as AsyncMutex;

pub trait Authenticator: Send + Sync {
    fn authenticate(&self, client_id: &str, username: &str, password: &[u8]) -> bool;
    fn authorize(&self, client_id: &str, topic: &str, is_publish: bool) -> bool;
}

pub struct AllowAll;
impl Authenticator for AllowAll {
    fn authenticate(&self, _: &str, _: &str, _: &[u8]) -> bool {
        true
    }
    fn authorize(&self, _: &str, _: &str, _: bool) -> bool {
        true
    }
}

pub struct DenyAll;
impl Authenticator for DenyAll {
    fn authenticate(&self, _: &str, _: &str, _: &[u8]) -> bool {
        false
    }
    fn authorize(&self, _: &str, _: &str, _: bool) -> bool {
        false
    }
}

pub struct SimpleAuth {
    pub users: HashMap<String, String>,  // username -> password
    pub acl: HashMap<String, Vec<String>>, // clientID -> allowed topic prefixes
}

impl Authenticator for SimpleAuth {
    fn authenticate(&self, _: &str, username: &str, password: &[u8]) -> bool {
        if self.users.is_empty() {
            // no users configured = nobody may authenticate (fail-closed)
            return false;
        }
        match self.users.get(username) {
            Some(expect) => constant_time_eq(expect.as_bytes(), password),
            None => false,
        }
    }

    fn authorize(&self, client_id: &str, topic: &str, _: bool) -> bool {
        if self.acl.is_empty() {
            return true;
        }
        match self.acl.get(client_id) {
            None => true,
            Some(allowed) => allowed.iter().any(|p| {
                p == "#" || p == topic || (!p.is_empty() && topic_has_prefix(topic, p))
            }),
        }
    }
}

/// JWT auth (HS256/HS384/HS512, `exp` required, optional `client_id` claim).
pub struct JwtAuth {
    pub secret: String,
}

impl Authenticator for JwtAuth {
    fn authenticate(&self, client_id: &str, username: &str, password: &[u8]) -> bool {
        if self.secret.is_empty() {
            return false;
        }
        let mut token_str = String::from_utf8_lossy(password).into_owned();
        if token_str.is_empty() {
            token_str = username.to_string();
        }
        if token_str.is_empty() || !token_str.contains('.') {
            return false;
        }
        verify_jwt_hmac(&token_str, &self.secret, client_id).unwrap_or(false)
    }

    fn authorize(&self, _: &str, _: &str, _: bool) -> bool {
        true // AllowAll authorization
    }
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s.as_bytes()).ok()
}

fn verify_jwt_hmac(token: &str, secret: &str, client_id: &str) -> Option<bool> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Some(false);
    }
    let header_raw = b64url_decode(parts[0])?;
    let header: serde_json::Value = serde_json::from_slice(&header_raw).ok()?;
    let alg = header.get("alg")?.as_str()?;
    if !matches!(alg, "HS256" | "HS384" | "HS512") {
        return Some(false);
    }
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let sig = b64url_decode(parts[2])?;
    let expected: Vec<u8> = match alg {
        "HS256" => {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).ok()?;
            mac.update(signing_input.as_bytes());
            mac.finalize().into_bytes().to_vec()
        }
        "HS384" => {
            let mut mac = Hmac::<Sha384>::new_from_slice(secret.as_bytes()).ok()?;
            mac.update(signing_input.as_bytes());
            mac.finalize().into_bytes().to_vec()
        }
        _ => {
            let mut mac = Hmac::<Sha512>::new_from_slice(secret.as_bytes()).ok()?;
            mac.update(signing_input.as_bytes());
            mac.finalize().into_bytes().to_vec()
        }
    };
    if !constant_time_eq(&sig, &expected) {
        return Some(false);
    }
    let claims_raw = b64url_decode(parts[1])?;
    let claims: serde_json::Value = serde_json::from_slice(&claims_raw).ok()?;
    // exp is required (jwt.WithExpirationRequired)
    let exp = claims.get("exp").and_then(|v| v.as_f64())?;
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs();
    if (exp as i64) < now as i64 {
        return Some(false);
    }
    if let Some(cid) = claims.get("client_id").and_then(|v| v.as_str()) {
        if !cid.is_empty() && cid != client_id {
            return Some(false);
        }
    }
    Some(true)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AclRule {
    username: String,
    client_id: String,
    topic: String,
    access: String, // read, write, readwrite
}

/// File-based ACL with mtime-based hot reload.
pub struct FileAcl {
    rules: RwLock<Vec<AclRule>>,
    mtime: AsyncMutex<SystemTime>,
    path: String,
}

impl FileAcl {
    pub fn new(path: &str) -> Result<Self, String> {
        let (rules, mtime) = load_acl(path)?;
        Ok(Self { rules: RwLock::new(rules), mtime: AsyncMutex::new(mtime), path: path.to_string() })
    }

    /// Reload if the file mtime advanced. Returns (reloaded?, error).
    pub async fn reload(&self) -> Result<bool, String> {
        if self.path.is_empty() {
            return Ok(false);
        }
        let meta = tokio::fs::metadata(&self.path).await.map_err(|e| e.to_string())?;
        let mtime = meta.modified().map_err(|e| e.to_string())?;
        if mtime <= *self.mtime.lock().await {
            return Ok(false);
        }
        let (rules, new_mtime) = load_acl(&self.path)?;
        *self.rules.write().unwrap() = rules;
        *self.mtime.lock().await = new_mtime;
        Ok(true)
    }

    pub fn rule_count(&self) -> usize {
        self.rules.read().unwrap().len()
    }
}

fn load_acl(path: &str) -> Result<(Vec<AclRule>, SystemTime), String> {
    let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut rules = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        let mut r = AclRule { username: String::new(), client_id: String::new(), topic: String::new(), access: String::new() };
        let mut i = 0;
        while i < parts.len() {
            match parts[i] {
                "user" if i + 1 < parts.len() => {
                    r.username = parts[i + 1].to_string();
                    i += 1;
                }
                "client" if i + 1 < parts.len() => {
                    r.client_id = parts[i + 1].to_string();
                    i += 1;
                }
                "topic" if i + 1 < parts.len() => {
                    r.topic = parts[i + 1].to_string();
                    i += 1;
                }
                "read" | "write" | "readwrite" => r.access = parts[i].to_string(),
                _ => {}
            }
            i += 1;
        }
        if !r.topic.is_empty() {
            if r.access.is_empty() {
                r.access = "readwrite".into();
            }
            rules.push(r);
        }
    }
    let mtime = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    Ok((rules, mtime))
}

impl Authenticator for FileAcl {
    /// Always fails: an ACL file grants topic permissions, not credentials.
    fn authenticate(&self, _: &str, _: &str, _: &[u8]) -> bool {
        false
    }

    fn authorize(&self, client_id: &str, topic: &str, is_publish: bool) -> bool {
        let rules = self.rules.read().unwrap();
        if rules.is_empty() {
            return true;
        }
        let need = if is_publish { "write" } else { "read" };
        for r in rules.iter() {
            if !r.client_id.is_empty() && r.client_id != client_id {
                continue;
            }
            if r.access != "readwrite" && r.access != need {
                continue;
            }
            if r.topic == "#" || r.topic == topic || topic_has_prefix(topic, &r.topic) || match_mqtt_filter(topic, &r.topic) {
                return true;
            }
        }
        false
    }
}

fn match_mqtt_filter(topic: &str, filter: &str) -> bool {
    let t_parts: Vec<&str> = topic.split('/').collect();
    let f_parts: Vec<&str> = filter.split('/').collect();
    for (i, fp) in f_parts.iter().enumerate() {
        if *fp == "#" {
            return i == f_parts.len() - 1;
        }
        if *fp == "+" {
            if i >= t_parts.len() {
                return false;
            }
            continue;
        }
        if i >= t_parts.len() || t_parts[i] != *fp {
            return false;
        }
    }
    t_parts.len() == f_parts.len()
}

fn topic_has_prefix(topic: &str, prefix: &str) -> bool {
    if prefix == "#" {
        return true;
    }
    if prefix.len() >= 2 && &prefix[prefix.len() - 2..] == "/#" {
        let base = &prefix[..prefix.len() - 2];
        return topic == base || (topic.len() > base.len() && &topic[..base.len() + 1] == format!("{}/", base));
    }
    topic == prefix
}

pub struct Chain {
    pub auths: Vec<Box<dyn Authenticator>>,
}

impl Authenticator for Chain {
    fn authenticate(&self, client_id: &str, username: &str, password: &[u8]) -> bool {
        self.auths.iter().all(|a| a.authenticate(client_id, username, password))
    }
    fn authorize(&self, client_id: &str, topic: &str, is_publish: bool) -> bool {
        self.auths.iter().all(|a| a.authorize(client_id, topic, is_publish))
    }
}

/// Adapter to use an Arc<FileAcl> as a boxed Authenticator.
struct SharedFileAcl(std::sync::Arc<FileAcl>);
impl Authenticator for SharedFileAcl {
    fn authenticate(&self, _: &str, _: &str, _: &[u8]) -> bool {
        false
    }
    fn authorize(&self, client_id: &str, topic: &str, is_publish: bool) -> bool {
        self.0.authorize(client_id, topic, is_publish)
    }
}

/// Adapter contributing only its Authorize decision (mirrors Go's authorizeOnly).
pub struct AuthorizeOnly {
    pub inner: Box<dyn Authenticator>,
}
impl Authenticator for AuthorizeOnly {
    fn authenticate(&self, _: &str, _: &str, _: &[u8]) -> bool {
        true
    }
    fn authorize(&self, client_id: &str, topic: &str, is_publish: bool) -> bool {
        self.inner.authorize(client_id, topic, is_publish)
    }
}

/// Dynamic authenticator wrapper used by the broker.
pub enum AnyAuth {
    AllowAll(AllowAll),
    DenyAll(DenyAll),
    Simple(SimpleAuth),
    Jwt(JwtAuth),
    Chain(Chain),
}

impl Authenticator for AnyAuth {
    fn authenticate(&self, client_id: &str, username: &str, password: &[u8]) -> bool {
        match self {
            AnyAuth::AllowAll(a) => a.authenticate(client_id, username, password),
            AnyAuth::DenyAll(a) => a.authenticate(client_id, username, password),
            AnyAuth::Simple(a) => a.authenticate(client_id, username, password),
            AnyAuth::Jwt(a) => a.authenticate(client_id, username, password),
            AnyAuth::Chain(a) => a.authenticate(client_id, username, password),
        }
    }
    fn authorize(&self, client_id: &str, topic: &str, is_publish: bool) -> bool {
        match self {
            AnyAuth::AllowAll(a) => a.authorize(client_id, topic, is_publish),
            AnyAuth::DenyAll(a) => a.authorize(client_id, topic, is_publish),
            AnyAuth::Simple(a) => a.authorize(client_id, topic, is_publish),
            AnyAuth::Jwt(a) => a.authorize(client_id, topic, is_publish),
            AnyAuth::Chain(a) => a.authorize(client_id, topic, is_publish),
        }
    }
}

/// Config subset needed to build the authenticator.
pub struct AuthConfig {
    pub acl_file: String,
    pub jwt_secret: String,
    pub allow_anonymous: bool,
}

/// Build the authenticator from config. Port of `buildAuthenticator`
/// in `internal/broker/broker_util.go` (fail-closed semantics preserved).
/// Also returns the live FileACL(s) so the broker can hot-reload them
/// (mirrors Go's `findFileACLs` traversal).
pub fn build_authenticator(cfg: &AuthConfig) -> Result<(AnyAuth, Vec<std::sync::Arc<FileAcl>>), String> {
    let mut chain: Vec<Box<dyn Authenticator>> = Vec::new();
    let mut file_acls: Vec<std::sync::Arc<FileAcl>> = Vec::new();
    if !cfg.jwt_secret.is_empty() {
        chain.push(Box::new(JwtAuth { secret: cfg.jwt_secret.clone() }));
    }
    if !cfg.acl_file.is_empty() {
        let acl = std::sync::Arc::new(
            FileAcl::new(&cfg.acl_file).map_err(|e| format!("load acl file {}: {}", cfg.acl_file, e))?,
        );
        file_acls.push(acl.clone());
        chain.push(Box::new(AuthorizeOnly { inner: Box::new(SharedFileAcl(acl)) }));
    }
    if chain.is_empty() {
        let auth = if cfg.allow_anonymous { AnyAuth::AllowAll(AllowAll) } else { AnyAuth::DenyAll(DenyAll) };
        return Ok((auth, file_acls));
    }
    if cfg.jwt_secret.is_empty() {
        // an ACL file alone is authorization-only: fail closed without
        // an explicit AllowAnonymous opt-in
        if !cfg.allow_anonymous {
            return Ok((AnyAuth::DenyAll(DenyAll), file_acls));
        }
        chain.insert(0, Box::new(AllowAll));
        return Ok((AnyAuth::Chain(Chain { auths: chain }), file_acls));
    }
    Ok((AnyAuth::Chain(Chain { auths: chain }), file_acls))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub fn now_unix() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_auth() {
        let mut users = HashMap::new();
        users.insert("alice".to_string(), "secret".to_string());
        let sa = SimpleAuth { users, acl: HashMap::new() };
        assert!(sa.authenticate("c1", "alice", b"secret"));
        assert!(!sa.authenticate("c1", "alice", b"wrong"));
        assert!(!sa.authenticate("c1", "bob", b"secret"));
        // empty users fail-closed
        let sa2 = SimpleAuth { users: HashMap::new(), acl: HashMap::new() };
        assert!(!sa2.authenticate("c1", "alice", b""));
    }

    #[test]
    fn file_acl_matching() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acl.txt");
        std::fs::write(&path, "user alice topic a/# readwrite\nuser bob topic sensor/# read\n").unwrap();
        let acl = FileAcl::new(path.to_str().unwrap()).unwrap();
        // alice: readwrite on a/#
        assert!(acl.authorize("c1", "a/b", false));
        assert!(acl.authorize("c1", "a/b", true));
        assert!(!acl.authorize("c1", "sensor/1", true));
        // bob: read only on sensor/#
        let dir2 = tempfile::tempdir().unwrap();
        let path2 = dir2.path().join("acl2.txt");
        std::fs::write(&path2, "client c2 topic sensor/# read\n").unwrap();
        let acl2 = FileAcl::new(path2.to_str().unwrap()).unwrap();
        assert!(acl2.authorize("c2", "sensor/1", false));
        assert!(!acl2.authorize("c2", "sensor/1", true));
        assert!(!acl2.authorize("c2", "act/1", false));
    }

    #[test]
    fn acl_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acl.txt");
        std::fs::write(&path, "topic a/#\n").unwrap();
        let acl = FileAcl::new(path.to_str().unwrap()).unwrap();
        // no mtime change -> no reload
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert!(!acl.reload().await.unwrap());
        });
        // touch file -> reload
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&path, "topic b/#\n").unwrap();
        let mut mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        // force older mtime compare issue: set explicit
        mtime += Duration::from_millis(0);
        rt.block_on(async {
            // The reload compares mtime > stored; ensure monotonic clock advanced
            loop {
                if acl.reload().await.unwrap() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
    }

    #[test]
    fn jwt_hs256() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        let secret = "topsecret";
        let header = B64.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let exp = now_unix() + 600;
        let claims = B64.encode(format!(r#"{{"exp":{},"client_id":"c9"}}"#, exp));
        let signing = format!("{}.{}", header, claims);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(signing.as_bytes());
        let sig = B64.encode(mac.finalize().into_bytes());
        let token = format!("{}.{}", signing, sig);

        let j = JwtAuth { secret: secret.into() };
        assert!(j.authenticate("c9", "", token.as_bytes()));
        assert!(!j.authenticate("other", "", token.as_bytes()));

        // wrong secret
        let j2 = JwtAuth { secret: "nope".into() };
        assert!(!j2.authenticate("c9", "", token.as_bytes()));

        // expired
        let claims = B64.encode(format!(r#"{{"exp":{}}}"#, now_unix() - 10));
        let signing = format!("{}.{}", header, claims);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(signing.as_bytes());
        let token2 = format!("{}.{}", signing, B64.encode(mac.finalize().into_bytes()));
        assert!(!j.authenticate("c9", "", token2.as_bytes()));
    }

    #[test]
    fn jwt_rejects_other_algs() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        let header = B64.encode(br#"{"alg":"none"}"#);
        let claims = B64.encode(format!(r#"{{"exp":{}}}"#, now_unix() + 100));
        let token = format!("{}.{}.", header, claims);
        let j = JwtAuth { secret: "s".into() };
        assert!(!j.authenticate("c", "", token.as_bytes()));
    }
}
