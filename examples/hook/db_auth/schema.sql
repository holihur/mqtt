-- SQLite / PostgreSQL / MySQL compatible schema for DB Auth Hook (medium)

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username VARCHAR(256) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'active' -- active | disabled
);

CREATE TABLE IF NOT EXISTS acl (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username VARCHAR(256) NOT NULL,
    topic_pattern VARCHAR(512) NOT NULL,
    FOREIGN KEY (username) REFERENCES users(username)
);

-- Example data (password for alice is "s3cr3t" bcrypt hashed)
-- INSERT INTO users (username, password_hash, status) VALUES ('alice', '$2a$10$...', 'active');
-- INSERT INTO acl (username, topic_pattern) VALUES ('alice', 'tenant/t42/#');
-- INSERT INTO acl (username, topic_pattern) VALUES ('alice', 'sensor/+/temp');
