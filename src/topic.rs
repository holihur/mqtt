//! Topic trie with `+`/`#` wildcard support and `$SYS` isolation.
//! Port of `internal/topic/trie.go`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubEntry {
    pub client_id: String,
    pub filter: String,
    pub qos: u8,
    pub no_local: bool,
}

#[derive(Debug, Default)]
struct Node {
    children: HashMap<String, Node>,
    subs: HashMap<String, SubEntry>, // key: clientID#filter
}

impl Node {
    fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default)]
pub struct Trie {
    root: RwLock<Node>,
}

impl Trie {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn add(&self, filter: &str, client_id: &str, qos: u8, no_local: bool) {
        let levels: Vec<&str> = filter.split('/').collect();
        let mut guard = self.root.write().unwrap();
        let mut n = &mut *guard;
        for lv in levels {
            n = n.children.entry(lv.to_string()).or_insert_with(Node::new);
        }
        let key = format!("{}#{}", client_id, filter);
        n.subs.insert(
            key,
            SubEntry { client_id: client_id.to_string(), filter: filter.to_string(), qos, no_local },
        );
    }

    pub fn remove(&self, filter: &str, client_id: &str) {
        let levels: Vec<&str> = filter.split('/').collect();
        // Navigate and collect the path of child keys first (borrow checker
        // friendly), then remove bottom-up with pruning.
        let mut path: Vec<String> = Vec::new();
        {
            let guard = self.root.read().unwrap();
            let mut cur = &*guard;
            for lv in &levels {
                match cur.children.get(*lv) {
                    Some(c) => {
                        path.push(lv.to_string());
                        cur = c;
                    }
                    None => return,
                }
            }
        }
        let mut guard = self.root.write().unwrap();
        Self::remove_rec(&mut guard, &path, client_id, filter);
    }

    fn remove_rec(node: &mut Node, path: &[String], client_id: &str, filter: &str) {
        if path.is_empty() {
            let key = format!("{}#{}", client_id, filter);
            node.subs.remove(&key);
            return;
        }
        let key = &path[0];
        let child = match node.children.get_mut(key) {
            Some(c) => c,
            None => return,
        };
        Self::remove_rec(child, &path[1..], client_id, filter);
        // prune empty branches
        if child.subs.is_empty() && child.children.is_empty() {
            node.children.remove(key);
        }
    }

    /// Return all subscribers whose filter matches topic (stack DFS like Go).
    pub fn match_topic(&self, topic: &str) -> Vec<SubEntry> {
        let levels: Vec<&str> = topic.split('/').collect();
        let guard = self.root.read().unwrap();
        let mut result: Vec<SubEntry> = Vec::new();
        let mut stack: Vec<(&Node, usize)> = vec![(&*guard, 0)];
        while let Some((n, idx)) = stack.pop() {
            if idx == levels.len() {
                for s in n.subs.values() {
                    result.push(s.clone());
                }
                if let Some(child) = n.children.get("#") {
                    for s in child.subs.values() {
                        result.push(s.clone());
                    }
                }
                continue;
            }
            if let Some(child) = n.children.get("#") {
                for s in child.subs.values() {
                    result.push(s.clone());
                }
            }
            if let Some(child) = n.children.get("+") {
                stack.push((child, idx + 1));
            }
            if let Some(child) = n.children.get(levels[idx]) {
                stack.push((child, idx + 1));
            }
        }
        // $SYS isolation: subs on "#" or starting with "+" never match "$..." topics
        if topic.starts_with('$') {
            result.retain(|s| s.filter != "#" && !s.filter.starts_with('+') && !s.filter.starts_with('#'));
        }
        result
    }

    /// All subscription entries across the trie (management API).
    pub fn subscriptions(&self) -> Vec<SubEntry> {
        let guard = self.root.read().unwrap();
        let mut out = Vec::new();
        walk(&guard, &mut |s| out.push(s.clone()), None);
        out
    }

    /// All subscription entries of a given client (management API).
    pub fn subscriptions_for(&self, client_id: &str) -> Vec<SubEntry> {
        let guard = self.root.read().unwrap();
        let mut out = Vec::new();
        walk(&guard, &mut |s| out.push(s.clone()), Some(client_id));
        out
    }
}

fn walk(node: &Node, out: &mut dyn FnMut(SubEntry), only_client: Option<&str>) {
    for s in node.subs.values() {
        if let Some(cid) = only_client {
            if s.client_id != cid {
                continue;
            }
        }
        out(s.clone());
    }
    for c in node.children.values() {
        walk(c, out, only_client);
    }
}

/// Validate filter per MQTT spec.
pub fn is_valid_filter(f: &str) -> bool {
    if f.is_empty() {
        return false;
    }
    let levels: Vec<&str> = f.split('/').collect();
    for (i, lv) in levels.iter().enumerate() {
        if lv.is_empty() {
            continue;
        }
        if *lv == "#" {
            if i != levels.len() - 1 {
                return false;
            }
        } else if lv.contains('#') || lv.contains('+') {
            if *lv != "+" && *lv != "#" {
                return false;
            }
        }
    }
    true
}

pub fn is_valid_topic(topic: &str) -> bool {
    if topic.is_empty() {
        return false;
    }
    !topic.contains('+') && !topic.contains('#')
}

/// Stateless topic/filter match (no trie allocation). Same semantics as Trie.
pub fn match_filter(topic: &str, filter: &str) -> bool {
    if filter == "#" {
        if topic.starts_with('$') {
            return false;
        }
        return true;
    }
    if filter == topic {
        return true;
    }
    let t_levels: Vec<&str> = topic.split('/').collect();
    let f_levels: Vec<&str> = filter.split('/').collect();
    for (i, f) in f_levels.iter().enumerate() {
        if *f == "#" {
            return i == f_levels.len() - 1;
        }
        if i >= t_levels.len() {
            return false;
        }
        if *f == "+" {
            continue;
        }
        if *f != t_levels[i] {
            return false;
        }
    }
    t_levels.len() == f_levels.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_remove_match() {
        let t = Trie::new();
        t.add("a/b/c", "c1", 1, false);
        t.add("a/+/c", "c2", 0, false);
        t.add("a/#", "c3", 2, false);
        t.add("#", "c4", 0, false);

        let m = t.match_topic("a/b/c");
        let ids: Vec<&str> = m.iter().map(|s| s.client_id.as_str()).collect();
        assert!(ids.contains(&"c1") && ids.contains(&"c2") && ids.contains(&"c3") && ids.contains(&"c4"));

        let m = t.match_topic("a/x/c");
        let ids: Vec<&str> = m.iter().map(|s| s.client_id.as_str()).collect();
        assert!(!ids.contains(&"c1") && ids.contains(&"c2") && ids.contains(&"c3"));

        t.remove("a/b/c", "c1");
        let m = t.match_topic("a/b/c");
        let ids: Vec<&str> = m.iter().map(|s| s.client_id.as_str()).collect();
        assert!(!ids.contains(&"c1"));
    }

    #[test]
    fn sys_isolation() {
        let t = Trie::new();
        t.add("#", "c1", 0, false);
        t.add("+/broker/uptime", "c2", 0, false);
        t.add("$SYS/broker/uptime", "c3", 0, false);
        let m = t.match_topic("$SYS/broker/uptime");
        let ids: Vec<&str> = m.iter().map(|s| s.client_id.as_str()).collect();
        assert_eq!(ids, vec!["c3"]);
    }

    #[test]
    fn wildcard_edge() {
        let t = Trie::new();
        t.add("a/b/#", "c1", 0, false);
        assert!(t.match_topic("a/b").iter().any(|s| s.client_id == "c1"));
        assert!(t.match_topic("a/b/c/d").iter().any(|s| s.client_id == "c1"));
        assert!(t.match_topic("a/b/c").iter().any(|s| s.client_id == "c1"));
        assert!(t.match_topic("a/x").is_empty());
    }

    #[test]
    fn subscriptions_listing() {
        let t = Trie::new();
        t.add("x/y", "c1", 1, true);
        t.add("x/z", "c2", 0, false);
        t.add("w", "c1", 2, false);
        let all = t.subscriptions();
        assert_eq!(all.len(), 3);
        let mine = t.subscriptions_for("c1");
        assert_eq!(mine.len(), 2);
        let nl = mine.iter().find(|s| s.filter == "x/y").unwrap();
        assert!(nl.no_local);
    }

    #[test]
    fn valid_filters() {
        assert!(is_valid_filter("a/b/c"));
        assert!(is_valid_filter("a/+/c"));
        assert!(is_valid_filter("a/#"));
        assert!(is_valid_filter("#"));
        assert!(is_valid_filter("a//b")); // empty level allowed
        assert!(!is_valid_filter(""));
        assert!(!is_valid_filter("a/#/b"));
        assert!(!is_valid_filter("a/b#/c"));
        assert!(!is_valid_filter("a/+x/c"));
        assert!(!is_valid_topic("a/+/c"));
        assert!(is_valid_topic("a/b/c"));
        assert!(!is_valid_topic(""));
    }

    #[test]
    fn match_filter_stateless() {
        assert!(match_filter("a/b/c", "a/+/c"));
        assert!(match_filter("a/b/c", "a/#"));
        assert!(match_filter("a/b/c", "#"));
        assert!(!match_filter("$SYS/x", "#"));
        assert!(!match_filter("a/b", "a/b/c"));
        assert!(!match_filter("a/b/c", "a/b"));
        assert!(match_filter("a/b", "a/b"));
        assert!(match_filter("a//c", "a/+/c"));
        assert!(!match_filter("a/b/c/d", "a/+/+"));
        assert!(match_filter("a/b/c/d", "a/+/+/+"));
    }

    #[test]
    fn remove_prunes_empty_branches() {
        let t = Trie::new();
        t.add("a/b/c", "c1", 0, false);
        t.remove("a/b/c", "c1");
        assert!(t.match_topic("a/b/c").is_empty());
        assert!(t.subscriptions().is_empty());
        // removing a non-existent path is a no-op
        t.remove("x/y/z", "nope");
    }
}
