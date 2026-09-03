//! Embedded web dashboard. Port of `internal/webui/webui.go`:
//! serves the React build from `internal/webui/dist` (embedded at compile
//! time), with SPA fallback to index.html.

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "internal/webui/dist"]
struct Dist;

use crate::broker::admin::HttpResponse;

pub fn serve(path: &str) -> HttpResponse {
    let p = path.trim_start_matches('/');
    let p = if p.is_empty() { "index.html" } else { p };
    let file = Dist::get(p).or_else(|| Dist::get("index.html"));
    match file {
        Some(f) => {
            let data = f.data;
            let mime = mime_of(p);
            HttpResponse {
                status: 200,
                content_type: mime,
                body: data.to_vec(),
                extra_headers: vec![],
            }
        }
        None => HttpResponse::text(404, "404 page not found\n"),
    }
}

fn mime_of(p: &str) -> &'static str {
    let ext = p.rsplit('.').next().unwrap_or("");
    match ext {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        "json" => "application/json",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}
