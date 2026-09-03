// Package webui 通过 go:embed 将 web/dashboard 的构建产物 (dist) 嵌入到
// broker 二进制中，使最终 release 是单个可执行文件，无需额外部署静态资源。
//
// 构建前请先运行 `task webui` (或直接 `npm run build` + 拷贝到 dist)，
// 否则嵌入的是 dist/index.html 占位页。
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var dist embed.FS

// Handler 返回一个服务于嵌入 dashboard 的 http.Handler。
// 支持 SPA 回退：对不存在的路径统一返回 index.html（本前端使用 hash 路由，
// 该回退主要保证直接访问子路径/刷新时也能加载）。
func Handler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return http.NotFoundHandler()
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(sub, p); err != nil {
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})
}
