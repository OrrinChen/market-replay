package observability

import (
	"net/http"
	"net/http/pprof"
	"strings"
)

func RegisterPprof(mux *http.ServeMux, prefix string) {
	if prefix == "" {
		prefix = "/debug/pprof"
	}
	prefix = strings.TrimRight(prefix, "/")

	mux.HandleFunc(prefix+"/", pprof.Index)
	mux.HandleFunc(prefix+"/cmdline", pprof.Cmdline)
	mux.HandleFunc(prefix+"/profile", pprof.Profile)
	mux.HandleFunc(prefix+"/symbol", pprof.Symbol)
	mux.HandleFunc(prefix+"/trace", pprof.Trace)
}
