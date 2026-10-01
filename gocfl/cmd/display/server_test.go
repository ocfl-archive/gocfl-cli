package display

import (
	"bytes"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/dustin/go-humanize"
	"github.com/gin-contrib/multitemplate"
	"github.com/gin-gonic/gin"
	"github.com/ocfl-archive/gocfl-cli/data/displaydata"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_content_subpath"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/inventory"
	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
	"github.com/stretchr/testify/require"
)

func TestNewServer_HTTPAddr(t *testing.T) {
	tests := []struct {
		name         string
		addr         string
		urlExt       string
		expectedAddr string
	}{
		{
			name:         "empty host fallback to localhost",
			addr:         ":8080",
			urlExt:       "",
			expectedAddr: "http://localhost:8080",
		},
		{
			name:         "0.0.0.0 host fallback to localhost",
			addr:         "0.0.0.0:9090",
			urlExt:       "",
			expectedAddr: "http://localhost:9090",
		},
		{
			name:         "specific host",
			addr:         "127.0.0.1:8080",
			urlExt:       "",
			expectedAddr: "http://127.0.0.1:8080",
		},
		{
			name:         "hostname",
			addr:         "example.com:443",
			urlExt:       "",
			expectedAddr: "http://example.com:443",
		},
		{
			name:         "scheme from urlExt",
			addr:         ":8443",
			urlExt:       "https://my-domain.org:8443",
			expectedAddr: "https://localhost:8443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var u *url.URL
			if tt.urlExt != "" {
				var err error
				u, err = url.Parse(tt.urlExt)
				require.NoError(t, err)
			}
			srv, err := NewServer(nil, nil, "test-service", tt.addr, u, nil, nil, "", "test-id", nil, nil, nil)
			require.NoError(t, err)
			require.NotNil(t, srv)
			require.Equal(t, tt.expectedAddr, srv.HTTPAddr)
			require.Equal(t, tt.expectedAddr, srv.GetHTTPAddr())
			require.Equal(t, tt.expectedAddr, srv.GetAddr())
		})
	}
}

func TestObjectTemplate_AreaStats(t *testing.T) {
	funcMap := sprig.FuncMap()
	funcMap["basename"] = func(str string) string {
		return filepath.Base(str)
	}
	funcMap["PathEscape"] = func(str string) string {
		return url.PathEscape(str)
	}
	funcMap["humanizeBytes"] = func(size uint64) string {
		return humanize.Bytes(size)
	}
	funcMap["humanizeTime"] = func(t time.Time) string {
		return t.Format("2006-01-02 15:04:05")
	}

	tpl, err := template.New("object.gohtml").Funcs(funcMap).ParseFS(displaydata.TemplateRoot, "templates/object.gohtml")
	require.NoError(t, err)

	areaStats := []*AreaStats{
		{
			Name:           "",
			Path:           "",
			Description:    "Default Area",
			NumFiles:       5,
			DifferentFiles: 3,
			Size:           1024,
			SizeStr:        "1.0 kB",
			NoSizeFiles:    0,
			MimeTypes:      map[string]int{"text/plain": 2, "image/jpeg": 1},
			Pronoms:        map[string]int{"fmt/1": 2},
		},
		{
			Name:           "metadata",
			Path:           "metadata",
			Description:    "Metadata files",
			NumFiles:       2,
			DifferentFiles: 2,
			Size:           512,
			SizeStr:        "512 B",
			NoSizeFiles:    0,
			MimeTypes:      map[string]int{"application/json": 2},
			Pronoms:        map[string]int{"fmt/817": 2},
		},
	}

	params := map[string]any{
		"title":          "gocfl",
		"id":             "test-obj-1",
		"versions":       map[string]any{},
		"differentFiles": 5,
		"numFiles":       7,
		"size":           "1.5 kB",
		"noSizeFiles":    0,
		"mimeTypes":      map[string]int{"text/plain": 2, "image/jpeg": 1, "application/json": 2},
		"pronoms":        map[string]int{"fmt/1": 2, "fmt/817": 2},
		"areaStats":      areaStats,
	}

	var buf bytes.Buffer
	err = tpl.Execute(&buf, params)
	require.NoError(t, err)

	htmlOut := buf.String()
	require.Contains(t, htmlOut, "Default Area")
	require.Contains(t, htmlOut, "Metadata files")
	require.Contains(t, htmlOut, "metadata")
	require.Contains(t, htmlOut, "fmt/817")
	require.Contains(t, htmlOut, "application/json")
}

func TestServer_DisplayObjectRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	mt := multitemplate.New()
	funcMap := sprig.FuncMap()
	funcMap["basename"] = func(str string) string { return filepath.Base(str) }
	funcMap["PathEscape"] = func(str string) string { return url.PathEscape(str) }
	funcMap["humanizeBytes"] = func(size uint64) string { return humanize.Bytes(size) }
	funcMap["humanizeTime"] = func(t time.Time) string { return t.Format("2006-01-02 15:04:05") }

	tpl, err := template.New("object.gohtml").Funcs(funcMap).ParseFS(displaydata.TemplateRoot, "templates/object.gohtml")
	require.NoError(t, err)
	mt.Add("object.gohtml", tpl)
	r.HTMLRender = mt

	srv := &Server{
		templateFS: displaydata.TemplateRoot,
		metadata: &inventory.Metadata{
			ID: "test-object-area",
			Extension: map[string]any{
				ext_NNNN_content_subpath.ContentSubPathName: map[string]ext_NNNN_content_subpath.ContentSubPathEntry{
					"master":      {Path: "data/master", Description: "Master Files"},
					"derivatives": {Path: "data/derivatives", Description: "Derivative Files"},
					"unused":      {Path: "data/unused", Description: "Unused Empty Area"},
				},
			},
			Files: map[string]*inventory.FileMetadata{
				"hash1": {
					InternalName: []string{"v1/content/data/master/file1.tif"},
					Extension: map[string]any{
						ext_NNNN_content_subpath.ContentSubPathName: []string{"master"},
						ext_NNNN_indexer.IndexerName: &indexer.ResultV2{
							Size:     1000,
							Mimetype: "image/tiff",
							Pronom:   "fmt/353",
						},
					},
				},
				"hash2": {
					InternalName: []string{"v1/content/data/derivatives/file1.jpg"},
					Extension: map[string]any{
						ext_NNNN_content_subpath.ContentSubPathName: []string{"derivatives"},
						ext_NNNN_indexer.IndexerName: &indexer.ResultV2{
							Size:     200,
							Mimetype: "image/jpeg",
							Pronom:   "fmt/43",
						},
					},
				},
			},
		},
	}

	r.GET("/object/test", srv.displayObject)

	req := httptest.NewRequest(http.MethodGet, "/object/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "Master Files")
	require.Contains(t, body, "Derivative Files")
	require.NotContains(t, body, "Unused Empty Area")
	require.Contains(t, body, "image/tiff")
	require.Contains(t, body, "image/jpeg")
	require.Contains(t, body, "fmt/353")
	require.Contains(t, body, "fmt/43")
}

func TestServer_DisplayObjectRoute_NoSubPathExtension(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	mt := multitemplate.New()
	funcMap := sprig.FuncMap()
	funcMap["basename"] = func(str string) string { return filepath.Base(str) }
	funcMap["PathEscape"] = func(str string) string { return url.PathEscape(str) }
	funcMap["humanizeBytes"] = func(size uint64) string { return humanize.Bytes(size) }
	funcMap["humanizeTime"] = func(t time.Time) string { return t.Format("2006-01-02 15:04:05") }

	tpl, err := template.New("object.gohtml").Funcs(funcMap).ParseFS(displaydata.TemplateRoot, "templates/object.gohtml")
	require.NoError(t, err)
	mt.Add("object.gohtml", tpl)
	r.HTMLRender = mt

	srv := &Server{
		templateFS: displaydata.TemplateRoot,
		metadata: &inventory.Metadata{
			ID:        "test-object-no-ext",
			Extension: nil,
			Files: map[string]*inventory.FileMetadata{
				"hash1": {
					InternalName: []string{"v1/content/file1.txt"},
					Extension: map[string]any{
						ext_NNNN_indexer.IndexerName: &indexer.ResultV2{
							Size:     500,
							Mimetype: "text/plain",
							Pronom:   "x-fmt/111",
						},
					},
				},
			},
		},
	}

	r.GET("/object/test-no-ext", srv.displayObject)

	req := httptest.NewRequest(http.MethodGet, "/object/test-no-ext", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "text/plain")
	require.Contains(t, body, "x-fmt/111")
	require.Contains(t, body, "500 B")
	// Since there is only the default area without subpaths, redundant "Area: Default Area" or "Total" should not clutter the UI
	require.NotContains(t, body, "Default Area")
	require.NotContains(t, body, "<h3>Total</h3>")
}
