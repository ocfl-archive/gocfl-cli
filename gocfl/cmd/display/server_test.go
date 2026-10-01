package display

import (
	"net/url"
	"testing"

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
			srv, err := NewServer(nil, nil, "test-service", tt.addr, u, nil, nil, "", "test-id", nil, nil)
			require.NoError(t, err)
			require.NotNil(t, srv)
			require.Equal(t, tt.expectedAddr, srv.HTTPAddr)
			require.Equal(t, tt.expectedAddr, srv.GetHTTPAddr())
			require.Equal(t, tt.expectedAddr, srv.GetAddr())
		})
	}
}
