package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newProxyCommand() *cobra.Command {
	var port int
	var address string

	cmd := &cobra.Command{
		Use:   "proxy",
		Short: "Run a proxy to the Kubernetes API server",
		Long: `Run a local reverse proxy to the Kubernetes API server.

The proxy authenticates with the cluster and forwards requests.

Examples:
  # Start proxy on default port 8001
  ocp proxy

  # Start proxy on custom port
  ocp proxy --port=9090

  # Bind to all interfaces
  ocp proxy --address=0.0.0.0 --port=8001`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			out := cmd.OutOrStdout()

			config, err := kube.GetConfig(ctx)
			if err != nil {
				return fmt.Errorf("failed to get config: %w", err)
			}

			serverURL, err := url.Parse(config.Host)
			if err != nil {
				return fmt.Errorf("failed to parse server URL: %w", err)
			}

			// Build reverse proxy
			proxy := httputil.NewSingleHostReverseProxy(serverURL)

			// Configure transport with auth
			transport, err := newAuthenticatedTransport(config)
			if err != nil {
				return fmt.Errorf("failed to create transport: %w", err)
			}
			proxy.Transport = transport

			listenAddr := fmt.Sprintf("%s:%d", address, port)
			listener, err := net.Listen("tcp", listenAddr)
			if err != nil {
				return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
			}

			fmt.Fprintf(out, "Starting to serve on %s\n", listener.Addr().String())

			// Handle shutdown
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt)
			go func() {
				<-sigChan
				listener.Close()
			}()

			server := &http.Server{Handler: proxy}
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("proxy server error: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().IntVarP(&port, "port", "p", 8001, "Port to listen on")
	cmd.Flags().StringVar(&address, "address", "127.0.0.1", "Address to bind to")

	return cmd
}

// authenticatedTransport wraps http.RoundTripper to add auth headers
type authenticatedTransport struct {
	base        http.RoundTripper
	bearerToken string
}

func newAuthenticatedTransport(config *rest.Config) (*authenticatedTransport, error) {
	base, err := rest.TransportFor(config)
	if err != nil {
		return nil, err
	}

	return &authenticatedTransport{
		base:        base,
		bearerToken: config.BearerToken,
	}, nil
}

func (t *authenticatedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.bearerToken != "" && req.Header.Get("Authorization") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+t.bearerToken)
	}
	return t.base.RoundTrip(req)
}
