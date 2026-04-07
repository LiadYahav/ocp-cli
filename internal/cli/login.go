package cli

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newLoginCommand() *cobra.Command {
	var server string
	var token string
	var username string
	var password string
	var insecure bool
	var caFile string

	cmd := &cobra.Command{
		Use:   "login [--server=<server>]",
		Short: "Log in to an OpenShift or Kubernetes cluster",
		Long: `Log in to a cluster by providing a server URL and credentials.

You can authenticate with a token or username/password. On OpenShift clusters,
username/password login uses OAuth token discovery.

Examples:
  # Login with a token
  ocp login --server=https://api.cluster.example.com:6443 --token=sha256~xxxx

  # Login with username and password
  ocp login -s https://api.cluster.example.com:6443 -u admin -p password

  # Login with insecure TLS
  ocp login -s https://api.cluster.example.com:6443 --token=sha256~xxxx --insecure-skip-tls-verify`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			out := cmd.OutOrStdout()

			if server == "" {
				// Try to get server from current kubeconfig
				config, err := loadRawKubeconfig(ctx)
				if err == nil && config.CurrentContext != "" {
					if ctxObj, ok := config.Contexts[config.CurrentContext]; ok {
						if cluster, ok := config.Clusters[ctxObj.Cluster]; ok {
							server = cluster.Server
						}
					}
				}
				if server == "" {
					return fmt.Errorf("--server is required (no current cluster found)")
				}
			}

			// Normalize server URL
			if !strings.HasPrefix(server, "http://") && !strings.HasPrefix(server, "https://") {
				server = "https://" + server
			}
			server = strings.TrimRight(server, "/")

			// If username/password provided but no token, try OAuth discovery
			if token == "" && username != "" && password != "" {
				var err error
				token, err = discoverOAuthToken(server, username, password, insecure, caFile)
				if err != nil {
					// OAuth not available, will use basic auth in kubeconfig
					fmt.Fprintf(out, "OAuth token discovery unavailable, using basic auth\n")
				}
			}

			if token == "" && username == "" {
				return fmt.Errorf("--token or --username/--password required")
			}

			// Build kubeconfig entries
			serverURL, err := url.Parse(server)
			if err != nil {
				return fmt.Errorf("invalid server URL: %w", err)
			}

			clusterName := strings.ReplaceAll(serverURL.Host, ".", "-")
			clusterName = strings.ReplaceAll(clusterName, ":", "-")
			userName := "user/" + clusterName
			contextName := clusterName

			if username != "" {
				userName = username + "/" + clusterName
				contextName = clusterName + "/" + username
			}

			// Load or create kubeconfig
			configAccess := clientcmd.NewDefaultPathOptions()
			if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
				configAccess.LoadingRules.ExplicitPath = configPath
			}

			config, err := configAccess.GetStartingConfig()
			if err != nil {
				config = clientcmdapi.NewConfig()
			}

			// Set cluster
			cluster := clientcmdapi.NewCluster()
			cluster.Server = server
			cluster.InsecureSkipTLSVerify = insecure
			if caFile != "" {
				cluster.CertificateAuthority = caFile
			}
			config.Clusters[clusterName] = cluster

			// Set user credentials
			authInfo := clientcmdapi.NewAuthInfo()
			if token != "" {
				authInfo.Token = token
			} else {
				authInfo.Username = username
				authInfo.Password = password
			}
			config.AuthInfos[userName] = authInfo

			// Set context
			ctxObj := clientcmdapi.NewContext()
			ctxObj.Cluster = clusterName
			ctxObj.AuthInfo = userName
			config.Contexts[contextName] = ctxObj
			config.CurrentContext = contextName

			// Verify connection
			restConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
			if err != nil {
				return fmt.Errorf("failed to build client config: %w", err)
			}

			clientset, err := kubernetes.NewForConfig(restConfig)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			serverVersion, err := clientset.Discovery().ServerVersion()
			if err != nil {
				return fmt.Errorf("failed to verify connection: %w", err)
			}

			// Save kubeconfig
			if err := clientcmd.ModifyConfig(configAccess, *config, true); err != nil {
				return fmt.Errorf("failed to save kubeconfig: %w", err)
			}

			fmt.Fprintf(out, "Login successful.\n\n")
			fmt.Fprintf(out, "Server:  %s\n", server)
			fmt.Fprintf(out, "Version: %s\n", serverVersion.GitVersion)
			if username != "" {
				fmt.Fprintf(out, "User:    %s\n", username)
			}
			fmt.Fprintf(out, "\nUsing context %q\n", contextName)

			return nil
		},
	}

	cmd.Flags().StringVarP(&server, "server", "s", "", "API server URL")
	cmd.Flags().StringVar(&token, "token", "", "Bearer token for authentication")
	cmd.Flags().StringVarP(&username, "username", "u", "", "Username for authentication")
	cmd.Flags().StringVarP(&password, "password", "p", "", "Password for authentication")
	cmd.Flags().BoolVar(&insecure, "insecure-skip-tls-verify", false, "Skip TLS certificate verification")
	cmd.Flags().StringVar(&caFile, "certificate-authority", "", "Path to CA certificate file")
	_ = cmd.RegisterFlagCompletionFunc("certificate-authority", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return nil, cobra.ShellCompDirectiveFilterFileExt
	})

	return cmd
}

func loadRawKubeconfig(ctx context.Context) (clientcmdapi.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
		loadingRules.ExplicitPath = configPath
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
}

// discoverOAuthToken attempts OpenShift OAuth token discovery
func discoverOAuthToken(server, username, password string, insecure bool, caFile string) (string, error) {
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	if insecure {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	// Discover OAuth server metadata
	metadataURL := server + "/.well-known/oauth-authorization-server"
	resp, err := httpClient.Get(metadataURL)
	if err != nil {
		return "", fmt.Errorf("OAuth discovery failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OAuth discovery returned %d", resp.StatusCode)
	}

	var metadata struct {
		TokenEndpoint string `json:"token_endpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return "", fmt.Errorf("failed to parse OAuth metadata: %w", err)
	}

	if metadata.TokenEndpoint == "" {
		return "", fmt.Errorf("no token_endpoint in OAuth metadata")
	}

	// Request token with basic auth
	tokenReq, err := http.NewRequest("GET", metadata.TokenEndpoint+"?grant_type=password", nil)
	if err != nil {
		return "", err
	}
	tokenReq.SetBasicAuth(username, password)
	tokenReq.Header.Set("X-CSRF-Token", "1")

	tokenResp, err := httpClient.Do(tokenReq)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer tokenResp.Body.Close()

	// OpenShift returns a redirect with the token in the fragment
	if tokenResp.StatusCode == http.StatusFound {
		location := tokenResp.Header.Get("Location")
		if idx := strings.Index(location, "access_token="); idx >= 0 {
			token := location[idx+len("access_token="):]
			if ampIdx := strings.IndexByte(token, '&'); ampIdx >= 0 {
				token = token[:ampIdx]
			}
			return token, nil
		}
	}

	// Try JSON response
	if tokenResp.StatusCode == http.StatusOK {
		var tokenData struct {
			AccessToken string `json:"access_token"`
		}
		if err := json.NewDecoder(tokenResp.Body).Decode(&tokenData); err == nil && tokenData.AccessToken != "" {
			return tokenData.AccessToken, nil
		}
	}

	return "", fmt.Errorf("failed to obtain token (status %d)", tokenResp.StatusCode)
}
