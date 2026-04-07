package cli

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newCertsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "certs",
		Short: "Certificate management commands",
	}

	cmd.AddCommand(newCertsCheckCommand())
	return cmd
}

func newCertsCheckCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var warnDays int

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Scan TLS certificates for expiration",
		Long: `Scans all kubernetes.io/tls secrets for certificate expiration.
Certificates are sorted by expiry date. Expired certs are shown in red,
those within the warning window in yellow.`,
		Example: `  # Check all TLS certs cluster-wide
  ocp certs check

  # Check with 60-day warning window
  ocp certs check --warn-days=60

  # Check in a specific namespace
  ocp certs check --namespace=my-app`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			opts := metav1.ListOptions{
				FieldSelector: "type=kubernetes.io/tls",
				Limit:         500,
			}

			type certInfo struct {
				namespace string
				secret    string
				cn        string
				san       string
				expires   time.Time
				daysLeft  int
				status    string
			}

			var certs []certInfo
			for {
				secrets, err := clientset.CoreV1().Secrets(ns).List(ctx, opts)
				if err != nil {
					return fmt.Errorf("failed to list secrets: %w", err)
				}

				for _, secret := range secrets.Items {
					tlsCrt, ok := secret.Data["tls.crt"]
					if !ok {
						continue
					}

					parsed := parseCertChain(tlsCrt)
					for _, cert := range parsed {
						daysLeft := int(time.Until(cert.NotAfter).Hours() / 24)
						status := "OK"
						if daysLeft < 0 {
							status = "EXPIRED"
						} else if daysLeft <= warnDays {
							status = "WARNING"
						}

						san := strings.Join(cert.DNSNames, ",")
						if len(san) > 50 {
							san = san[:50] + "..."
						}

						certs = append(certs, certInfo{
							namespace: secret.Namespace,
							secret:    secret.Name,
							cn:        cert.Subject.CommonName,
							san:       san,
							expires:   cert.NotAfter,
							daysLeft:  daysLeft,
							status:    status,
						})
					}
				}

				if secrets.Continue == "" {
					break
				}
				opts.Continue = secrets.Continue
			}

			if len(certs) == 0 {
				fmt.Fprintln(out, "No TLS certificates found.")
				return nil
			}

			sort.Slice(certs, func(i, j int) bool {
				return certs[i].expires.Before(certs[j].expires)
			})

			w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NAMESPACE\tSECRET\tCN\tSAN\tEXPIRES\tDAYS_LEFT\tSTATUS")
			for _, c := range certs {
				statusStr := c.status
				switch c.status {
				case "EXPIRED":
					statusStr = "\033[1;31mEXPIRED\033[0m"
				case "WARNING":
					statusStr = "\033[1;33mWARNING\033[0m"
				}

				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
					c.namespace, c.secret, c.cn, c.san,
					c.expires.Format("2006-01-02"), c.daysLeft, statusStr)
			}
			w.Flush()

			var expired, warning, ok int
			for _, c := range certs {
				switch c.status {
				case "EXPIRED":
					expired++
				case "WARNING":
					warning++
				default:
					ok++
				}
			}

			fmt.Fprintf(out, "\nTotal: %d  OK: %d  Warning: %d  Expired: %d\n",
				len(certs), ok, warning, expired)

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace to check")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", true, "Check all namespaces")
	cmd.Flags().IntVar(&warnDays, "warn-days", 30, "Warning threshold in days")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func parseCertChain(data []byte) []*x509.Certificate {
	var certs []*x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		certs = append(certs, cert)
	}
	return certs
}
