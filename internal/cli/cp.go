package cli

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/remotecommand"
)

func newCpCommand() *cobra.Command {
	var namespace string
	var container string

	cmd := &cobra.Command{
		Use:   "cp <source> <destination>",
		Short: "Copy files and directories to and from containers",
		Long: `Copy files and directories to and from containers.

Use pod:path syntax to specify container paths.

Examples:
  # Copy local file to pod
  ocp cp ./local.txt mypod:/tmp/remote.txt

  # Copy from pod to local
  ocp cp mypod:/etc/hostname ./hostname

  # Copy with specific namespace
  ocp cp mypod:/var/log ./logs -n my-namespace`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// Complete pod names (user types podname:<TAB> for path)
			if len(args) < 2 {
				// If toComplete contains ":", it's already a pod:path — let the shell handle the path part
				if strings.Contains(toComplete, ":") {
					return nil, cobra.ShellCompDirectiveNoFileComp
				}
				// Complete pod names, append colon for pod:path format
				pods, directive := completePodNames(cmd, toComplete, namespace)
				suffixed := make([]string, 0, len(pods))
				for _, p := range pods {
					suffixed = append(suffixed, p+":")
				}
				return suffixed, directive | cobra.ShellCompDirectiveNoSpace
			}
			return nil, cobra.ShellCompDirectiveDefault
		},
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			src := args[0]
			dst := args[1]
			ns := resolveNamespace(ctx, namespace, false)

			srcPod, srcPath := splitPodPath(src)
			dstPod, dstPath := splitPodPath(dst)

			if srcPod != "" && dstPod != "" {
				return fmt.Errorf("cannot copy between two pods")
			}
			if srcPod == "" && dstPod == "" {
				return fmt.Errorf("one of source or destination must be a pod (pod:path)")
			}

			if srcPod != "" {
				// Copy FROM pod
				return copyFromPod(ctx, ns, srcPod, container, srcPath, dstPath)
			}

			// Copy TO pod
			return copyToPod(ctx, ns, dstPod, container, srcPath, dstPath)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVarP(&container, "container", "c", "", "Container name")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("container", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			pod, _ := splitPodPath(args[0])
			if pod != "" {
				return completeContainerNames(cmd, namespace, pod, toComplete)
			}
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}

func splitPodPath(arg string) (pod, path string) {
	idx := strings.Index(arg, ":")
	if idx < 0 {
		return "", arg
	}
	return arg[:idx], arg[idx+1:]
}

func copyFromPod(ctx context.Context, namespace, podName, container, srcPath, dstPath string) error {
	reader, writer := io.Pipe()

	go func() {
		defer writer.Close()
		streamOpts := remotecommand.StreamOptions{
			Stdout: writer,
			Stderr: os.Stderr,
		}
		err := execInPodWithStreams(ctx, namespace, podName, container,
			[]string{"tar", "cf", "-", "-C", filepath.Dir(srcPath), filepath.Base(srcPath)},
			streamOpts)
		if err != nil {
			writer.CloseWithError(err)
		}
	}()

	return untarStream(reader, dstPath)
}

func copyToPod(ctx context.Context, namespace, podName, container, srcPath, dstPath string) error {
	reader, writer := io.Pipe()

	go func() {
		defer writer.Close()
		if err := tarPath(writer, srcPath); err != nil {
			writer.CloseWithError(err)
		}
	}()

	destDir := dstPath
	streamOpts := remotecommand.StreamOptions{
		Stdin:  reader,
		Stderr: os.Stderr,
	}
	return execInPodWithStreams(ctx, namespace, podName, container,
		[]string{"tar", "xf", "-", "-C", destDir},
		streamOpts)
}

func tarPath(w io.Writer, srcPath string) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	srcPath, err := filepath.Abs(srcPath)
	if err != nil {
		return err
	}

	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}

	baseDir := filepath.Dir(srcPath)

	if !info.IsDir() {
		return addFileToTar(tw, srcPath, baseDir)
	}

	return filepath.Walk(srcPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return addFileToTar(tw, path, baseDir)
	})
}

func addFileToTar(tw *tar.Writer, path, baseDir string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}

	relPath, err := filepath.Rel(baseDir, path)
	if err != nil {
		return err
	}

	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return err
	}
	header.Name = relPath

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	if info.IsDir() {
		return nil
	}

	if info.Mode().IsRegular() {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	}

	return nil
}

func untarStream(reader io.Reader, destPath string) error {
	tr := tar.NewReader(reader)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read error: %w", err)
		}

		target := filepath.Join(destPath, header.Name)

		// Security: prevent path traversal
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destPath)) {
			return fmt.Errorf("tar entry %q attempts path traversal", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
	}

	return nil
}
