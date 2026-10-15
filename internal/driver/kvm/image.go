package kvm

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

// imageCacheDir holds downloaded images, one file per URL. The domain XML
// points at the cached file, never at the network: libvirt must not depend
// on a web server being up every time a domain boots.
var imageCacheDir = "/var/lib/arsan/images"

// resolveImage maps an image_ref to a local qcow2 file. A plain path is used
// as-is (it must already exist on the host); an http(s) URL is downloaded
// once into the cache and reused by every later VM from the same URL.
func resolveImage(ctx context.Context, imageRef string) (string, error) {
	if !isRemote(imageRef) {
		if _, err := os.Stat(imageRef); err != nil {
			return "", fmt.Errorf("kvm: image %q not found on this host: %w", imageRef, err)
		}
		return imageRef, nil
	}
	if err := os.MkdirAll(imageCacheDir, 0o755); err != nil {
		return "", fmt.Errorf("kvm: image cache: %w", err)
	}
	sum := sha256.Sum256([]byte(imageRef))
	path := filepath.Join(imageCacheDir, fmt.Sprintf("%x.qcow2", sum))
	if _, err := os.Stat(path); err == nil {
		return path, nil // already fetched by an earlier build
	}
	if err := fetch(ctx, imageRef, path); err != nil {
		return "", err
	}
	return path, nil
}

func isRemote(ref string) bool {
	u, err := url.Parse(ref)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// fetch downloads url to a temp file and renames it into place, so a
// concurrent build never sees a half-written image. The rename is atomic;
// two racers both download fully and one wins, with identical content.
func fetch(ctx context.Context, rawURL, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("kvm: image request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("kvm: download image: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kvm: download image: status %s", resp.Status)
	}
	tmp, err := os.CreateTemp(imageCacheDir, ".fetch-*")
	if err != nil {
		return fmt.Errorf("kvm: image cache: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return fmt.Errorf("kvm: download image: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("kvm: image cache: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("kvm: image cache: %w", err)
	}
	return nil
}
