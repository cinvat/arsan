package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

// patchedObject reads a patch from the request and returns current with the
// patch applied.
//
// It speaks RFC 7386 JSON merge patch (application/merge-patch+json), the patch
// a kubectl-style `patch` or `apply` sends. JSON Patch (RFC 6902, an ordered
// operation list) and strategic merge patch (a Kubernetes extension) are named
// and refused rather than silently mis-read as a merge patch.
func patchedObject[T any](c *gin.Context, current T) (T, bool) {
	var zero T
	switch c.ContentType() {
	case "application/json-patch+json":
		write(c, http.StatusUnsupportedMediaType, "unsupported_patch_type",
			errors.New("json patch (RFC 6902) is not supported; use application/merge-patch+json"))
		return zero, false
	case "application/strategic-merge-patch+json":
		write(c, http.StatusUnsupportedMediaType, "unsupported_patch_type",
			errors.New("strategic merge patch is not supported; use application/merge-patch+json"))
		return zero, false
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		write(c, http.StatusBadRequest, "bad_request", err)
		return zero, false
	}
	if len(body) == 0 {
		return current, true // an empty patch changes nothing
	}

	cur, err := toMap(current)
	if err != nil {
		write(c, http.StatusInternalServerError, "internal", err)
		return zero, false
	}
	var patch map[string]any
	if err := json.Unmarshal(body, &patch); err != nil {
		write(c, http.StatusBadRequest, "bad_request", err)
		return zero, false
	}
	mergePatch(cur, patch)

	merged, err := json.Marshal(cur)
	if err != nil {
		write(c, http.StatusInternalServerError, "internal", err)
		return zero, false
	}
	var out T
	if err := json.Unmarshal(merged, &out); err != nil {
		write(c, http.StatusBadRequest, "bad_request", err)
		return zero, false
	}
	return out, true
}

// mergePatch applies src onto dst per RFC 7386: a null removes the key, two
// objects merge recursively, and anything else (arrays included) replaces.
func mergePatch(dst, src map[string]any) {
	for k, sv := range src {
		if sv == nil {
			delete(dst, k)
			continue
		}
		sm, sok := sv.(map[string]any)
		dm, dok := dst[k].(map[string]any)
		if sok && dok {
			mergePatch(dm, sm)
			continue
		}
		dst[k] = sv
	}
}

func toMap(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}
