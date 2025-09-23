package kv_service

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
)

// readRequestJSON expects req to have a JSON content type with a body that
// contains a JSON-encoded value complying with the underlying type of target.
// It populates target, or returns an error.
func readRequestJSON(req *http.Request, target any) error {
	var contentType = req.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("failed to parse Content-Type header. err=%w", err)
	}
	if mediaType != "application/json" {
		return fmt.Errorf("Content-Type not application/json, got=%s", mediaType)
	}
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

// renderJSON renders 'v' as JSON and writes it as a response into w.
func renderJSON(w http.ResponseWriter, value any) {
	b, err := json.Marshal(value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
