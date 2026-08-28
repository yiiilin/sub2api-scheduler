package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeBoardJSONRejectsUnknownAndTrailingFields(t *testing.T) {
	for _, body := range []string{
		`{"name":"board","model":"flash-v1","lanes":[],"unexpected":true}`,
		`{"name":"board","model":"flash-v1","lanes":[]} {"extra":true}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/api/boards", strings.NewReader(body))
		if err := decodeBoardJSON(recorder, request, &LaneBoard{}); err == nil {
			t.Fatalf("decodeBoardJSON accepted invalid body %q", body)
		}
	}
}

func TestDecodeBoardJSONRejectsOversizedBody(t *testing.T) {
	recorder := httptest.NewRecorder()
	body := append([]byte(`{"name":"`), bytes.Repeat([]byte("x"), 1<<20)...)
	body = append(body, []byte(`","model":"flash-v1","lanes":[]}`)...)
	request := httptest.NewRequest(http.MethodPost, "/api/boards", bytes.NewReader(body))
	if err := decodeBoardJSON(recorder, request, &LaneBoard{}); err == nil {
		t.Fatal("decodeBoardJSON accepted a body larger than 1 MiB")
	}
}

func TestBoardMutationStatus(t *testing.T) {
	if got := boardMutationStatus(ErrInvalidBoard); got != http.StatusBadRequest {
		t.Fatalf("invalid board status = %d, want %d", got, http.StatusBadRequest)
	}
	if got := boardMutationStatus(ErrBoardNotFound); got != http.StatusNotFound {
		t.Fatalf("missing board status = %d, want %d", got, http.StatusNotFound)
	}
	if got := boardMutationStatus(errors.New("database unavailable")); got != http.StatusInternalServerError {
		t.Fatalf("internal error status = %d, want %d", got, http.StatusInternalServerError)
	}
}
