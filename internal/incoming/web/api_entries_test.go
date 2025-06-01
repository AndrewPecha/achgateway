package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/incoming/stream/streamtest"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base/log"
	"github.com/stretchr/testify/require"
)

func TestEntriesController_CreateEntriesHandler(t *testing.T) {
	topic, sub := streamtest.InmemStream(t)

	controller := NewEntriesController(log.NewTestLogger(), service.HTTPConfig{}, topic)
	r := mux.NewRouter()
	controller.AppendRoutes(r)

	// Create test request
	req := EntryRequest{
		SECCode:               "PPD",
		CompanyName:           "Test Company",
		CompanyIdentification: "121042882",
		CompanyEntryDesc:      "Test Entry",
		ImmediateDestination:  "231380104",
		ImmediateOrigin:       "121042882",
		Entries: []EntryDetail{
			{
				TransactionCode:    22,
				RDFIIdentification: "091000019",
				AccountNumber:      "81967038518",
				Amount:             100000,
				IndividualName:     "John Doe",
				Addenda: []AddendaRecord{
					{
						Type: "05",
						Data: "Payment for services",
					},
				},
			},
		},
	}

	bs, err := json.Marshal(req)
	require.NoError(t, err)

	// Send request
	httpReq := httptest.NewRequest("POST", "/shards/testing/entries", bytes.NewReader(bs))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httpReq)

	require.Equal(t, http.StatusOK, w.Code)

	// Verify response
	var resp map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Contains(t, resp, "fileID")
	require.Equal(t, "accepted", resp["status"])

	// Verify our subscription receives a message
	msg, err := sub.Receive(context.Background())
	require.NoError(t, err)

	var event models.Event
	require.NoError(t, json.Unmarshal(msg.Body, &event))

	var file incoming.ACHFile
	require.NoError(t, models.ReadEvent(msg.Body, &file))

	require.Equal(t, "testing", file.ShardKey)
	require.Equal(t, "PPD", file.File.Batches[0].GetHeader().StandardEntryClassCode)
	require.Equal(t, "Test Company", file.File.Batches[0].GetHeader().CompanyName)
	require.Equal(t, 1, len(file.File.Batches[0].GetEntries()))
	require.Equal(t, 100000, file.File.Batches[0].GetEntries()[0].Amount)

	// Verify addenda
	entries := file.File.Batches[0].GetEntries()
	require.Equal(t, 1, len(entries[0].Addenda05))
	require.Equal(t, "Payment for services", entries[0].Addenda05[0].PaymentRelatedInformation)
}

func TestEntriesController_InvalidSECCode(t *testing.T) {
	topic, _ := streamtest.InmemStream(t)

	controller := NewEntriesController(log.NewTestLogger(), service.HTTPConfig{}, topic)
	r := mux.NewRouter()
	controller.AppendRoutes(r)

	// Create test request with invalid SEC code
	req := EntryRequest{
		SECCode:               "XXX", // Invalid SEC code
		CompanyName:           "Test Company",
		CompanyIdentification: "121042882",
		CompanyEntryDesc:      "Test Entry",
		ImmediateDestination:  "231380104",
		ImmediateOrigin:       "121042882",
		Entries:               []EntryDetail{},
	}

	bs, err := json.Marshal(req)
	require.NoError(t, err)

	// Send request
	httpReq := httptest.NewRequest("POST", "/shards/testing/entries", bytes.NewReader(bs))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httpReq)

	require.Equal(t, http.StatusBadRequest, w.Code)

	var resp map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Equal(t, "invalid SEC code", resp["error"])
}
