package web

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/moov-io/achgateway/internal/incoming/stream"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base"
	"github.com/moov-io/base/log"
	"gocloud.dev/pubsub"
)

type EntryRequest struct {
	SECCode               string        `json:"secCode"`
	EffectiveEntryDate    string        `json:"effectiveEntryDate,omitempty"`
	CompanyName           string        `json:"companyName"`
	CompanyIdentification string        `json:"companyIdentification"`
	CompanyEntryDesc      string        `json:"companyEntryDescription"`
	ImmediateDestination  string        `json:"immediateDestination"` // Required: RDFI routing number
	ImmediateOrigin       string        `json:"immediateOrigin"`      // Required: ODFI routing number
	Entries               []EntryDetail `json:"entries"`
}

type EntryDetail struct {
	TransactionCode    int             `json:"transactionCode"`
	RDFIIdentification string          `json:"rdfiIdentification"`
	AccountNumber      string          `json:"accountNumber"`
	Amount             int             `json:"amount"`
	IndividualName     string          `json:"individualName"`
	TraceNumber        string          `json:"traceNumber,omitempty"`
	Addenda            []AddendaRecord `json:"addenda,omitempty"`
}

type AddendaRecord struct {
	Type string `json:"type"` // e.g., "99" for return, "05" for POS
	Data string `json:"data"` // The actual addenda information
}

type EntriesController struct {
	logger    log.Logger
	cfg       service.HTTPConfig
	publisher stream.Publisher
}

func NewEntriesController(logger log.Logger, cfg service.HTTPConfig, publisher stream.Publisher) *EntriesController {
	return &EntriesController{
		logger:    logger,
		cfg:       cfg,
		publisher: publisher,
	}
}

func (c *EntriesController) AppendRoutes(router *mux.Router) *mux.Router {
	router.
		Name("Entries.create").
		Methods("POST").
		Path("/shards/{shardKey}/entries").
		HandlerFunc(c.CreateEntriesHandler)

	return router
}

func (c *EntriesController) CreateEntriesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	shardKey := vars["shardKey"]
	if shardKey == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var req EntryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
		return
	}

	// Validate SEC code
	if !isValidSECCode(req.SECCode) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid SEC code"})
		return
	}

	// Create a new ACH file
	file := ach.NewFile()
	file.ID = base.ID()
	file.Header = ach.NewFileHeader()
	file.Header.ImmediateDestination = req.ImmediateDestination
	file.Header.ImmediateOrigin = req.ImmediateOrigin
	file.Header.ImmediateDestinationName = "Bank Name"
	file.Header.ImmediateOriginName = req.CompanyName
	file.Header.FileCreationDate = time.Now().Format("060102") // YYMMDD
	file.Header.FileCreationTime = time.Now().Format("1504")
	file.Header.FileIDModifier = "A"

	// Setup batch header
	bh := ach.NewBatchHeader()
	bh.StandardEntryClassCode = req.SECCode
	bh.ODFIIdentification = req.ImmediateOrigin
	bh.ServiceClassCode = ach.MixedDebitsAndCredits
	bh.CompanyName = req.CompanyName
	bh.CompanyIdentification = req.ImmediateOrigin
	bh.StandardEntryClassCode = req.SECCode
	bh.CompanyEntryDescription = req.CompanyEntryDesc

	// Create batch
	batch, err := ach.NewBatch(bh)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	if req.EffectiveEntryDate != "" {
		bh.EffectiveEntryDate = req.EffectiveEntryDate
	} else {
		bh.EffectiveEntryDate = time.Now().AddDate(0, 0, 1).Format("060102") // Tomorrow
	}

	// Add entries to batch
	for i, entry := range req.Entries {
		ed := ach.NewEntryDetail()
		ed.TransactionCode = entry.TransactionCode
		ed.SetRDFI(entry.RDFIIdentification)
		ed.DFIAccountNumber = entry.AccountNumber
		ed.Amount = entry.Amount
		ed.IndividualName = entry.IndividualName

		if entry.TraceNumber != "" {
			ed.TraceNumber = entry.TraceNumber
		} else {
			// Generate trace number if not provided
			ed.SetTraceNumber(req.ImmediateOrigin, i+1)
		}

		// Add any addenda records
		for _, addenda := range entry.Addenda {
			switch addenda.Type {
			case "99":
				a99 := ach.NewAddenda99()
				a99.ReturnCode = addenda.Data
				ed.Addenda99 = a99
				ed.AddendaRecordIndicator = 1
			case "05":
				a05 := ach.NewAddenda05()
				a05.PaymentRelatedInformation = addenda.Data
				ed.AddAddenda05(a05)
				ed.AddendaRecordIndicator = 1
			default:
				c.logger.Warn().Logf("unsupported addenda type: %s", addenda.Type)
			}
		}

		batch.AddEntry(ed)
	}

	if err := batch.Create(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	file.AddBatch(batch)
	if err := file.Create(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// Generate a unique file ID
	fileID := base.ID()

	// Submit the file to the pipeline
	if err := c.publishFile(r.Context(), shardKey, fileID, file); err != nil {
		c.logger.LogErrorf("problem publishing entries: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"fileID": fileID,
		"status": "accepted",
	})
}

func (c *EntriesController) publishFile(ctx context.Context, shardKey, fileID string, file *ach.File) error {
	bs, err := json.Marshal(models.Event{
		Event: incoming.ACHFile{
			FileID:   fileID,
			ShardKey: shardKey,
			File:     file,
		},
	})
	if err != nil {
		return fmt.Errorf("unable to marshal incoming file event: %v", err)
	}

	meta := make(map[string]string)
	meta["fileID"] = fileID
	meta["shardKey"] = shardKey

	return c.publisher.Send(ctx, &pubsub.Message{
		Body:     bs,
		Metadata: meta,
	})
}

// isValidSECCode returns true if the provided SEC code is supported
func isValidSECCode(code string) bool {
	switch code {
	case "PPD", "CCD", "CTX", "WEB", "TEL", "BOC", "ARC", "POP", "RCK":
		return true
	default:
		return false
	}
}
