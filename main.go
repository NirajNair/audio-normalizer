package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	ffmpeg "github.com/u2takey/ffmpeg-go"
)

const (
	MaxUploadSize = 10 << 20 // 10MB
	SampleRate    = 16000
	StorageDir    = "storage/normalized"
	WorkerCount   = 4
)

type Job struct {
	ctx     context.Context
	data    []byte
	hash    string
	ext     string
	outPath string
	result  chan Result
}

type Result struct {
	Duration float64
	Size     int64
	Sha256   string
	Err      error
}

var (
	jobQueue = make(chan Job, 32)
)

func main() {
	os.MkdirAll(StorageDir, 0755)

	for i := 0; i < WorkerCount; i++ {
		go worker()
	}

	http.HandleFunc("/v1/normalize", normalizeHandler)
	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}

func worker() {
	for job := range jobQueue {
		job.result <- process(job)
	}
}

func process(job Job) Result {
	tmpInput := filepath.Join(os.TempDir(), job.hash+job.ext)
	tmpOutput := job.outPath

	err := os.WriteFile(tmpInput, job.data, 0644)
	if err != nil {
		return Result{Err: err}
	}
	defer os.Remove(tmpInput)

	var stderr bytes.Buffer

	err = ffmpeg.Input(tmpInput).
		Output(tmpOutput, ffmpeg.KwArgs{
			"ac":         1,
			"ar":         SampleRate,
			"sample_fmt": "s16",
			"f":          "wav",
		}).
		WithErrorOutput(&stderr).
		Run()

	if err != nil {
		return Result{Err: fmt.Errorf("ffmpeg: %s", stderr.String())}
	}

	file, _ := os.Open(tmpOutput)
	defer file.Close()

	h := sha256.New()
	size, _ := io.Copy(h, file)

	return Result{
		Size:   size,
		Sha256: hex.EncodeToString(h.Sum(nil)),
	}
}

func normalizeHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()

	if err := r.ParseMultipartForm(MaxUploadSize); err != nil {
		httpError(w, 400, "INVALID_FORM", err.Error())
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		httpError(w, 400, "NO_FILE", "missing file field")
		return
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(header.Filename))
	if ext != ".mp3" && ext != ".wav" {
		httpError(w, 400, "UNSUPPORTED_FORMAT", "only mp3 and wav supported")
		return
	}

	data, err := io.ReadAll(file)
	if err != nil {
		httpError(w, 500, "READ_FAILED", err.Error())
		return
	}

	if int64(len(data)) > MaxUploadSize {
		httpError(w, 400, "FILE_TOO_LARGE", "max upload size is 10MB")
		return
	}

	fmt.Println("Upload bytes:", len(data))
	fmt.Println("First 16 bytes:", data[:16])

	hash := sha256Hex(data)
	outPath := filepath.Join(StorageDir, hash+".wav")

	if _, err := os.Stat(outPath); err == nil {
		respond(w, hash, header.Filename, outPath, true, start)
		return
	}

	tmpPath := outPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			respond(w, hash, header.Filename, outPath, true, start)
			return
		}
		httpError(w, 500, "FS_ERROR", err.Error())
		return
	}
	f.Close()

	job := Job{
		ctx:     ctx,
		data:    data,
		hash:    hash,
		ext:     ext,
		outPath: tmpPath,
		result:  make(chan Result, 1),
	}

	jobQueue <- job
	res := <-job.result

	if res.Err != nil {
		os.Remove(tmpPath)
		httpError(w, 500, "FFMPEG_FAILED", res.Err.Error())
		return
	}

	os.Rename(tmpPath, outPath)
	respond(w, hash, header.Filename, outPath, false, start)
}

func respond(w http.ResponseWriter, hash, original, path string, skipped bool, start time.Time) {
	info, _ := os.Stat(path)

	resp := map[string]any{
		"transactionId": fmt.Sprintf("%d", time.Now().UnixNano()),
		"fileId":        hash,
		"original": map[string]any{
			"filename": original,
		},
		"normalized": map[string]any{
			"filename":   filepath.Base(path),
			"sampleRate": SampleRate,
			"channels":   1,
			"encoding":   "pcm_s16le",
			"sizeBytes":  info.Size(),
		},
		"processing": map[string]any{
			"status":       ternary(skipped, "skipped", "done"), // skipped means already present
			"processingMs": time.Since(start).Milliseconds(),
		},
		"createdAt": time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func httpError(w http.ResponseWriter, statusCode int, code, msg string) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}
