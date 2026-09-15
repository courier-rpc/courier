package codec

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// Compression is the one-byte payload encoding in a v2 frame header.
type Compression uint8

const (
	CompressionNone Compression = 0
	CompressionGZIP Compression = 1
	// MaxPayloadSize bounds v2 payloads before compression and during decompression.
	MaxPayloadSize = 16 * 1024 * 1024
)

func compressPayload(payload []byte, algorithm Compression) ([]byte, error) {
	if algorithm != CompressionNone && algorithm != CompressionGZIP {
		return nil, ErrUnsupportedCompression
	}
	if len(payload) > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}
	if algorithm == CompressionNone {
		return payload, nil
	}
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(payload); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func decompressPayload(payload []byte, algorithm Compression) ([]byte, error) {
	if algorithm == CompressionNone {
		if len(payload) > MaxPayloadSize {
			return nil, ErrPayloadTooLarge
		}
		return payload, nil
	}
	if algorithm != CompressionGZIP {
		return nil, ErrUnsupportedCompression
	}
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("codec: invalid gzip payload: %w", err)
	}
	defer reader.Close()
	output, err := io.ReadAll(io.LimitReader(reader, MaxPayloadSize+1))
	if len(output) > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}
	if err != nil {
		return nil, fmt.Errorf("codec: invalid gzip payload: %w", err)
	}
	return output, nil
}
