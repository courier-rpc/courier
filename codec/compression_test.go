package codec

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func TestCompressionRoundtrip(t *testing.T) {
	for _, algorithm := range []Compression{CompressionNone, CompressionGZIP} {
		for _, payload := range [][]byte{nil, []byte("hello 世界"), bytes.Repeat([]byte("status: online;"), 1000)} {
			id := [16]byte{1, 2, 3}
			ext := []byte{0xa, 0xb}
			encoded, err := EncodeRequestWithCompression(42, id, ext, payload, algorithm)
			if err != nil {
				t.Fatal(err)
			}
			if encoded[28] != byte(algorithm) || binary.BigEndian.Uint16(encoded[4:6]) != 2 {
				t.Fatal("incorrect request header")
			}
			req, err := DecodeRequest(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if req.Compression != algorithm || req.Cmd != 42 || req.RequestID != id || !bytes.Equal(req.Extensions, ext) || !bytes.Equal(req.Payload, payload) {
				t.Fatalf("request mismatch: %+v", req)
			}
			encoded, err = EncodeResponseWithCompression(id, 70000, payload, algorithm)
			if err != nil {
				t.Fatal(err)
			}
			if encoded[24] != byte(algorithm) {
				t.Fatal("incorrect response header")
			}
			resp, err := DecodeResponseWithVersion(encoded, 2)
			if err != nil || resp.Code != 70000 || resp.RequestID != id || resp.Compression != algorithm || !bytes.Equal(resp.Payload, payload) {
				t.Fatalf("response mismatch: %+v %v", resp, err)
			}
		}
	}
}

func TestCompressionRejectsMalformed(t *testing.T) {
	id := [16]byte{}
	if _, err := EncodeRequestWithCompression(1, id, nil, nil, 99); !errors.Is(err, ErrUnsupportedCompression) {
		t.Fatal(err)
	}
	req, _ := EncodeRequestWithCompression(1, id, nil, []byte("hello"), CompressionGZIP)
	for _, mutate := range []func([]byte) []byte{
		func(b []byte) []byte { b[28] = 99; return b },
		func(b []byte) []byte { b[len(b)-8] ^= 0xff; return b }, // CRC
		func(b []byte) []byte { b = b[:len(b)-1]; binary.BigEndian.PutUint32(b[:4], uint32(len(b))); return b },
		func(b []byte) []byte { binary.BigEndian.PutUint16(b[26:28], 65535); return b },
		func(b []byte) []byte { b[5] = 99; return b },
		func(b []byte) []byte { binary.BigEndian.PutUint32(b[:4], 28); return b },
	} {
		if _, err := DecodeRequest(mutate(bytes.Clone(req))); err == nil {
			t.Fatal("accepted malformed request")
		}
	}
	resp, _ := EncodeResponseWithCompression(id, 0, []byte("hello"), CompressionGZIP)
	resp[24] = 99
	if _, err := DecodeResponseWithVersion(resp, 2); !errors.Is(err, ErrUnsupportedCompression) {
		t.Fatal(err)
	}
	if _, err := DecodeResponseWithVersion(resp, 99); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestCompressionLimits(t *testing.T) {
	payload := make([]byte, MaxPayloadSize+1)
	if _, err := EncodeRequestWithCompression(1, [16]byte{}, nil, payload, CompressionGZIP); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	_, _ = w.Write(payload)
	_ = w.Close()
	if _, err := decompressPayload(compressed.Bytes(), CompressionGZIP); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatal(err)
	}
	if _, err := EncodeRequestWithCompression(1, [16]byte{}, make([]byte, 65536), nil, CompressionNone); !errors.Is(err, ErrInvalidLength) {
		t.Fatal(err)
	}
}

// Fixtures were independently encoded by Go, pako (JS), and archive (Dart).
func TestCrossLanguageCompression(t *testing.T) {
	data, err := os.ReadFile("testdata/compression.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Producer, Mode             string
		Version                    uint16
		Compression                Compression
		Payload, Request, Response []byte
	}
	if err = json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, v := range vectors {
		t.Run(v.Producer+"/"+v.Mode, func(t *testing.T) {
			req, err := DecodeRequest(v.Request)
			if err != nil {
				t.Fatal(err)
			}
			if req.Version != v.Version || req.Compression != v.Compression || req.Cmd != 70001 || !bytes.Equal(req.Payload, v.Payload) || !bytes.Equal(req.Extensions, []byte{170, 187}) {
				t.Fatal("request mismatch")
			}
			resp, err := DecodeResponseWithVersion(v.Response, v.Version)
			if err != nil {
				t.Fatal(err)
			}
			if resp.Compression != v.Compression || resp.RequestID != req.RequestID || resp.Code != 70000 || !bytes.Equal(resp.Payload, v.Payload) {
				t.Fatal("response mismatch")
			}
		})
	}
}
