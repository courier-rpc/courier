package codec

import "encoding/binary"

// ResponseCodeOK indicates a successful response.
const ResponseCodeOK uint32 = 0

// ResponseFrame represents a decoded RPC response on the wire.
type ResponseFrame struct {
	Length      uint32
	Version     uint16
	Compression Compression
	RequestID   [16]byte
	Code        uint32
	Payload     []byte
}

// EncodeResponse serializes requestID, code and payload into the wire format:
//
//	[4B length][16B requestID][4B code][...payload]
func EncodeResponse(requestID [16]byte, code uint32, payload []byte) []byte {
	length := uint32(ResponseHeaderLen + len(payload))
	b := make([]byte, length)
	binary.BigEndian.PutUint32(b[0:4], length)
	copy(b[4:20], requestID[:])
	binary.BigEndian.PutUint32(b[20:24], code)
	copy(b[24:], payload)
	return b
}

// DecodeResponse parses a raw byte slice into a ResponseFrame.
func DecodeResponse(data []byte) (*ResponseFrame, error) {
	return DecodeResponseWithVersion(data, ProtocolVersion)
}

// DecodeResponseWithVersion decodes using the originating request's version.
// Response frames retain their legacy request-ID offset and do not carry a version.
func DecodeResponseWithVersion(data []byte, version uint16) (*ResponseFrame, error) {
	headerLen := ResponseHeaderLen
	switch version {
	case ProtocolVersion:
	case CompressionProtocolVersion:
		headerLen = ResponseHeaderLenV2
	default:
		return nil, ErrUnsupportedVersion
	}
	if len(data) < headerLen {
		return nil, ErrFrameTooShort
	}
	length := binary.BigEndian.Uint32(data[:4])
	if length < uint32(headerLen) {
		return nil, ErrInvalidLength
	}
	if uint64(len(data)) < uint64(length) {
		return nil, ErrTruncatedFrame
	}
	frame := &ResponseFrame{Length: length, Version: version, Code: binary.BigEndian.Uint32(data[20:24])}
	copy(frame.RequestID[:], data[4:20])
	frame.Payload = append([]byte(nil), data[headerLen:length]...)
	if version == CompressionProtocolVersion {
		frame.Compression = Compression(data[24])
		var err error
		frame.Payload, err = decompressPayload(frame.Payload, frame.Compression)
		if err != nil {
			return nil, err
		}
	}
	return frame, nil
}

// EncodeResponseWithCompression writes a v2 response matching a v2 request.
func EncodeResponseWithCompression(requestID [16]byte, code uint32, payload []byte, algorithm Compression) ([]byte, error) {
	body, err := compressPayload(payload, algorithm)
	if err != nil {
		return nil, err
	}
	b := make([]byte, ResponseHeaderLenV2+len(body))
	binary.BigEndian.PutUint32(b[:4], uint32(len(b)))
	copy(b[4:20], requestID[:])
	binary.BigEndian.PutUint32(b[20:24], code)
	b[24] = byte(algorithm)
	copy(b[25:], body)
	return b, nil
}
