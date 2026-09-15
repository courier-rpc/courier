package codec

import "encoding/binary"

// RequestFrame represents a decoded RPC request on the wire.
type RequestFrame struct {
	Length        uint32
	Version       uint16
	Compression   Compression
	Cmd           uint32
	RequestID     [16]byte
	ExtensionsLen uint16
	Extensions    []byte
	Payload       []byte
}

// EncodeRequest serializes cmd, requestID, extensions and payload into the wire format:
//
//	[4B length][2B version][4B cmd][16B requestID][2B extensionsLen][...extensions][...payload]
func EncodeRequest(cmd uint32, requestID [16]byte, extensions []byte, payload []byte) []byte {
	extLen := uint16(len(extensions))
	length := uint32(RequestHeaderLen + len(extensions) + len(payload))
	b := make([]byte, length)
	binary.BigEndian.PutUint32(b[0:4], length)
	binary.BigEndian.PutUint16(b[4:6], ProtocolVersion)
	binary.BigEndian.PutUint32(b[6:10], cmd)
	copy(b[10:26], requestID[:])
	binary.BigEndian.PutUint16(b[ExtensionsLenOffset:ExtensionsDataOffset], extLen)
	copy(b[ExtensionsDataOffset:], extensions)
	copy(b[ExtensionsDataOffset+len(extensions):], payload)
	return b
}

// DecodeRequest parses a raw byte slice into a RequestFrame.
func DecodeRequest(data []byte) (*RequestFrame, error) {
	if len(data) < RequestHeaderLen {
		return nil, ErrFrameTooShort
	}

	length := binary.BigEndian.Uint32(data[0:4])
	if length < uint32(RequestHeaderLen) {
		return nil, ErrInvalidLength
	}
	if uint32(len(data)) < length {
		return nil, ErrTruncatedFrame
	}

	frame := &RequestFrame{
		Length:        length,
		Version:       binary.BigEndian.Uint16(data[4:6]),
		Cmd:           binary.BigEndian.Uint32(data[6:10]),
		ExtensionsLen: binary.BigEndian.Uint16(data[ExtensionsLenOffset:ExtensionsDataOffset]),
	}
	copy(frame.RequestID[:], data[10:26])

	headerLen := RequestHeaderLen
	switch frame.Version {
	case ProtocolVersion:
	case CompressionProtocolVersion:
		headerLen = RequestHeaderLenV2
		if length < uint32(headerLen) {
			return nil, ErrInvalidLength
		}
		frame.Compression = Compression(data[RequestHeaderLen])
	default:
		return nil, ErrUnsupportedVersion
	}
	payloadOffset := headerLen + int(frame.ExtensionsLen)
	if payloadOffset > int(length) {
		return nil, ErrInvalidLength
	}
	frame.Extensions = append([]byte(nil), data[headerLen:payloadOffset]...)
	frame.Payload = append([]byte(nil), data[payloadOffset:length]...)
	if frame.Version == CompressionProtocolVersion {
		var err error
		frame.Payload, err = decompressPayload(frame.Payload, frame.Compression)
		if err != nil {
			return nil, err
		}
	}
	return frame, nil
}

// EncodeRequestWithCompression writes a v2 request. Only payload is compressed;
// extensions and all routing fields remain uncompressed.
func EncodeRequestWithCompression(cmd uint32, requestID [16]byte, extensions, payload []byte, algorithm Compression) ([]byte, error) {
	if len(extensions) > 65535 {
		return nil, ErrInvalidLength
	}
	body, err := compressPayload(payload, algorithm)
	if err != nil {
		return nil, err
	}
	b := make([]byte, RequestHeaderLenV2+len(extensions)+len(body))
	binary.BigEndian.PutUint32(b[:4], uint32(len(b)))
	binary.BigEndian.PutUint16(b[4:6], CompressionProtocolVersion)
	binary.BigEndian.PutUint32(b[6:10], cmd)
	copy(b[10:26], requestID[:])
	binary.BigEndian.PutUint16(b[26:28], uint16(len(extensions)))
	b[28] = byte(algorithm)
	copy(b[29:], extensions)
	copy(b[29+len(extensions):], body)
	return b, nil
}
