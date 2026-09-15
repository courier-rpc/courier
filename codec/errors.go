package codec

import "errors"

var (
	ErrUnsupportedVersion     = errors.New("codec: unsupported protocol version")
	ErrUnsupportedCompression = errors.New("codec: unsupported compression algorithm")
	ErrPayloadTooLarge        = errors.New("codec: payload exceeds 16 MiB limit")
	// ErrFrameTooShort is returned when the input data is shorter than the minimum header size.
	ErrFrameTooShort = errors.New("codec: frame too short")

	// ErrInvalidLength is returned when the length field in the header is invalid.
	ErrInvalidLength = errors.New("codec: invalid length field")

	// ErrTruncatedFrame is returned when the actual data length is less than the declared length.
	ErrTruncatedFrame = errors.New("codec: truncated frame")
)
