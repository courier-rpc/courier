package codec

const (
	// ProtocolVersion is the default (legacy) wire protocol version.
	ProtocolVersion uint16 = 1

	// RequestHeaderLen is the byte length of a request frame header:
	// 4 (length) + 2 (version) + 4 (cmd) + 16 (requestID) + 2 (extensionsLen).
	RequestHeaderLen = 28

	// ExtensionsLenOffset is the byte offset of the ExtensionsLen field.
	ExtensionsLenOffset = 26

	// ExtensionsDataOffset is the byte offset where extensions data begins.
	ExtensionsDataOffset = 28

	// ResponseHeaderLen is the byte length of a response frame header:
	// 4 (length) + 16 (requestID) + 4 (code).
	ResponseHeaderLen = 24
)

const (
	// CompressionProtocolVersion opts into the additional compression header byte.
	CompressionProtocolVersion uint16 = 2
	RequestHeaderLenV2                = RequestHeaderLen + 1
	ResponseHeaderLenV2               = ResponseHeaderLen + 1
)
