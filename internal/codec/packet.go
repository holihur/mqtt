package codec

import "errors"

// Protocol versions
const (
	ProtocolV31  = 3
	ProtocolV311 = 4
	ProtocolV5   = 5
)

// Packet types
const (
	TypeCONNECT     = 1
	TypeCONNACK     = 2
	TypePUBLISH     = 3
	TypePUBACK      = 4
	TypePUBREC      = 5
	TypePUBREL      = 6
	TypePUBCOMP     = 7
	TypeSUBSCRIBE   = 8
	TypeSUBACK      = 9
	TypeUNSUBSCRIBE = 10
	TypeUNSUBACK    = 11
	TypePINGREQ     = 12
	TypePINGRESP    = 13
	TypeDISCONNECT  = 14
	TypeAUTH        = 15
)

// QoS levels
const (
	QoS0 = 0
	QoS1 = 1
	QoS2 = 2
)

var (
	ErrMalformedPacket     = errors.New("malformed packet")
	ErrUnsupportedProtocol = errors.New("unsupported protocol version")
	ErrInvalidQoS          = errors.New("invalid QoS")
	ErrProtocolViolation   = errors.New("protocol violation")
)

// Connect flags
type ConnectFlags struct {
	CleanSession bool // v3: clean session, v5: clean start
	WillFlag     bool
	WillQoS      byte
	WillRetain   bool
	PasswordFlag bool
	UsernameFlag bool
}

// Will holds will message (normalized across versions)
type Will struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	// v5 only
	DelayInterval uint32
	Properties    *Properties
}

// Packet is unified representation across v3 and v5
type Packet struct {
	Type    byte
	Version byte // 3,4,5
	Fixed   byte // raw fixed header first byte

	// CONNECT
	ProtocolName  string
	ProtocolLevel byte
	ConnectFlags  ConnectFlags
	KeepAlive     uint16
	ClientID      string
	Will          *Will
	Username      string
	Password      []byte
	Properties    *Properties // v5 CONNECT properties

	// CONNACK
	SessionPresent bool
	ReasonCode     byte // v5 reason code, v3 0-5 mapped
	ConnProperties *Properties

	// PUBLISH
	Topic    string
	PacketID uint16
	QoS      byte
	Retain   bool
	Dup      bool
	Payload  []byte
	PubProps *Properties // v5 publish properties

	// PUBACK/PUBREC/PUBREL/PUBCOMP
	Reason   byte
	AckProps *Properties

	// SUBSCRIBE
	Subscriptions []Subscription
	SubProps      *Properties
	// SUBACK
	SubackCodes []byte // v3: QoS or 0x80 failure, v5: reason codes
	SubackProps *Properties

	// UNSUBSCRIBE
	Topics        []string
	UnsubProps    *Properties
	UnsubackCodes []byte
	UnsubackProps *Properties

	// DISCONNECT
	DiscReason byte
	DiscProps  *Properties

	// AUTH (v5)
	AuthReason byte
	AuthProps  *Properties
}

type Subscription struct {
	Filter string
	QoS    byte
	// v5 options
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte // 0,1,2
	SubscriptionID    uint32
}

// Message is normalized internal message for routing
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Props   *Properties // v5 user properties etc.
	From    string
}

// Properties for MQTT5 (subset + generic handling)
type Properties struct {
	// Common
	SessionExpiryInterval *uint32
	ReceiveMaximum        *uint16
	MaximumPacketSize     *uint32
	TopicAliasMaximum     *uint16
	RequestResponseInfo   *byte
	RequestProblemInfo    *byte
	User                  []UserProperty
	AuthMethod            *string
	AuthData              []byte
	// Will / Publish
	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	TopicAlias             *uint16
	ResponseTopic          *string
	CorrelationData        []byte
	SubscriptionID         []uint32 // can be multiple for publish via multiple subs? spec says one but store as slice
	ContentType            *string
	// Connack / Disconnect
	ServerKeepAlive      *uint16
	AssignedClientID     *string
	ReasonString         *string
	WildcardSubAvailable *byte
	SubIDAvailable       *byte
	SharedSubAvailable   *byte
	RetainAvailable      *byte
	MaximumQoS           *byte
	ServerReference      *string
}

type UserProperty struct {
	Key string
	Val string
}

// Property IDs per MQTT5 spec
const (
	PropPayloadFormatIndicator = 0x01
	PropMessageExpiryInterval  = 0x02
	PropContentType            = 0x03
	PropResponseTopic          = 0x08
	PropCorrelationData        = 0x09
	PropSubscriptionID         = 0x0B
	PropSessionExpiryInterval  = 0x11
	PropAssignedClientID       = 0x12
	PropServerKeepAlive        = 0x13
	PropAuthMethod             = 0x15
	PropAuthData               = 0x16
	PropRequestProblemInfo     = 0x17
	PropWillDelayInterval      = 0x18
	PropRequestResponseInfo    = 0x19
	PropResponseInfo           = 0x1A
	PropServerReference        = 0x1C
	PropReasonString           = 0x1F
	PropReceiveMaximum         = 0x21
	PropTopicAliasMaximum      = 0x22
	PropTopicAlias             = 0x23
	PropMaximumQoS             = 0x24
	PropRetainAvailable        = 0x25
	PropUserProperty           = 0x26
	PropMaximumPacketSize      = 0x27
	PropWildcardSubAvailable   = 0x28
	PropSubIDAvailable         = 0x29
	PropSharedSubAvailable     = 0x2A
)
