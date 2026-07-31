package redisbus

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/sumit/rtmds/internal/tracing"
)

// wireEnvelope is the serialization format for events sent over Redis.
// TraceCtx carries the W3C trace context from the producer, enabling the
// subscriber to reconstruct the parent span and create a distributed trace.
type wireEnvelope struct {
	Symbol      string
	Type        string
	Seq         int64
	Timestamp   time.Time
	JSON        []byte
	Protobuf    []byte
	FlatBuffers []byte
	TraceCtx    tracing.TraceCarrier
}

func (e *wireEnvelope) MarshalBinary() ([]byte, error) {
	// Simple custom binary format to avoid jsoniter base64 overhead
	// and reflection-based generic serializers.

	// Format:
	// [2 bytes] symbol length
	// [N bytes] symbol
	// [2 bytes] type length
	// [N bytes] type
	// [8 bytes] seq
	// [8 bytes] timestamp unix nano
	// [4 bytes] JSON length
	// [N bytes] JSON
	// [4 bytes] Protobuf length
	// [N bytes] Protobuf
	// [4 bytes] FlatBuffers length
	// [N bytes] FlatBuffers
	// [2 bytes] trace context map size
	// For each map entry:
	//   [2 bytes] key length
	//   [N bytes] key
	//   [2 bytes] value length
	//   [N bytes] value

	var size int
	size += 2 + len(e.Symbol)
	size += 2 + len(e.Type)
	size += 8 + 8
	size += 4 + len(e.JSON)
	size += 4 + len(e.Protobuf)
	size += 4 + len(e.FlatBuffers)
	size += 2
	for k, v := range e.TraceCtx {
		size += 2 + len(k)
		size += 2 + len(v)
	}

	buf := make([]byte, size)
	offset := 0

	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(e.Symbol)))
	offset += 2
	copy(buf[offset:], e.Symbol)
	offset += len(e.Symbol)

	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(e.Type)))
	offset += 2
	copy(buf[offset:], e.Type)
	offset += len(e.Type)

	binary.LittleEndian.PutUint64(buf[offset:], uint64(e.Seq))
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], uint64(e.Timestamp.UnixNano()))
	offset += 8

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.JSON)))
	offset += 4
	copy(buf[offset:], e.JSON)
	offset += len(e.JSON)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.Protobuf)))
	offset += 4
	copy(buf[offset:], e.Protobuf)
	offset += len(e.Protobuf)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.FlatBuffers)))
	offset += 4
	copy(buf[offset:], e.FlatBuffers)
	offset += len(e.FlatBuffers)

	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(e.TraceCtx)))
	offset += 2
	for k, v := range e.TraceCtx {
		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(k)))
		offset += 2
		copy(buf[offset:], k)
		offset += len(k)

		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(v)))
		offset += 2
		copy(buf[offset:], v)
		offset += len(v)
	}

	return buf, nil
}

func (e *wireEnvelope) UnmarshalBinary(data []byte) error {
	if len(data) < 2 {
		return errors.New("invalid wire envelope: too short")
	}

	offset := 0
	symLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2
	if len(data) < offset+symLen {
		return errors.New("invalid wire envelope: symbol length")
	}
	e.Symbol = string(data[offset : offset+symLen])
	offset += symLen

	if len(data) < offset+2 {
		return errors.New("invalid wire envelope: type length header")
	}
	typLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2
	if len(data) < offset+typLen {
		return errors.New("invalid wire envelope: type length")
	}
	e.Type = string(data[offset : offset+typLen])
	offset += typLen

	if len(data) < offset+16 {
		return errors.New("invalid wire envelope: seq and timestamp")
	}
	e.Seq = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	tsNano := int64(binary.LittleEndian.Uint64(data[offset:]))
	e.Timestamp = time.Unix(0, tsNano)
	offset += 8

	if len(data) < offset+4 {
		return errors.New("invalid wire envelope: json length header")
	}
	jsonLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if len(data) < offset+jsonLen {
		return errors.New("invalid wire envelope: json length")
	}
	e.JSON = make([]byte, jsonLen)
	copy(e.JSON, data[offset:offset+jsonLen])
	offset += jsonLen

	if len(data) < offset+4 {
		return errors.New("invalid wire envelope: protobuf length header")
	}
	protoLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if len(data) < offset+protoLen {
		return errors.New("invalid wire envelope: protobuf length")
	}
	e.Protobuf = make([]byte, protoLen)
	copy(e.Protobuf, data[offset:offset+protoLen])
	offset += protoLen

	if len(data) < offset+4 {
		return errors.New("invalid wire envelope: flatbuffers length header")
	}
	flatLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if len(data) < offset+flatLen {
		return errors.New("invalid wire envelope: flatbuffers length")
	}
	e.FlatBuffers = make([]byte, flatLen)
	copy(e.FlatBuffers, data[offset:offset+flatLen])
	offset += flatLen

	if len(data) < offset+2 {
		return errors.New("invalid wire envelope: trace ctx length")
	}
	mapLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	if mapLen > 0 {
		e.TraceCtx = make(tracing.TraceCarrier, mapLen)
		for i := 0; i < mapLen; i++ {
			if len(data) < offset+2 {
				return errors.New("invalid wire envelope: trace key length")
			}
			kLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2
			if len(data) < offset+kLen {
				return errors.New("invalid wire envelope: trace key bounds")
			}
			k := string(data[offset : offset+kLen])
			offset += kLen

			if len(data) < offset+2 {
				return errors.New("invalid wire envelope: trace val length")
			}
			vLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2
			if len(data) < offset+vLen {
				return errors.New("invalid wire envelope: trace val bounds")
			}
			v := string(data[offset : offset+vLen])
			offset += vLen

			e.TraceCtx[k] = v
		}
	}

	return nil
}
