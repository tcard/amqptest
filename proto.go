// Copyright (c) 2012, Sean Treadway, SoundCloud Ltd.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// Redistributions of source code must retain the above copyright notice, this
// list of conditions and the following disclaimer.
//
// Redistributions in binary form must reproduce the above copyright notice, this
// list of conditions and the following disclaimer in the documentation and/or
// other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package amqptest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/streadway/amqp"
)

func WriteFrame(w io.Writer, frame frame) (err error) {
	if err = frame.write(w); err != nil {
		return
	}

	if buf, ok := w.(*bufio.Writer); ok {
		err = buf.Flush()
	}

	return
}

func (me *methodFrame) write(w io.Writer) (err error) {
	var payload bytes.Buffer

	if me.Method == nil {
		return errors.New("malformed frame: missing method")
	}

	class, method := me.Method.id()

	if err = binary.Write(&payload, binary.BigEndian, class); err != nil {
		return
	}

	if err = binary.Write(&payload, binary.BigEndian, method); err != nil {
		return
	}

	if err = me.Method.write(&payload); err != nil {
		return
	}

	return writeFrame(w, frameMethod, me.ChannelId, payload.Bytes())
}

// Heartbeat
//
// Payload is empty
func (me *heartbeatFrame) write(w io.Writer) (err error) {
	return writeFrame(w, frameHeartbeat, me.ChannelId, []byte{})
}

// CONTENT HEADER
// 0          2        4           12               14
// +----------+--------+-----------+----------------+------------- - -
// | class-id | weight | body size | property flags | property list...
// +----------+--------+-----------+----------------+------------- - -
//    short     short    long long       short        remainder...
//
func (me *headerFrame) write(w io.Writer) (err error) {
	var payload bytes.Buffer
	var zeroTime time.Time

	if err = binary.Write(&payload, binary.BigEndian, me.ClassId); err != nil {
		return
	}

	if err = binary.Write(&payload, binary.BigEndian, me.weight); err != nil {
		return
	}

	if err = binary.Write(&payload, binary.BigEndian, me.Size); err != nil {
		return
	}

	// First pass will build the mask to be serialized, second pass will serialize
	// each of the fields that appear in the mask.

	var mask uint16

	if len(me.Properties.ContentType) > 0 {
		mask = mask | flagContentType
	}
	if len(me.Properties.ContentEncoding) > 0 {
		mask = mask | flagContentEncoding
	}
	if me.Properties.Headers != nil && len(me.Properties.Headers) > 0 {
		mask = mask | flagHeaders
	}
	if me.Properties.DeliveryMode > 0 {
		mask = mask | flagDeliveryMode
	}
	if me.Properties.Priority > 0 {
		mask = mask | flagPriority
	}
	if len(me.Properties.CorrelationId) > 0 {
		mask = mask | flagCorrelationId
	}
	if len(me.Properties.ReplyTo) > 0 {
		mask = mask | flagReplyTo
	}
	if len(me.Properties.Expiration) > 0 {
		mask = mask | flagExpiration
	}
	if len(me.Properties.MessageId) > 0 {
		mask = mask | flagMessageId
	}
	if me.Properties.Timestamp != zeroTime {
		mask = mask | flagTimestamp
	}
	if len(me.Properties.Type) > 0 {
		mask = mask | flagType
	}
	if len(me.Properties.UserId) > 0 {
		mask = mask | flagUserId
	}
	if len(me.Properties.AppId) > 0 {
		mask = mask | flagAppId
	}

	if err = binary.Write(&payload, binary.BigEndian, mask); err != nil {
		return
	}

	if hasProperty(mask, flagContentType) {
		if err = writeShortstr(&payload, me.Properties.ContentType); err != nil {
			return
		}
	}
	if hasProperty(mask, flagContentEncoding) {
		if err = writeShortstr(&payload, me.Properties.ContentEncoding); err != nil {
			return
		}
	}
	if hasProperty(mask, flagHeaders) {
		if err = writeTable(&payload, me.Properties.Headers); err != nil {
			return
		}
	}
	if hasProperty(mask, flagDeliveryMode) {
		if err = binary.Write(&payload, binary.BigEndian, me.Properties.DeliveryMode); err != nil {
			return
		}
	}
	if hasProperty(mask, flagPriority) {
		if err = binary.Write(&payload, binary.BigEndian, me.Properties.Priority); err != nil {
			return
		}
	}
	if hasProperty(mask, flagCorrelationId) {
		if err = writeShortstr(&payload, me.Properties.CorrelationId); err != nil {
			return
		}
	}
	if hasProperty(mask, flagReplyTo) {
		if err = writeShortstr(&payload, me.Properties.ReplyTo); err != nil {
			return
		}
	}
	if hasProperty(mask, flagExpiration) {
		if err = writeShortstr(&payload, me.Properties.Expiration); err != nil {
			return
		}
	}
	if hasProperty(mask, flagMessageId) {
		if err = writeShortstr(&payload, me.Properties.MessageId); err != nil {
			return
		}
	}
	if hasProperty(mask, flagTimestamp) {
		if err = binary.Write(&payload, binary.BigEndian, uint64(me.Properties.Timestamp.Unix())); err != nil {
			return
		}
	}
	if hasProperty(mask, flagType) {
		if err = writeShortstr(&payload, me.Properties.Type); err != nil {
			return
		}
	}
	if hasProperty(mask, flagUserId) {
		if err = writeShortstr(&payload, me.Properties.UserId); err != nil {
			return
		}
	}
	if hasProperty(mask, flagAppId) {
		if err = writeShortstr(&payload, me.Properties.AppId); err != nil {
			return
		}
	}

	return writeFrame(w, frameHeader, me.ChannelId, payload.Bytes())
}

// Body
//
// Payload is one byterange from the full body who's size is declared in the
// Header frame
func (me *bodyFrame) write(w io.Writer) (err error) {
	return writeFrame(w, frameBody, me.ChannelId, me.Body)
}

func writeFrame(w io.Writer, typ uint8, channel uint16, payload []byte) (err error) {
	end := []byte{frameEnd}
	size := uint(len(payload))

	_, err = w.Write([]byte{
		byte(typ),
		byte((channel & 0xff00) >> 8),
		byte((channel & 0x00ff) >> 0),
		byte((size & 0xff000000) >> 24),
		byte((size & 0x00ff0000) >> 16),
		byte((size & 0x0000ff00) >> 8),
		byte((size & 0x000000ff) >> 0),
	})

	if err != nil {
		return
	}

	if _, err = w.Write(payload); err != nil {
		return
	}

	if _, err = w.Write(end); err != nil {
		return
	}

	return
}

func writeShortstr(w io.Writer, s string) (err error) {
	b := []byte(s)

	var length uint8 = uint8(len(b))

	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}

	if _, err = w.Write(b[:length]); err != nil {
		return
	}

	return
}

func writeLongstr(w io.Writer, s string) (err error) {
	b := []byte(s)

	var length uint32 = uint32(len(b))

	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}

	if _, err = w.Write(b[:length]); err != nil {
		return
	}

	return
}

/*
'A': []interface{}
'D': Decimal
'F': amqp.Table
'I': int32
'S': string
'T': time.Time
'V': nil
'b': byte
'd': float64
'f': float32
'l': int64
's': int16
't': bool
'x': []byte
*/
func writeField(w io.Writer, value interface{}) (err error) {
	var buf [9]byte
	var enc []byte

	switch v := value.(type) {
	case bool:
		buf[0] = 't'
		if v {
			buf[1] = byte(1)
		} else {
			buf[1] = byte(0)
		}
		enc = buf[:2]

	case byte:
		buf[0] = 'b'
		buf[1] = byte(v)
		enc = buf[:2]

	case int16:
		buf[0] = 's'
		binary.BigEndian.PutUint16(buf[1:3], uint16(v))
		enc = buf[:3]

	case int32:
		buf[0] = 'I'
		binary.BigEndian.PutUint32(buf[1:5], uint32(v))
		enc = buf[:5]

	case int64:
		buf[0] = 'l'
		binary.BigEndian.PutUint64(buf[1:9], uint64(v))
		enc = buf[:9]

	case float32:
		buf[0] = 'f'
		binary.BigEndian.PutUint32(buf[1:5], math.Float32bits(v))
		enc = buf[:5]

	case float64:
		buf[0] = 'd'
		binary.BigEndian.PutUint64(buf[1:9], math.Float64bits(v))
		enc = buf[:9]

	case amqp.Decimal:
		buf[0] = 'D'
		buf[1] = byte(v.Scale)
		binary.BigEndian.PutUint32(buf[2:6], uint32(v.Value))
		enc = buf[:6]

	case string:
		buf[0] = 'S'
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(v)))
		enc = append(buf[:5], []byte(v)...)

	case []interface{}: // field-array
		buf[0] = 'A'

		sec := new(bytes.Buffer)
		for _, val := range v {
			if err = writeField(sec, val); err != nil {
				return
			}
		}

		binary.BigEndian.PutUint32(buf[1:5], uint32(sec.Len()))
		if _, err = w.Write(buf[:5]); err != nil {
			return
		}

		if _, err = w.Write(sec.Bytes()); err != nil {
			return
		}

		return

	case time.Time:
		buf[0] = 'T'
		binary.BigEndian.PutUint64(buf[1:9], uint64(v.Unix()))
		enc = buf[:9]

	case amqp.Table:
		if _, err = w.Write([]byte{'F'}); err != nil {
			return
		}
		return writeTable(w, v)

	case []byte:
		buf[0] = 'x'
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(v)))
		if _, err = w.Write(buf[0:5]); err != nil {
			return
		}
		if _, err = w.Write(v); err != nil {
			return
		}
		return

	case nil:
		buf[0] = 'V'
		enc = buf[:1]

	default:
		return amqp.ErrFieldType
	}

	_, err = w.Write(enc)

	return
}

func writeTable(w io.Writer, table amqp.Table) (err error) {
	var buf bytes.Buffer

	for key, val := range table {
		if err = writeShortstr(&buf, key); err != nil {
			return
		}
		if err = writeField(&buf, val); err != nil {
			return
		}
	}

	return writeLongstr(w, string(buf.Bytes()))
}

/*
Reads a frame from an input stream and returns an interface that can be cast into
one of the following:

   methodFrame
   PropertiesFrame
   bodyFrame
   heartbeatFrame

2.3.5  frame Details

All frames consist of a header (7 octets), a payload of arbitrary size, and a
'frame-end' octet that detects malformed frames:

  0      1         3             7                  size+7 size+8
  +------+---------+-------------+  +------------+  +-----------+
  | type | channel |     size    |  |  payload   |  | frame-end |
  +------+---------+-------------+  +------------+  +-----------+
   octet   short         long         size octets       octet

To read a frame, we:
  1. Read the header and check the frame type and channel.
	2. Depending on the frame type, we read the payload and process it.
  3. Read the frame end octet.

In realistic implementations where performance is a concern, we would use
“read-ahead buffering” or

“gathering reads” to avoid doing three separate system calls to read a frame.
*/
func ReadFrame(r io.Reader) (frame frame, err error) {
	var scratch [7]byte

	if _, err = io.ReadFull(r, scratch[:7]); err != nil {
		return
	}

	typ := uint8(scratch[0])
	channel := binary.BigEndian.Uint16(scratch[1:3])
	size := binary.BigEndian.Uint32(scratch[3:7])

	switch typ {
	case frameMethod:
		if frame, err = parseMethodFrame(r, channel, size); err != nil {
			return
		}

	case frameHeader:
		if frame, err = parseHeaderFrame(r, channel, size); err != nil {
			return
		}

	case frameBody:
		if frame, err = parseBodyFrame(r, channel, size); err != nil {
			return nil, err
		}

	case frameHeartbeat:
		if frame, err = parseHeartbeatFrame(r, channel, size); err != nil {
			return
		}

	default:
		return nil, amqp.ErrFrame
	}

	if _, err = io.ReadFull(r, scratch[:1]); err != nil {
		return nil, err
	}

	if scratch[0] != frameEnd {
		return nil, amqp.ErrFrame
	}

	return
}

func readShortstr(r io.Reader) (v string, err error) {
	var length uint8
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}

	bytes := make([]byte, length)
	if _, err = io.ReadFull(r, bytes); err != nil {
		return
	}
	return string(bytes), nil
}

func readLongstr(r io.Reader) (v string, err error) {
	var length uint32
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}

	bytes := make([]byte, length)
	if _, err = io.ReadFull(r, bytes); err != nil {
		return
	}
	return string(bytes), nil
}

func readDecimal(r io.Reader) (v amqp.Decimal, err error) {
	if err = binary.Read(r, binary.BigEndian, &v.Scale); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &v.Value); err != nil {
		return
	}
	return
}

func readFloat32(r io.Reader) (v float32, err error) {
	if err = binary.Read(r, binary.BigEndian, &v); err != nil {
		return
	}
	return
}

func readFloat64(r io.Reader) (v float64, err error) {
	if err = binary.Read(r, binary.BigEndian, &v); err != nil {
		return
	}
	return
}

func readTimestamp(r io.Reader) (v time.Time, err error) {
	var sec int64
	if err = binary.Read(r, binary.BigEndian, &sec); err != nil {
		return
	}
	return time.Unix(sec, 0), nil
}

/*
'A': []interface{}
'D': Decimal
'F': amqp.Table
'I': int32
'S': string
'T': time.Time
'V': nil
'b': byte
'd': float64
'f': float32
'l': int64
's': int16
't': bool
'x': []byte
*/
func readField(r io.Reader) (v interface{}, err error) {
	var typ byte
	if err = binary.Read(r, binary.BigEndian, &typ); err != nil {
		return
	}

	switch typ {
	case 't':
		var value uint8
		if err = binary.Read(r, binary.BigEndian, &value); err != nil {
			return
		}
		return (value != 0), nil

	case 'b':
		var value [1]byte
		if _, err = io.ReadFull(r, value[0:1]); err != nil {
			return
		}
		return value[0], nil

	case 's':
		var value int16
		if err = binary.Read(r, binary.BigEndian, &value); err != nil {
			return
		}
		return value, nil

	case 'I':
		var value int32
		if err = binary.Read(r, binary.BigEndian, &value); err != nil {
			return
		}
		return value, nil

	case 'l':
		var value int64
		if err = binary.Read(r, binary.BigEndian, &value); err != nil {
			return
		}
		return value, nil

	case 'f':
		var value float32
		if err = binary.Read(r, binary.BigEndian, &value); err != nil {
			return
		}
		return value, nil

	case 'd':
		var value float64
		if err = binary.Read(r, binary.BigEndian, &value); err != nil {
			return
		}
		return value, nil

	case 'D':
		return readDecimal(r)

	case 'S':
		return readLongstr(r)

	case 'A':
		return readArray(r)

	case 'T':
		return readTimestamp(r)

	case 'F':
		return readTable(r)

	case 'x':
		var len int32
		if err = binary.Read(r, binary.BigEndian, &len); err != nil {
			return nil, err
		}

		value := make([]byte, len)
		if _, err = io.ReadFull(r, value); err != nil {
			return nil, err
		}
		return value, err

	case 'V':
		return nil, nil
	}

	return nil, amqp.ErrSyntax
}

/*
	Field tables are long strings that contain packed name-value pairs.  The
	name-value pairs are encoded as short string defining the name, and octet
	defining the values type and then the value itself.   The valid field types for
	tables are an extension of the native integer, bit, string, and timestamp
	types, and are shown in the grammar.  Multi-octet integer fields are always
	held in network byte order.
*/
func readTable(r io.Reader) (table amqp.Table, err error) {
	var nested bytes.Buffer
	var str string

	if str, err = readLongstr(r); err != nil {
		return
	}

	nested.Write([]byte(str))

	table = make(amqp.Table)

	for nested.Len() > 0 {
		var key string
		var value interface{}

		if key, err = readShortstr(&nested); err != nil {
			return
		}

		if value, err = readField(&nested); err != nil {
			return
		}

		table[key] = value
	}

	return
}

func readArray(r io.Reader) ([]interface{}, error) {
	var size uint32
	var err error

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}

	lim := &io.LimitedReader{R: r, N: int64(size)}
	arr := make([]interface{}, 0)
	var field interface{}

	for {
		if field, err = readField(lim); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		arr = append(arr, field)
	}

	return arr, nil
}

// Checks if this bit mask matches the flags bitset
func hasProperty(mask uint16, prop int) bool {
	return int(mask)&prop > 0
}

func parseHeaderFrame(r io.Reader, channel uint16, size uint32) (frame frame, err error) {
	hf := &headerFrame{
		ChannelId: channel,
	}

	if err = binary.Read(r, binary.BigEndian, &hf.ClassId); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &hf.weight); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &hf.Size); err != nil {
		return
	}

	var flags uint16

	if err = binary.Read(r, binary.BigEndian, &flags); err != nil {
		return
	}

	if hasProperty(flags, flagContentType) {
		if hf.Properties.ContentType, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagContentEncoding) {
		if hf.Properties.ContentEncoding, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagHeaders) {
		if hf.Properties.Headers, err = readTable(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagDeliveryMode) {
		if err = binary.Read(r, binary.BigEndian, &hf.Properties.DeliveryMode); err != nil {
			return
		}
	}
	if hasProperty(flags, flagPriority) {
		if err = binary.Read(r, binary.BigEndian, &hf.Properties.Priority); err != nil {
			return
		}
	}
	if hasProperty(flags, flagCorrelationId) {
		if hf.Properties.CorrelationId, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagReplyTo) {
		if hf.Properties.ReplyTo, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagExpiration) {
		if hf.Properties.Expiration, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagMessageId) {
		if hf.Properties.MessageId, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagTimestamp) {
		if hf.Properties.Timestamp, err = readTimestamp(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagType) {
		if hf.Properties.Type, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagUserId) {
		if hf.Properties.UserId, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagAppId) {
		if hf.Properties.AppId, err = readShortstr(r); err != nil {
			return
		}
	}
	if hasProperty(flags, flagReserved1) {
		if hf.Properties.reserved1, err = readShortstr(r); err != nil {
			return
		}
	}

	return hf, nil
}

func parseBodyFrame(r io.Reader, channel uint16, size uint32) (frame frame, err error) {
	bf := &bodyFrame{
		ChannelId: channel,
		Body:      make([]byte, size),
	}

	if _, err = io.ReadFull(r, bf.Body); err != nil {
		return nil, err
	}

	return bf, nil
}

var errHeartbeatPayload = errors.New("Heartbeats should not have a payload")

func parseHeartbeatFrame(r io.Reader, channel uint16, size uint32) (frame frame, err error) {
	hf := &heartbeatFrame{
		ChannelId: channel,
	}

	if size > 0 {
		return nil, errHeartbeatPayload
	}

	return hf, nil
}

// Error codes that can be sent from the server during a connection or
// channel exception or used by the client to indicate a class of error like
// ErrCredentials.  The text of the error is likely more interesting than
// these constants.
const (
	frameMethod        = 1
	frameHeader        = 2
	frameBody          = 3
	frameHeartbeat     = 8
	frameMinSize       = 4096
	frameEnd           = 206
	replySuccess       = 200
	ContentTooLarge    = 311
	NoRoute            = 312
	NoConsumers        = 313
	ConnectionForced   = 320
	InvalidPath        = 402
	AccessRefused      = 403
	NotFound           = 404
	ResourceLocked     = 405
	PreconditionFailed = 406
	FrameError         = 501
	SyntaxError        = 502
	CommandInvalid     = 503
	ChannelError       = 504
	UnexpectedFrame    = 505
	ResourceError      = 506
	NotAllowed         = 530
	NotImplemented     = 540
	InternalError      = 541
)

func isSoftExceptionCode(code int) bool {
	switch code {
	case 311:
		return true
	case 312:
		return true
	case 313:
		return true
	case 403:
		return true
	case 404:
		return true
	case 405:
		return true
	case 406:
		return true

	}
	return false
}

type connectionStart struct {
	VersionMajor     byte
	VersionMinor     byte
	ServerProperties amqp.Table
	Mechanisms       string
	Locales          string
}

func (me *connectionStart) id() (uint16, uint16) {
	return 10, 10
}

func (me *connectionStart) wait() bool {
	return true
}

func (me *connectionStart) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.VersionMajor); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, me.VersionMinor); err != nil {
		return
	}

	if err = writeTable(w, me.ServerProperties); err != nil {
		return
	}

	if err = writeLongstr(w, me.Mechanisms); err != nil {
		return
	}
	if err = writeLongstr(w, me.Locales); err != nil {
		return
	}

	return
}

func (me *connectionStart) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.VersionMajor); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &me.VersionMinor); err != nil {
		return
	}

	if me.ServerProperties, err = readTable(r); err != nil {
		return
	}

	if me.Mechanisms, err = readLongstr(r); err != nil {
		return
	}
	if me.Locales, err = readLongstr(r); err != nil {
		return
	}

	return
}

type connectionStartOk struct {
	ClientProperties amqp.Table
	Mechanism        string
	Response         string
	Locale           string
}

func (me *connectionStartOk) id() (uint16, uint16) {
	return 10, 11
}

func (me *connectionStartOk) wait() bool {
	return true
}

func (me *connectionStartOk) write(w io.Writer) (err error) {

	if err = writeTable(w, me.ClientProperties); err != nil {
		return
	}

	if err = writeShortstr(w, me.Mechanism); err != nil {
		return
	}

	if err = writeLongstr(w, me.Response); err != nil {
		return
	}

	if err = writeShortstr(w, me.Locale); err != nil {
		return
	}

	return
}

func (me *connectionStartOk) read(r io.Reader) (err error) {

	if me.ClientProperties, err = readTable(r); err != nil {
		return
	}

	if me.Mechanism, err = readShortstr(r); err != nil {
		return
	}

	if me.Response, err = readLongstr(r); err != nil {
		return
	}

	if me.Locale, err = readShortstr(r); err != nil {
		return
	}

	return
}

type connectionSecure struct {
	Challenge string
}

func (me *connectionSecure) id() (uint16, uint16) {
	return 10, 20
}

func (me *connectionSecure) wait() bool {
	return true
}

func (me *connectionSecure) write(w io.Writer) (err error) {

	if err = writeLongstr(w, me.Challenge); err != nil {
		return
	}

	return
}

func (me *connectionSecure) read(r io.Reader) (err error) {

	if me.Challenge, err = readLongstr(r); err != nil {
		return
	}

	return
}

type connectionSecureOk struct {
	Response string
}

func (me *connectionSecureOk) id() (uint16, uint16) {
	return 10, 21
}

func (me *connectionSecureOk) wait() bool {
	return true
}

func (me *connectionSecureOk) write(w io.Writer) (err error) {

	if err = writeLongstr(w, me.Response); err != nil {
		return
	}

	return
}

func (me *connectionSecureOk) read(r io.Reader) (err error) {

	if me.Response, err = readLongstr(r); err != nil {
		return
	}

	return
}

type connectionTune struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (me *connectionTune) id() (uint16, uint16) {
	return 10, 30
}

func (me *connectionTune) wait() bool {
	return true
}

func (me *connectionTune) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.ChannelMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.FrameMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.Heartbeat); err != nil {
		return
	}

	return
}

func (me *connectionTune) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.ChannelMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.FrameMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.Heartbeat); err != nil {
		return
	}

	return
}

type connectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (me *connectionTuneOk) id() (uint16, uint16) {
	return 10, 31
}

func (me *connectionTuneOk) wait() bool {
	return true
}

func (me *connectionTuneOk) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.ChannelMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.FrameMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.Heartbeat); err != nil {
		return
	}

	return
}

func (me *connectionTuneOk) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.ChannelMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.FrameMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.Heartbeat); err != nil {
		return
	}

	return
}

type connectionOpen struct {
	VirtualHost string
	reserved1   string
	reserved2   bool
}

func (me *connectionOpen) id() (uint16, uint16) {
	return 10, 40
}

func (me *connectionOpen) wait() bool {
	return true
}

func (me *connectionOpen) write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, me.VirtualHost); err != nil {
		return
	}
	if err = writeShortstr(w, me.reserved1); err != nil {
		return
	}

	if me.reserved2 {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *connectionOpen) read(r io.Reader) (err error) {
	var bits byte

	if me.VirtualHost, err = readShortstr(r); err != nil {
		return
	}
	if me.reserved1, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.reserved2 = (bits&(1<<0) > 0)

	return
}

type connectionOpenOk struct {
	reserved1 string
}

func (me *connectionOpenOk) id() (uint16, uint16) {
	return 10, 41
}

func (me *connectionOpenOk) wait() bool {
	return true
}

func (me *connectionOpenOk) write(w io.Writer) (err error) {

	if err = writeShortstr(w, me.reserved1); err != nil {
		return
	}

	return
}

func (me *connectionOpenOk) read(r io.Reader) (err error) {

	if me.reserved1, err = readShortstr(r); err != nil {
		return
	}

	return
}

type connectionClose struct {
	ReplyCode uint16
	ReplyText string
	ClassId   uint16
	MethodId  uint16
}

func (me *connectionClose) id() (uint16, uint16) {
	return 10, 50
}

func (me *connectionClose) wait() bool {
	return true
}

func (me *connectionClose) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.ReplyCode); err != nil {
		return
	}

	if err = writeShortstr(w, me.ReplyText); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.ClassId); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, me.MethodId); err != nil {
		return
	}

	return
}

func (me *connectionClose) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.ReplyCode); err != nil {
		return
	}

	if me.ReplyText, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.ClassId); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &me.MethodId); err != nil {
		return
	}

	return
}

type connectionCloseOk struct {
}

func (me *connectionCloseOk) id() (uint16, uint16) {
	return 10, 51
}

func (me *connectionCloseOk) wait() bool {
	return true
}

func (me *connectionCloseOk) write(w io.Writer) (err error) {

	return
}

func (me *connectionCloseOk) read(r io.Reader) (err error) {

	return
}

type connectionBlocked struct {
	Reason string
}

func (me *connectionBlocked) id() (uint16, uint16) {
	return 10, 60
}

func (me *connectionBlocked) wait() bool {
	return false
}

func (me *connectionBlocked) write(w io.Writer) (err error) {

	if err = writeShortstr(w, me.Reason); err != nil {
		return
	}

	return
}

func (me *connectionBlocked) read(r io.Reader) (err error) {

	if me.Reason, err = readShortstr(r); err != nil {
		return
	}

	return
}

type connectionUnblocked struct {
}

func (me *connectionUnblocked) id() (uint16, uint16) {
	return 10, 61
}

func (me *connectionUnblocked) wait() bool {
	return false
}

func (me *connectionUnblocked) write(w io.Writer) (err error) {

	return
}

func (me *connectionUnblocked) read(r io.Reader) (err error) {

	return
}

type channelOpen struct {
	reserved1 string
}

func (me *channelOpen) id() (uint16, uint16) {
	return 20, 10
}

func (me *channelOpen) wait() bool {
	return true
}

func (me *channelOpen) write(w io.Writer) (err error) {

	if err = writeShortstr(w, me.reserved1); err != nil {
		return
	}

	return
}

func (me *channelOpen) read(r io.Reader) (err error) {

	if me.reserved1, err = readShortstr(r); err != nil {
		return
	}

	return
}

type channelOpenOk struct {
	reserved1 string
}

func (me *channelOpenOk) id() (uint16, uint16) {
	return 20, 11
}

func (me *channelOpenOk) wait() bool {
	return true
}

func (me *channelOpenOk) write(w io.Writer) (err error) {

	if err = writeLongstr(w, me.reserved1); err != nil {
		return
	}

	return
}

func (me *channelOpenOk) read(r io.Reader) (err error) {

	if me.reserved1, err = readLongstr(r); err != nil {
		return
	}

	return
}

type channelFlow struct {
	Active bool
}

func (me *channelFlow) id() (uint16, uint16) {
	return 20, 20
}

func (me *channelFlow) wait() bool {
	return true
}

func (me *channelFlow) write(w io.Writer) (err error) {
	var bits byte

	if me.Active {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *channelFlow) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Active = (bits&(1<<0) > 0)

	return
}

type channelFlowOk struct {
	Active bool
}

func (me *channelFlowOk) id() (uint16, uint16) {
	return 20, 21
}

func (me *channelFlowOk) wait() bool {
	return false
}

func (me *channelFlowOk) write(w io.Writer) (err error) {
	var bits byte

	if me.Active {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *channelFlowOk) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Active = (bits&(1<<0) > 0)

	return
}

type channelClose struct {
	ReplyCode uint16
	ReplyText string
	ClassId   uint16
	MethodId  uint16
}

func (me *channelClose) id() (uint16, uint16) {
	return 20, 40
}

func (me *channelClose) wait() bool {
	return true
}

func (me *channelClose) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.ReplyCode); err != nil {
		return
	}

	if err = writeShortstr(w, me.ReplyText); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.ClassId); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, me.MethodId); err != nil {
		return
	}

	return
}

func (me *channelClose) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.ReplyCode); err != nil {
		return
	}

	if me.ReplyText, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.ClassId); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &me.MethodId); err != nil {
		return
	}

	return
}

type channelCloseOk struct {
}

func (me *channelCloseOk) id() (uint16, uint16) {
	return 20, 41
}

func (me *channelCloseOk) wait() bool {
	return true
}

func (me *channelCloseOk) write(w io.Writer) (err error) {

	return
}

func (me *channelCloseOk) read(r io.Reader) (err error) {

	return
}

type ExchangeDeclare struct {
	reserved1  uint16
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  amqp.Table
}

func (me *ExchangeDeclare) id() (uint16, uint16) {
	return 40, 10
}

func (me *ExchangeDeclare) wait() bool {
	return true && !me.NoWait
}

func (me *ExchangeDeclare) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, me.Type); err != nil {
		return
	}

	if me.Passive {
		bits |= 1 << 0
	}

	if me.Durable {
		bits |= 1 << 1
	}

	if me.AutoDelete {
		bits |= 1 << 2
	}

	if me.Internal {
		bits |= 1 << 3
	}

	if me.NoWait {
		bits |= 1 << 4
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeTable(w, me.Arguments); err != nil {
		return
	}

	return
}

func (me *ExchangeDeclare) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if me.Type, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Passive = (bits&(1<<0) > 0)
	me.Durable = (bits&(1<<1) > 0)
	me.AutoDelete = (bits&(1<<2) > 0)
	me.Internal = (bits&(1<<3) > 0)
	me.NoWait = (bits&(1<<4) > 0)

	if me.Arguments, err = readTable(r); err != nil {
		return
	}

	return
}

type exchangeDeclareOk struct {
}

func (me *exchangeDeclareOk) id() (uint16, uint16) {
	return 40, 11
}

func (me *exchangeDeclareOk) wait() bool {
	return true
}

func (me *exchangeDeclareOk) write(w io.Writer) (err error) {

	return
}

func (me *exchangeDeclareOk) read(r io.Reader) (err error) {

	return
}

type ExchangeDelete struct {
	reserved1 uint16
	Exchange  string
	IfUnused  bool
	NoWait    bool
}

func (me *ExchangeDelete) id() (uint16, uint16) {
	return 40, 20
}

func (me *ExchangeDelete) wait() bool {
	return true && !me.NoWait
}

func (me *ExchangeDelete) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}

	if me.IfUnused {
		bits |= 1 << 0
	}

	if me.NoWait {
		bits |= 1 << 1
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *ExchangeDelete) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.IfUnused = (bits&(1<<0) > 0)
	me.NoWait = (bits&(1<<1) > 0)

	return
}

type exchangeDeleteOk struct {
}

func (me *exchangeDeleteOk) id() (uint16, uint16) {
	return 40, 21
}

func (me *exchangeDeleteOk) wait() bool {
	return true
}

func (me *exchangeDeleteOk) write(w io.Writer) (err error) {

	return
}

func (me *exchangeDeleteOk) read(r io.Reader) (err error) {

	return
}

type ExchangeBind struct {
	reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   amqp.Table
}

func (me *ExchangeBind) id() (uint16, uint16) {
	return 40, 30
}

func (me *ExchangeBind) wait() bool {
	return true && !me.NoWait
}

func (me *ExchangeBind) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Destination); err != nil {
		return
	}
	if err = writeShortstr(w, me.Source); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	if me.NoWait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeTable(w, me.Arguments); err != nil {
		return
	}

	return
}

func (me *ExchangeBind) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Destination, err = readShortstr(r); err != nil {
		return
	}
	if me.Source, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.NoWait = (bits&(1<<0) > 0)

	if me.Arguments, err = readTable(r); err != nil {
		return
	}

	return
}

type exchangeBindOk struct {
}

func (me *exchangeBindOk) id() (uint16, uint16) {
	return 40, 31
}

func (me *exchangeBindOk) wait() bool {
	return true
}

func (me *exchangeBindOk) write(w io.Writer) (err error) {

	return
}

func (me *exchangeBindOk) read(r io.Reader) (err error) {

	return
}

type ExchangeUnbind struct {
	reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   amqp.Table
}

func (me *ExchangeUnbind) id() (uint16, uint16) {
	return 40, 40
}

func (me *ExchangeUnbind) wait() bool {
	return true && !me.NoWait
}

func (me *ExchangeUnbind) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Destination); err != nil {
		return
	}
	if err = writeShortstr(w, me.Source); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	if me.NoWait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeTable(w, me.Arguments); err != nil {
		return
	}

	return
}

func (me *ExchangeUnbind) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Destination, err = readShortstr(r); err != nil {
		return
	}
	if me.Source, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.NoWait = (bits&(1<<0) > 0)

	if me.Arguments, err = readTable(r); err != nil {
		return
	}

	return
}

type exchangeUnbindOk struct {
}

func (me *exchangeUnbindOk) id() (uint16, uint16) {
	return 40, 51
}

func (me *exchangeUnbindOk) wait() bool {
	return true
}

func (me *exchangeUnbindOk) write(w io.Writer) (err error) {

	return
}

func (me *exchangeUnbindOk) read(r io.Reader) (err error) {

	return
}

type QueueDeclare struct {
	reserved1  uint16
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  amqp.Table
}

func (me *QueueDeclare) id() (uint16, uint16) {
	return 50, 10
}

func (me *QueueDeclare) wait() bool {
	return true && !me.NoWait
}

func (me *QueueDeclare) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}

	if me.Passive {
		bits |= 1 << 0
	}

	if me.Durable {
		bits |= 1 << 1
	}

	if me.Exclusive {
		bits |= 1 << 2
	}

	if me.AutoDelete {
		bits |= 1 << 3
	}

	if me.NoWait {
		bits |= 1 << 4
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeTable(w, me.Arguments); err != nil {
		return
	}

	return
}

func (me *QueueDeclare) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Passive = (bits&(1<<0) > 0)
	me.Durable = (bits&(1<<1) > 0)
	me.Exclusive = (bits&(1<<2) > 0)
	me.AutoDelete = (bits&(1<<3) > 0)
	me.NoWait = (bits&(1<<4) > 0)

	if me.Arguments, err = readTable(r); err != nil {
		return
	}

	return
}

type queueDeclareOk struct {
	Queue         string
	MessageCount  uint32
	ConsumerCount uint32
}

func (me *queueDeclareOk) id() (uint16, uint16) {
	return 50, 11
}

func (me *queueDeclareOk) wait() bool {
	return true
}

func (me *queueDeclareOk) write(w io.Writer) (err error) {

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.MessageCount); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, me.ConsumerCount); err != nil {
		return
	}

	return
}

func (me *queueDeclareOk) read(r io.Reader) (err error) {

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.MessageCount); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &me.ConsumerCount); err != nil {
		return
	}

	return
}

type QueueBind struct {
	reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  amqp.Table
}

func (me *QueueBind) id() (uint16, uint16) {
	return 50, 20
}

func (me *QueueBind) wait() bool {
	return true && !me.NoWait
}

func (me *QueueBind) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}
	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	if me.NoWait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeTable(w, me.Arguments); err != nil {
		return
	}

	return
}

func (me *QueueBind) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}
	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.NoWait = (bits&(1<<0) > 0)

	if me.Arguments, err = readTable(r); err != nil {
		return
	}

	return
}

type queueBindOk struct {
}

func (me *queueBindOk) id() (uint16, uint16) {
	return 50, 21
}

func (me *queueBindOk) wait() bool {
	return true
}

func (me *queueBindOk) write(w io.Writer) (err error) {

	return
}

func (me *queueBindOk) read(r io.Reader) (err error) {

	return
}

type queueUnbind struct {
	reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	Arguments  amqp.Table
}

func (me *queueUnbind) id() (uint16, uint16) {
	return 50, 50
}

func (me *queueUnbind) wait() bool {
	return true
}

func (me *queueUnbind) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}
	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	if err = writeTable(w, me.Arguments); err != nil {
		return
	}

	return
}

func (me *queueUnbind) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}
	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	if me.Arguments, err = readTable(r); err != nil {
		return
	}

	return
}

type queueUnbindOk struct {
}

func (me *queueUnbindOk) id() (uint16, uint16) {
	return 50, 51
}

func (me *queueUnbindOk) wait() bool {
	return true
}

func (me *queueUnbindOk) write(w io.Writer) (err error) {

	return
}

func (me *queueUnbindOk) read(r io.Reader) (err error) {

	return
}

type queuePurge struct {
	reserved1 uint16
	Queue     string
	NoWait    bool
}

func (me *queuePurge) id() (uint16, uint16) {
	return 50, 30
}

func (me *queuePurge) wait() bool {
	return true && !me.NoWait
}

func (me *queuePurge) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}

	if me.NoWait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *queuePurge) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.NoWait = (bits&(1<<0) > 0)

	return
}

type queuePurgeOk struct {
	MessageCount uint32
}

func (me *queuePurgeOk) id() (uint16, uint16) {
	return 50, 31
}

func (me *queuePurgeOk) wait() bool {
	return true
}

func (me *queuePurgeOk) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.MessageCount); err != nil {
		return
	}

	return
}

func (me *queuePurgeOk) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.MessageCount); err != nil {
		return
	}

	return
}

type queueDelete struct {
	reserved1 uint16
	Queue     string
	IfUnused  bool
	IfEmpty   bool
	NoWait    bool
}

func (me *queueDelete) id() (uint16, uint16) {
	return 50, 40
}

func (me *queueDelete) wait() bool {
	return true && !me.NoWait
}

func (me *queueDelete) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}

	if me.IfUnused {
		bits |= 1 << 0
	}

	if me.IfEmpty {
		bits |= 1 << 1
	}

	if me.NoWait {
		bits |= 1 << 2
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *queueDelete) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.IfUnused = (bits&(1<<0) > 0)
	me.IfEmpty = (bits&(1<<1) > 0)
	me.NoWait = (bits&(1<<2) > 0)

	return
}

type queueDeleteOk struct {
	MessageCount uint32
}

func (me *queueDeleteOk) id() (uint16, uint16) {
	return 50, 41
}

func (me *queueDeleteOk) wait() bool {
	return true
}

func (me *queueDeleteOk) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.MessageCount); err != nil {
		return
	}

	return
}

func (me *queueDeleteOk) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.MessageCount); err != nil {
		return
	}

	return
}

type basicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

func (me *basicQos) id() (uint16, uint16) {
	return 60, 10
}

func (me *basicQos) wait() bool {
	return true
}

func (me *basicQos) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.PrefetchSize); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.PrefetchCount); err != nil {
		return
	}

	if me.Global {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicQos) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.PrefetchSize); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.PrefetchCount); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Global = (bits&(1<<0) > 0)

	return
}

type basicQosOk struct {
}

func (me *basicQosOk) id() (uint16, uint16) {
	return 60, 11
}

func (me *basicQosOk) wait() bool {
	return true
}

func (me *basicQosOk) write(w io.Writer) (err error) {

	return
}

func (me *basicQosOk) read(r io.Reader) (err error) {

	return
}

type BasicConsume struct {
	reserved1   uint16
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   amqp.Table
}

func (me *BasicConsume) id() (uint16, uint16) {
	return 60, 20
}

func (me *BasicConsume) wait() bool {
	return true && !me.NoWait
}

func (me *BasicConsume) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}
	if err = writeShortstr(w, me.ConsumerTag); err != nil {
		return
	}

	if me.NoLocal {
		bits |= 1 << 0
	}

	if me.NoAck {
		bits |= 1 << 1
	}

	if me.Exclusive {
		bits |= 1 << 2
	}

	if me.NoWait {
		bits |= 1 << 3
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeTable(w, me.Arguments); err != nil {
		return
	}

	return
}

func (me *BasicConsume) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}
	if me.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.NoLocal = (bits&(1<<0) > 0)
	me.NoAck = (bits&(1<<1) > 0)
	me.Exclusive = (bits&(1<<2) > 0)
	me.NoWait = (bits&(1<<3) > 0)

	if me.Arguments, err = readTable(r); err != nil {
		return
	}

	return
}

type basicConsumeOk struct {
	ConsumerTag string
}

func (me *basicConsumeOk) id() (uint16, uint16) {
	return 60, 21
}

func (me *basicConsumeOk) wait() bool {
	return true
}

func (me *basicConsumeOk) write(w io.Writer) (err error) {

	if err = writeShortstr(w, me.ConsumerTag); err != nil {
		return
	}

	return
}

func (me *basicConsumeOk) read(r io.Reader) (err error) {

	if me.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	return
}

type basicCancel struct {
	ConsumerTag string
	NoWait      bool
}

func (me *basicCancel) id() (uint16, uint16) {
	return 60, 30
}

func (me *basicCancel) wait() bool {
	return true && !me.NoWait
}

func (me *basicCancel) write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, me.ConsumerTag); err != nil {
		return
	}

	if me.NoWait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicCancel) read(r io.Reader) (err error) {
	var bits byte

	if me.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.NoWait = (bits&(1<<0) > 0)

	return
}

type basicCancelOk struct {
	ConsumerTag string
}

func (me *basicCancelOk) id() (uint16, uint16) {
	return 60, 31
}

func (me *basicCancelOk) wait() bool {
	return true
}

func (me *basicCancelOk) write(w io.Writer) (err error) {

	if err = writeShortstr(w, me.ConsumerTag); err != nil {
		return
	}

	return
}

func (me *basicCancelOk) read(r io.Reader) (err error) {

	if me.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	return
}

type BasicPublish struct {
	reserved1  uint16
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Properties Properties
	Body       []byte
}

// Used by header frames to capture routing and header information
type Properties struct {
	ContentType     string     // MIME content type
	ContentEncoding string     // MIME content encoding
	Headers         amqp.Table // Application or header exchange table
	DeliveryMode    uint8      // queue implemention use - Transient (1) or Persistent (2)
	Priority        uint8      // queue implementation use - 0 to 9
	CorrelationId   string     // application use - correlation identifier
	ReplyTo         string     // application use - address to to reply to (ex: RPC)
	Expiration      string     // implementation use - message expiration spec
	MessageId       string     // application use - message identifier
	Timestamp       time.Time  // application use - message timestamp
	Type            string     // application use - message type name
	UserId          string     // application use - creating user id
	AppId           string     // application use - creating application
	reserved1       string     // was cluster-id - process for buffer consumption
}

func (me *BasicPublish) id() (uint16, uint16) {
	return 60, 40
}

func (me *BasicPublish) wait() bool {
	return false
}

func (me *BasicPublish) getContent() (Properties, []byte) {
	return me.Properties, me.Body
}

func (me *BasicPublish) setContent(props Properties, body []byte) {
	me.Properties, me.Body = props, body
}

func (me *BasicPublish) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	if me.Mandatory {
		bits |= 1 << 0
	}

	if me.Immediate {
		bits |= 1 << 1
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *BasicPublish) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Mandatory = (bits&(1<<0) > 0)
	me.Immediate = (bits&(1<<1) > 0)

	return
}

type basicReturn struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
	Properties Properties
	Body       []byte
}

func (me *basicReturn) id() (uint16, uint16) {
	return 60, 50
}

func (me *basicReturn) wait() bool {
	return false
}

func (me *basicReturn) getContent() (Properties, []byte) {
	return me.Properties, me.Body
}

func (me *basicReturn) setContent(props Properties, body []byte) {
	me.Properties, me.Body = props, body
}

func (me *basicReturn) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, me.ReplyCode); err != nil {
		return
	}

	if err = writeShortstr(w, me.ReplyText); err != nil {
		return
	}
	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	return
}

func (me *basicReturn) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &me.ReplyCode); err != nil {
		return
	}

	if me.ReplyText, err = readShortstr(r); err != nil {
		return
	}
	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	return
}

type BasicDeliver struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
	Properties  Properties
	Body        []byte
}

func (me *BasicDeliver) id() (uint16, uint16) {
	return 60, 60
}

func (me *BasicDeliver) wait() bool {
	return false
}

func (me *BasicDeliver) getContent() (Properties, []byte) {
	return me.Properties, me.Body
}

func (me *BasicDeliver) setContent(props Properties, body []byte) {
	me.Properties, me.Body = props, body
}

func (me *BasicDeliver) write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, me.ConsumerTag); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.DeliveryTag); err != nil {
		return
	}

	if me.Redelivered {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	return
}

func (me *BasicDeliver) read(r io.Reader) (err error) {
	var bits byte

	if me.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Redelivered = (bits&(1<<0) > 0)

	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	return
}

type basicGet struct {
	reserved1 uint16
	Queue     string
	NoAck     bool
}

func (me *basicGet) id() (uint16, uint16) {
	return 60, 70
}

func (me *basicGet) wait() bool {
	return true
}

func (me *basicGet) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, me.Queue); err != nil {
		return
	}

	if me.NoAck {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicGet) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.reserved1); err != nil {
		return
	}

	if me.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.NoAck = (bits&(1<<0) > 0)

	return
}

type basicGetOk struct {
	DeliveryTag  uint64
	Redelivered  bool
	Exchange     string
	RoutingKey   string
	MessageCount uint32
	Properties   Properties
	Body         []byte
}

func (me *basicGetOk) id() (uint16, uint16) {
	return 60, 71
}

func (me *basicGetOk) wait() bool {
	return true
}

func (me *basicGetOk) getContent() (Properties, []byte) {
	return me.Properties, me.Body
}

func (me *basicGetOk) setContent(props Properties, body []byte) {
	me.Properties, me.Body = props, body
}

func (me *basicGetOk) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.DeliveryTag); err != nil {
		return
	}

	if me.Redelivered {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeShortstr(w, me.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, me.RoutingKey); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, me.MessageCount); err != nil {
		return
	}

	return
}

func (me *basicGetOk) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Redelivered = (bits&(1<<0) > 0)

	if me.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if me.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &me.MessageCount); err != nil {
		return
	}

	return
}

type basicGetEmpty struct {
	reserved1 string
}

func (me *basicGetEmpty) id() (uint16, uint16) {
	return 60, 72
}

func (me *basicGetEmpty) wait() bool {
	return true
}

func (me *basicGetEmpty) write(w io.Writer) (err error) {

	if err = writeShortstr(w, me.reserved1); err != nil {
		return
	}

	return
}

func (me *basicGetEmpty) read(r io.Reader) (err error) {

	if me.reserved1, err = readShortstr(r); err != nil {
		return
	}

	return
}

type basicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

func (me *basicAck) id() (uint16, uint16) {
	return 60, 80
}

func (me *basicAck) wait() bool {
	return false
}

func (me *basicAck) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.DeliveryTag); err != nil {
		return
	}

	if me.Multiple {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicAck) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Multiple = (bits&(1<<0) > 0)

	return
}

type basicReject struct {
	DeliveryTag uint64
	Requeue     bool
}

func (me *basicReject) id() (uint16, uint16) {
	return 60, 90
}

func (me *basicReject) wait() bool {
	return false
}

func (me *basicReject) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.DeliveryTag); err != nil {
		return
	}

	if me.Requeue {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicReject) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Requeue = (bits&(1<<0) > 0)

	return
}

type basicRecoverAsync struct {
	Requeue bool
}

func (me *basicRecoverAsync) id() (uint16, uint16) {
	return 60, 100
}

func (me *basicRecoverAsync) wait() bool {
	return false
}

func (me *basicRecoverAsync) write(w io.Writer) (err error) {
	var bits byte

	if me.Requeue {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicRecoverAsync) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Requeue = (bits&(1<<0) > 0)

	return
}

type basicRecover struct {
	Requeue bool
}

func (me *basicRecover) id() (uint16, uint16) {
	return 60, 110
}

func (me *basicRecover) wait() bool {
	return true
}

func (me *basicRecover) write(w io.Writer) (err error) {
	var bits byte

	if me.Requeue {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicRecover) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Requeue = (bits&(1<<0) > 0)

	return
}

type basicRecoverOk struct {
}

func (me *basicRecoverOk) id() (uint16, uint16) {
	return 60, 111
}

func (me *basicRecoverOk) wait() bool {
	return true
}

func (me *basicRecoverOk) write(w io.Writer) (err error) {

	return
}

func (me *basicRecoverOk) read(r io.Reader) (err error) {

	return
}

type basicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

func (me *basicNack) id() (uint16, uint16) {
	return 60, 120
}

func (me *basicNack) wait() bool {
	return false
}

func (me *basicNack) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, me.DeliveryTag); err != nil {
		return
	}

	if me.Multiple {
		bits |= 1 << 0
	}

	if me.Requeue {
		bits |= 1 << 1
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *basicNack) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &me.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Multiple = (bits&(1<<0) > 0)
	me.Requeue = (bits&(1<<1) > 0)

	return
}

type txSelect struct {
}

func (me *txSelect) id() (uint16, uint16) {
	return 90, 10
}

func (me *txSelect) wait() bool {
	return true
}

func (me *txSelect) write(w io.Writer) (err error) {

	return
}

func (me *txSelect) read(r io.Reader) (err error) {

	return
}

type txSelectOk struct {
}

func (me *txSelectOk) id() (uint16, uint16) {
	return 90, 11
}

func (me *txSelectOk) wait() bool {
	return true
}

func (me *txSelectOk) write(w io.Writer) (err error) {

	return
}

func (me *txSelectOk) read(r io.Reader) (err error) {

	return
}

type txCommit struct {
}

func (me *txCommit) id() (uint16, uint16) {
	return 90, 20
}

func (me *txCommit) wait() bool {
	return true
}

func (me *txCommit) write(w io.Writer) (err error) {

	return
}

func (me *txCommit) read(r io.Reader) (err error) {

	return
}

type txCommitOk struct {
}

func (me *txCommitOk) id() (uint16, uint16) {
	return 90, 21
}

func (me *txCommitOk) wait() bool {
	return true
}

func (me *txCommitOk) write(w io.Writer) (err error) {

	return
}

func (me *txCommitOk) read(r io.Reader) (err error) {

	return
}

type txRollback struct {
}

func (me *txRollback) id() (uint16, uint16) {
	return 90, 30
}

func (me *txRollback) wait() bool {
	return true
}

func (me *txRollback) write(w io.Writer) (err error) {

	return
}

func (me *txRollback) read(r io.Reader) (err error) {

	return
}

type txRollbackOk struct {
}

func (me *txRollbackOk) id() (uint16, uint16) {
	return 90, 31
}

func (me *txRollbackOk) wait() bool {
	return true
}

func (me *txRollbackOk) write(w io.Writer) (err error) {

	return
}

func (me *txRollbackOk) read(r io.Reader) (err error) {

	return
}

type confirmSelect struct {
	Nowait bool
}

func (me *confirmSelect) id() (uint16, uint16) {
	return 85, 10
}

func (me *confirmSelect) wait() bool {
	return true
}

func (me *confirmSelect) write(w io.Writer) (err error) {
	var bits byte

	if me.Nowait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (me *confirmSelect) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	me.Nowait = (bits&(1<<0) > 0)

	return
}

type confirmSelectOk struct {
}

func (me *confirmSelectOk) id() (uint16, uint16) {
	return 85, 11
}

func (me *confirmSelectOk) wait() bool {
	return true
}

func (me *confirmSelectOk) write(w io.Writer) (err error) {

	return
}

func (me *confirmSelectOk) read(r io.Reader) (err error) {

	return
}

func parseMethodFrame(r io.Reader, channel uint16, size uint32) (f frame, err error) {
	mf := &methodFrame{
		ChannelId: channel,
	}

	if err = binary.Read(r, binary.BigEndian, &mf.ClassId); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &mf.MethodId); err != nil {
		return
	}

	switch mf.ClassId {

	case 10: // connection
		switch mf.MethodId {

		case 10: // connection start
			//fmt.Println("NextMethod: class:10 method:10")
			method := &connectionStart{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 11: // connection start-ok
			//fmt.Println("NextMethod: class:10 method:11")
			method := &connectionStartOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 20: // connection secure
			//fmt.Println("NextMethod: class:10 method:20")
			method := &connectionSecure{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 21: // connection secure-ok
			//fmt.Println("NextMethod: class:10 method:21")
			method := &connectionSecureOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 30: // connection tune
			//fmt.Println("NextMethod: class:10 method:30")
			method := &connectionTune{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 31: // connection tune-ok
			//fmt.Println("NextMethod: class:10 method:31")
			method := &connectionTuneOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 40: // connection open
			//fmt.Println("NextMethod: class:10 method:40")
			method := &connectionOpen{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 41: // connection open-ok
			//fmt.Println("NextMethod: class:10 method:41")
			method := &connectionOpenOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 50: // connection close
			//fmt.Println("NextMethod: class:10 method:50")
			method := &connectionClose{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 51: // connection close-ok
			//fmt.Println("NextMethod: class:10 method:51")
			method := &connectionCloseOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 60: // connection blocked
			//fmt.Println("NextMethod: class:10 method:60")
			method := &connectionBlocked{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 61: // connection unblocked
			//fmt.Println("NextMethod: class:10 method:61")
			method := &connectionUnblocked{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case 20: // channel
		switch mf.MethodId {

		case 10: // channel open
			//fmt.Println("NextMethod: class:20 method:10")
			method := &channelOpen{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 11: // channel open-ok
			//fmt.Println("NextMethod: class:20 method:11")
			method := &channelOpenOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 20: // channel flow
			//fmt.Println("NextMethod: class:20 method:20")
			method := &channelFlow{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 21: // channel flow-ok
			//fmt.Println("NextMethod: class:20 method:21")
			method := &channelFlowOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 40: // channel close
			//fmt.Println("NextMethod: class:20 method:40")
			method := &channelClose{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 41: // channel close-ok
			//fmt.Println("NextMethod: class:20 method:41")
			method := &channelCloseOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case 40: // exchange
		switch mf.MethodId {

		case 10: // exchange declare
			//fmt.Println("NextMethod: class:40 method:10")
			method := &ExchangeDeclare{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 11: // exchange declare-ok
			//fmt.Println("NextMethod: class:40 method:11")
			method := &exchangeDeclareOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 20: // exchange delete
			//fmt.Println("NextMethod: class:40 method:20")
			method := &ExchangeDelete{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 21: // exchange delete-ok
			//fmt.Println("NextMethod: class:40 method:21")
			method := &exchangeDeleteOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 30: // exchange bind
			//fmt.Println("NextMethod: class:40 method:30")
			method := &ExchangeBind{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 31: // exchange bind-ok
			//fmt.Println("NextMethod: class:40 method:31")
			method := &exchangeBindOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 40: // exchange unbind
			//fmt.Println("NextMethod: class:40 method:40")
			method := &ExchangeUnbind{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 51: // exchange unbind-ok
			//fmt.Println("NextMethod: class:40 method:51")
			method := &exchangeUnbindOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case 50: // queue
		switch mf.MethodId {

		case 10: // queue declare
			//fmt.Println("NextMethod: class:50 method:10")
			method := &QueueDeclare{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 11: // queue declare-ok
			//fmt.Println("NextMethod: class:50 method:11")
			method := &queueDeclareOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 20: // queue bind
			//fmt.Println("NextMethod: class:50 method:20")
			method := &QueueBind{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 21: // queue bind-ok
			//fmt.Println("NextMethod: class:50 method:21")
			method := &queueBindOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 50: // queue unbind
			//fmt.Println("NextMethod: class:50 method:50")
			method := &queueUnbind{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 51: // queue unbind-ok
			//fmt.Println("NextMethod: class:50 method:51")
			method := &queueUnbindOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 30: // queue purge
			//fmt.Println("NextMethod: class:50 method:30")
			method := &queuePurge{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 31: // queue purge-ok
			//fmt.Println("NextMethod: class:50 method:31")
			method := &queuePurgeOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 40: // queue delete
			//fmt.Println("NextMethod: class:50 method:40")
			method := &queueDelete{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 41: // queue delete-ok
			//fmt.Println("NextMethod: class:50 method:41")
			method := &queueDeleteOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case 60: // basic
		switch mf.MethodId {

		case 10: // basic qos
			//fmt.Println("NextMethod: class:60 method:10")
			method := &basicQos{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 11: // basic qos-ok
			//fmt.Println("NextMethod: class:60 method:11")
			method := &basicQosOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 20: // basic consume
			//fmt.Println("NextMethod: class:60 method:20")
			method := &BasicConsume{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 21: // basic consume-ok
			//fmt.Println("NextMethod: class:60 method:21")
			method := &basicConsumeOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 30: // basic cancel
			//fmt.Println("NextMethod: class:60 method:30")
			method := &basicCancel{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 31: // basic cancel-ok
			//fmt.Println("NextMethod: class:60 method:31")
			method := &basicCancelOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 40: // basic publish
			//fmt.Println("NextMethod: class:60 method:40")
			method := &BasicPublish{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 50: // basic return
			//fmt.Println("NextMethod: class:60 method:50")
			method := &basicReturn{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 60: // basic deliver
			//fmt.Println("NextMethod: class:60 method:60")
			method := &BasicDeliver{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 70: // basic get
			//fmt.Println("NextMethod: class:60 method:70")
			method := &basicGet{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 71: // basic get-ok
			//fmt.Println("NextMethod: class:60 method:71")
			method := &basicGetOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 72: // basic get-empty
			//fmt.Println("NextMethod: class:60 method:72")
			method := &basicGetEmpty{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 80: // basic ack
			//fmt.Println("NextMethod: class:60 method:80")
			method := &basicAck{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 90: // basic reject
			//fmt.Println("NextMethod: class:60 method:90")
			method := &basicReject{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 100: // basic recover-async
			//fmt.Println("NextMethod: class:60 method:100")
			method := &basicRecoverAsync{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 110: // basic recover
			//fmt.Println("NextMethod: class:60 method:110")
			method := &basicRecover{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 111: // basic recover-ok
			//fmt.Println("NextMethod: class:60 method:111")
			method := &basicRecoverOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 120: // basic nack
			//fmt.Println("NextMethod: class:60 method:120")
			method := &basicNack{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case 90: // tx
		switch mf.MethodId {

		case 10: // tx select
			//fmt.Println("NextMethod: class:90 method:10")
			method := &txSelect{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 11: // tx select-ok
			//fmt.Println("NextMethod: class:90 method:11")
			method := &txSelectOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 20: // tx commit
			//fmt.Println("NextMethod: class:90 method:20")
			method := &txCommit{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 21: // tx commit-ok
			//fmt.Println("NextMethod: class:90 method:21")
			method := &txCommitOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 30: // tx rollback
			//fmt.Println("NextMethod: class:90 method:30")
			method := &txRollback{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 31: // tx rollback-ok
			//fmt.Println("NextMethod: class:90 method:31")
			method := &txRollbackOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	case 85: // confirm
		switch mf.MethodId {

		case 10: // confirm select
			//fmt.Println("NextMethod: class:85 method:10")
			method := &confirmSelect{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		case 11: // confirm select-ok
			//fmt.Println("NextMethod: class:85 method:11")
			method := &confirmSelectOk{}
			if err = method.read(r); err != nil {
				return
			}
			mf.Method = method

		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}

	default:
		return nil, fmt.Errorf("Bad method frame, unknown class %d", mf.ClassId)
	}

	return mf, nil
}

type frame interface {
	write(io.Writer) error
	channel() uint16
}

/*
Method frames carry the high-level protocol commands (which we call "methods").
One method frame carries one command.  The method frame payload has this format:

  0          2           4
  +----------+-----------+-------------- - -
  | class-id | method-id | arguments...
  +----------+-----------+-------------- - -
     short      short    ...

To process a method frame, we:
 1. Read the method frame payload.
 2. Unpack it into a structure.  A given method always has the same structure,
 so we can unpack the method rapidly.  3. Check that the method is allowed in
 the current context.
 4. Check that the method arguments are valid.
 5. Execute the method.

Method frame bodies are constructed as a list of AMQP data fields (bits,
integers, strings and string tables).  The marshalling code is trivially
generated directly from the protocol specifications, and can be very rapid.
*/
type methodFrame struct {
	ChannelId uint16
	ClassId   uint16
	MethodId  uint16
	Method    message
}

func (me *methodFrame) channel() uint16 { return me.ChannelId }

/*
Heartbeating is a technique designed to undo one of TCP/IP's features, namely
its ability to recover from a broken physical connection by closing only after
a quite long time-out.  In some scenarios we need to know very rapidly if a
peer is disconnected or not responding for other reasons (e.g. it is looping).
Since heartbeating can be done at a low level, we implement this as a special
type of frame that peers exchange at the transport level, rather than as a
class method.
*/
type heartbeatFrame struct {
	ChannelId uint16
}

func (me *heartbeatFrame) channel() uint16 { return me.ChannelId }

/*
Certain methods (such as Basic.Publish, Basic.Deliver, etc.) are formally
defined as carrying content.  When a peer sends such a method frame, it always
follows it with a content header and zero or more content body frames.

A content header frame has this format:

    0          2        4           12               14
    +----------+--------+-----------+----------------+------------- - -
    | class-id | weight | body size | property flags | property list...
    +----------+--------+-----------+----------------+------------- - -
      short     short    long long       short        remainder...

We place content body in distinct frames (rather than including it in the
method) so that AMQP may support "zero copy" techniques in which content is
never marshalled or encoded.  We place the content Properties in their own
frame so that recipients can selectively discard contents they do not want to
process
*/
type headerFrame struct {
	ChannelId  uint16
	ClassId    uint16
	weight     uint16
	Size       uint64
	Properties Properties
}

func (me *headerFrame) channel() uint16 { return me.ChannelId }

/*
Content is the application data we carry from client-to-client via the AMQP
server.  Content is, roughly speaking, a set of Properties plus a binary data
part.  The set of allowed Properties are defined by the Basic class, and these
form the "content header frame".  The data can be any size, and MAY be broken
into several (or many) chunks, each forming a "content body frame".

Looking at the frames for a specific channel, as they pass on the wire, we
might see something like this:

		[method]
		[method] [header] [body] [body]
		[method]
		...
*/
type bodyFrame struct {
	ChannelId uint16
	Body      []byte
}

func (me *bodyFrame) channel() uint16 { return me.ChannelId }

type message interface {
	id() (uint16, uint16)
	wait() bool
	read(io.Reader) error
	write(io.Writer) error
}

type messageWithContent interface {
	message
	getContent() (Properties, []byte)
	setContent(Properties, []byte)
}

// The property flags are an array of bits that indicate the presence or
// absence of each property value in sequence.  The bits are ordered from most
// high to low - bit 15 indicates the first property.
const (
	flagContentType     = 0x8000
	flagContentEncoding = 0x4000
	flagHeaders         = 0x2000
	flagDeliveryMode    = 0x1000
	flagPriority        = 0x0800
	flagCorrelationId   = 0x0400
	flagReplyTo         = 0x0200
	flagExpiration      = 0x0100
	flagMessageId       = 0x0080
	flagTimestamp       = 0x0040
	flagType            = 0x0020
	flagUserId          = 0x0010
	flagAppId           = 0x0008
	flagReserved1       = 0x0004
)
