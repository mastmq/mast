package bridge

import (
	"encoding/json"

	"github.com/mastmq/mast/internal/infra/store"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
)

// headerProps carries a message's MQTT 5 properties across the fabric, as
// JSON of [store.Properties].
//
// One header rather than one per property, because user property keys are
// arbitrary UTF-8 and may repeat, and a NATS header name can hold neither.
// It is absent when a message has no properties, so an MQTT 3 publish, which
// never has any, pays nothing for it.
const headerProps = "Mast-Props"

// messageProps extracts the properties that belong to the message from a
// mochi packet, or nil when there are none.
func messageProps(p packets.Properties) *store.Properties {
	out := store.Properties{
		PayloadFormat:   nil,
		MessageExpiry:   p.MessageExpiryInterval,
		ContentType:     p.ContentType,
		ResponseTopic:   p.ResponseTopic,
		CorrelationData: p.CorrelationData,
		User:            userProps(p.User),
	}

	if p.PayloadFormatFlag {
		format := p.PayloadFormat
		out.PayloadFormat = &format
	}

	if out.PayloadFormat == nil && out.MessageExpiry == 0 && out.ContentType == "" &&
		out.ResponseTopic == "" && len(out.CorrelationData) == 0 && len(out.User) == 0 {
		return nil
	}

	return &out
}

func userProps(in []packets.UserProperty) []store.UserProperty {
	if len(in) == 0 {
		return nil
	}

	out := make([]store.UserProperty, len(in))
	for i, u := range in {
		out[i] = store.UserProperty{Key: u.Key, Value: u.Val}
	}

	return out
}

// applyProps writes stored properties onto a packet about to be delivered.
//
// The expiry interval is deliberately not copied. mochi derives what it
// sends from the packet's absolute Expiry, which the caller sets from when
// the message was published, so the subscriber is told how long is left
// rather than the interval the publisher started with.
func applyProps(dst *packets.Properties, p *store.Properties) {
	if p == nil {
		return
	}

	if p.PayloadFormat != nil {
		dst.PayloadFormat = *p.PayloadFormat
		dst.PayloadFormatFlag = true
	}

	dst.ContentType = p.ContentType
	dst.ResponseTopic = p.ResponseTopic
	dst.CorrelationData = p.CorrelationData

	if len(p.User) > 0 {
		dst.User = make([]packets.UserProperty, len(p.User))
		for i, u := range p.User {
			dst.User[i] = packets.UserProperty{Key: u.Key, Val: u.Value}
		}
	}
}

// setPropsHeader attaches properties to a message bound for the fabric.
func setPropsHeader(msg *nats.Msg, p *store.Properties) error {
	if p == nil {
		return nil
	}

	encoded, err := json.Marshal(p)
	if err != nil {
		return err //nolint:wrapcheck // the caller names the message it was encoding
	}

	msg.Header.Set(headerProps, string(encoded))

	return nil
}

// propsFromHeader reads properties back off a message from the fabric. A
// header that does not parse is dropped rather than failing the delivery:
// the payload is what the subscriber is waiting for.
func propsFromHeader(header nats.Header) *store.Properties {
	raw := header.Get(headerProps)
	if raw == "" {
		return nil
	}

	var p store.Properties
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil
	}

	return &p
}
