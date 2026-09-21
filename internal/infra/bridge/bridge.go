// Package bridge connects a mochi-mqtt server to core NATS.
//
// # How a message travels
//
// Every publish leaves the node. A client's PUBLISH is forwarded to NATS and
// mochi's own local fan-out is suppressed; the message comes back over the
// node's own subscription and is injected through the inline client, which is
// what actually delivers it to local subscribers. Routing therefore has one
// path rather than two, and a shared subscription spanning several nodes
// behaves correctly because the NATS queue group picks exactly one node.
//
// The cost is an in-process round trip for a same-node delivery. It rides
// net.Pipe rather than a socket, and buying uniform correctness with it is
// the right trade until there is a measurement saying otherwise.
//
// # How tenants stay apart
//
// mochi's topic tree has no notion of a tenant, so two tenants subscribing to
// "a/b" would share one node in it. mast mounts every client under its own
// tenant: inbound packets are rewritten to "<tenant>/<topic>" in OnPacketRead,
// before validation, ACL, and subscription all see them, and outbound PUBLISH
// packets are unmounted again in OnPacketEncode. The client never sees the
// prefix, and the isolation is structural rather than a matter of remembering
// to check.
//
// Mounting in OnPacketRead rather than OnSubscribe is deliberate: OnSubscribe
// runs before OnACLCheck, so rewriting there would show the ACL a prefixed
// filter on subscribe and a bare topic on publish. One insertion point keeps
// every downstream hook consistent.
package bridge

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/domain/topic"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/nats-io/nats.go"
)

// hookID names this hook in mochi's logs.
const hookID = "mast-bridge"

// ErrNoServer is returned when the hook is started without a server attached.
var ErrNoServer = errors.New("bridge: no mqtt server attached")

// Hook is the mochi hook that bridges MQTT onto NATS. It must be attached to
// a server with [Hook.Attach] before the server is served.
type Hook struct {
	mqtt.HookBase

	nc       *nats.Conn
	resolver tenant.Resolver
	policy   tenant.Policy
	log      *slog.Logger

	server *mqtt.Server
	subs   *registry

	mu      sync.RWMutex
	tenants map[string]tenant.ID
}

// New builds a bridge over an established NATS connection.
func New(nc *nats.Conn, resolver tenant.Resolver, policy tenant.Policy, log *slog.Logger) *Hook {
	h := &Hook{
		HookBase: mqtt.HookBase{},
		mu:       sync.RWMutex{},
		nc:       nc,
		resolver: resolver,
		policy:   policy,
		log:      log.With("component", "bridge"),
		server:   nil,
		subs:     nil,
		tenants:  make(map[string]tenant.ID),
	}
	h.subs = newRegistry(nc, h.onNATSMessage)

	return h
}

// Attach binds the hook to the server it will inject messages into.
func (h *Hook) Attach(server *mqtt.Server) { h.server = server }

// Subscriptions reports the number of live NATS subscriptions this node holds.
func (h *Hook) Subscriptions() int { return h.subs.count() }

// ID implements [mqtt.Hook].
func (h *Hook) ID() string { return hookID }

// Provides implements [mqtt.Hook].
func (h *Hook) Provides(b byte) bool {
	return slices.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnPacketRead,
		mqtt.OnACLCheck,
		mqtt.OnPublish,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnDisconnect,
		mqtt.OnPacketEncode,
	}, b)
}

// OnConnectAuthenticate resolves the connection to a tenant. A connection
// that resolves to no tenant is refused: there is no such thing as an
// untenanted client.
func (h *Hook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	id, err := h.resolver.Resolve(context.Background(), tenant.Credentials{
		ClientID:   cl.ID,
		Username:   string(pk.Connect.Username),
		Password:   pk.Connect.Password,
		RemoteAddr: cl.Net.Remote,
	})
	if err != nil {
		h.log.Debug("authentication refused", "client", cl.ID, "error", err)

		return false
	}

	h.mu.Lock()
	h.tenants[cl.ID] = id
	h.mu.Unlock()

	h.log.Debug("client authenticated", "client", cl.ID, "tenant", string(id))

	return true
}

// OnPacketRead mounts every inbound topic and filter under the client's
// tenant, before anything downstream inspects them.
func (h *Hook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	id, ok := h.tenantOf(cl)
	if !ok {
		return pk, nil // pre-CONNECT packets have no tenant yet
	}

	switch pk.FixedHeader.Type {
	case packets.Publish:
		pk.TopicName = mount(id, pk.TopicName)
	case packets.Subscribe, packets.Unsubscribe:
		for i := range pk.Filters {
			pk.Filters[i].Filter = mountFilter(id, pk.Filters[i].Filter)
		}
	}

	return pk, nil
}

// OnPacketEncode unmounts outbound PUBLISH topics so a client only ever sees
// the topic it actually used.
func (h *Hook) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type != packets.Publish {
		return pk
	}

	if id, ok := h.tenantOf(cl); ok {
		pk.TopicName = unmount(id, pk.TopicName)
	}

	return pk
}

// OnACLCheck consults the policy with the tenant resolved at authentication
// and the topic as the client wrote it, never as the client asserted it.
func (h *Hook) OnACLCheck(cl *mqtt.Client, mountedTopic string, write bool) bool {
	id, ok := h.tenantOf(cl)
	if !ok {
		return false
	}

	return h.policy.Allows(id, bare(id, mountedTopic), write)
}

// OnPublish forwards the message to NATS and tells mochi not to deliver it
// locally.
//
// It returns [packets.CodeSuccessIgnore] rather than ErrRejectPacket: both
// suppress local fan-out, but Ignore still lets mochi send the PUBACK, which
// is what QoS 1 will need.
func (h *Hook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		return pk, nil // our own injection on its way to local subscribers
	}

	id, ok := h.tenantOf(cl)
	if !ok {
		return pk, packets.ErrNotAuthorized
	}

	subject, err := topic.EncodeTopic(string(id), unmount(id, pk.TopicName))
	if err != nil {
		h.log.Warn("dropping publish with unencodable topic",
			"client", cl.ID, "tenant", string(id), "error", err)

		return pk, packets.CodeSuccessIgnore
	}

	if err := h.nc.Publish(subject, pk.Payload); err != nil {
		h.log.Error("forwarding publish to nats", "subject", subject, "error", err)
	}

	return pk, packets.CodeSuccessIgnore
}

// OnSubscribed opens the NATS subscriptions the granted filters require.
func (h *Hook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	id, ok := h.tenantOf(cl)
	if !ok {
		return
	}

	keys := make([]subKey, 0, len(pk.Filters))

	for i, filter := range pk.Filters {
		if i < len(reasonCodes) && reasonCodes[i] >= packets.ErrUnspecifiedError.Code {
			continue // not granted
		}

		keys = append(keys, h.keysFor(id, filter)...)
	}

	if err := h.subs.acquire(cl.ID, keys); err != nil {
		h.log.Error("acquiring nats subscriptions", "client", cl.ID, "error", err)
	}
}

// OnUnsubscribed closes the NATS subscriptions the dropped filters required.
func (h *Hook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	id, ok := h.tenantOf(cl)
	if !ok {
		return
	}

	keys := make([]subKey, 0, len(pk.Filters))
	for _, filter := range pk.Filters {
		keys = append(keys, h.keysFor(id, filter)...)
	}

	h.subs.release(cl.ID, keys)
}

// OnDisconnect releases everything the client held.
func (h *Hook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.subs.releaseAll(cl.ID)

	h.mu.Lock()
	delete(h.tenants, cl.ID)
	h.mu.Unlock()
}

// tenantOf returns the tenant resolved for a client at CONNECT.
func (h *Hook) tenantOf(cl *mqtt.Client) (tenant.ID, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	id, ok := h.tenants[cl.ID]

	return id, ok
}

// keysFor turns one granted MQTT filter into the NATS subscriptions it needs.
//
// A shared subscription becomes a queue group, which is the whole feature:
// NATS picks one node, that node's mochi picks one local group member, and
// the result is a correct cluster-wide shared subscription for free.
func (h *Hook) keysFor(id tenant.ID, filter packets.Subscription) []subKey {
	group, rest, shared := splitShare(filter.Filter)
	bareFilter := unmount(id, rest)

	subjects, err := topic.FilterSubjects(string(id), bareFilter)
	if err != nil {
		h.log.Warn("ignoring unencodable filter",
			"tenant", string(id), "filter", bareFilter, "error", err)

		return nil
	}

	var queue string
	if shared {
		queue = queueName(id, group)
	}

	keys := make([]subKey, 0, len(subjects))
	for _, subject := range subjects {
		keys = append(keys, subKey{subject: subject, queue: queue})
	}

	return keys
}

// onNATSMessage delivers a message that arrived from the fabric to this node's
// local subscribers.
func (h *Hook) onNATSMessage(msg *nats.Msg) {
	if h.server == nil {
		return
	}

	tenantID, mqttTopic, err := topic.DecodeTopic(msg.Subject)
	if err != nil {
		h.log.Warn("undecodable subject from nats", "subject", msg.Subject, "error", err)

		return
	}

	// QoS 0 to local subscribers. Raising this needs the session store, so
	// that an offline subscriber's message has somewhere to wait.
	if err := h.server.Publish(mount(tenant.ID(tenantID), mqttTopic), msg.Data, false, 0); err != nil {
		h.log.Error("injecting message from nats", "subject", msg.Subject, "error", err)
	}
}

// sharePrefix introduces a shared subscription. MQTT spells it "$share" and
// mochi matches it case-insensitively, so mast does too.
const sharePrefix = "$share/"

// splitShare separates "$share/<group>/<filter>" into its group and the
// filter underneath. A filter that is not shared comes back unchanged.
func splitShare(filter string) (string, string, bool) {
	if len(filter) < len(sharePrefix) || !strings.EqualFold(filter[:len(sharePrefix)], sharePrefix) {
		return "", filter, false
	}

	after := filter[len(sharePrefix):]

	slash := strings.IndexByte(after, '/')
	if slash <= 0 {
		return "", filter, false // "$share/x" with no filter is malformed
	}

	return after[:slash], after[slash+1:], true
}

// mountFilter mounts a subscription filter, keeping any "$share/<group>/"
// prefix in front where mochi's topic index expects to find it. Mounting
// ahead of the prefix would hide the share from mochi entirely, and the
// subscription would silently stop being shared.
func mountFilter(id tenant.ID, filter string) string {
	group, rest, shared := splitShare(filter)
	if !shared {
		return mount(id, filter)
	}

	return sharePrefix + group + "/" + mount(id, rest)
}

// bare recovers the topic as the client wrote it, undoing both the share
// prefix and the tenant mount.
func bare(id tenant.ID, mounted string) string {
	_, rest, _ := splitShare(mounted)

	return unmount(id, rest)
}

// mount prefixes a topic with its tenant, giving each tenant its own subtree
// of mochi's topic index.
func mount(id tenant.ID, mqttTopic string) string {
	return string(id) + "/" + mqttTopic
}

// unmount removes the tenant prefix that [mount] added.
func unmount(id tenant.ID, mounted string) string {
	return strings.TrimPrefix(mounted, string(id)+"/")
}

// queueName scopes a shared-subscription group to its tenant, so two tenants
// using the same group name do not steal each other's messages.
func queueName(id tenant.ID, share string) string {
	return string(id) + "." + share
}
