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
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/domain/topic"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/store"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// hookID names this hook in mochi's logs.
const hookID = "mast-bridge"

// Headers carried with every message on the fabric. MQTT delivery semantics
// do not survive a bare payload, so the parts mochi needs to reproduce them
// travel alongside it.
const (
	headerQoS    = "Mast-Qos"
	headerRetain = "Mast-Retain"
	// headerID names one publish, so a node that receives it on several
	// overlapping subscriptions can tell the copies apart from new messages.
	headerID = "Mast-Id"
)

// storeTimeout bounds a single call to the durable store. It sits on the
// subscribe and publish paths, so it is short.
const storeTimeout = 5 * time.Second

// retainHandlingNever is MQTT 5's "do not send retained messages on
// subscribe" subscription option.
const retainHandlingNever = 2

// packetIDMask narrows mochi's uint32 allocation to the 16 bits an MQTT
// packet identifier actually has.
const packetIDMask = 0xFFFF

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
	store  *store.Store

	// injector is the inline client messages from the fabric are published
	// through. It is this hook's own rather than mochi's, because mochi's
	// Publish offers no way to say which NATS subscription a message came in
	// on, and [Hook.OnSelectSubscribers] needs to know.
	injector *mqtt.Client
	seen     *seen

	mu      sync.RWMutex
	tenants map[string]tenant.Identity
	// open is every connection the gauge has counted and not yet released,
	// by connection rather than by client id, so that a disconnect reported
	// twice — once by a takeover, once by mochi — is released once, and a
	// new connection with the same id is never mistaken for the old one.
	open map[*mqtt.Client]tenant.ID

	// sessions is this node's claim on each client id it holds, and nodeID
	// names the node in a notice so a human reading the log knows where a
	// client went.
	sessions *sessions
	nodeID   string

	metrics *obs.Metrics

	// internalListener is the mochi listener id whose connections skip
	// authentication, and internalTenant is where they land.
	internalListener string
	internalTenant   tenant.ID
}

// Options configures a [Hook].
type Options struct {
	// Metrics records broker activity. Nil disables recording.
	Metrics *obs.Metrics

	// InternalListener is the mochi listener id that bypasses authentication
	// and authorization. Empty disables the bypass.
	InternalListener string
	// InternalTenant is the tenant connections on that listener join.
	InternalTenant tenant.ID

	// NodeID names this node in session notices. It is for humans reading
	// logs; correctness rests on the per-connection owner token.
	NodeID string
}

// New builds a bridge over an established NATS connection.
//
// A nil store disables everything durable — retained messages and offline
// queues — which is only appropriate where nothing is expected to survive.
func New(
	nc *nats.Conn,
	st *store.Store,
	resolver tenant.Resolver,
	policy tenant.Policy,
	opts Options,
	log *slog.Logger,
) *Hook {
	h := &Hook{
		metrics:          opts.Metrics,
		internalListener: opts.InternalListener,
		internalTenant:   opts.InternalTenant,
		HookBase:         mqtt.HookBase{},
		mu:               sync.RWMutex{},
		nc:               nc,
		store:            st,
		resolver:         resolver,
		policy:           policy,
		log:              log.With("component", "bridge"),
		server:           nil,
		subs:             nil,
		injector:         nil,
		seen:             newSeen(),
		tenants:          make(map[string]tenant.Identity),
		open:             make(map[*mqtt.Client]tenant.ID),
		sessions:         newSessions(),
		nodeID:           opts.NodeID,
	}
	h.subs = newRegistry(nc, h.onNATSMessage)

	return h
}

// Attach binds the hook to the server it will inject messages into.
func (h *Hook) Attach(server *mqtt.Server) {
	h.server = server
	// Built exactly as mochi builds its own, so an injected message is
	// indistinguishable from one sent through [mqtt.Server.Publish].
	h.injector = server.NewClient(nil, mqtt.LocalListener, mqtt.InlineClientId, true)
}

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
		mqtt.OnClientExpired,
		mqtt.OnWill,
		mqtt.OnPacketEncode,
		mqtt.OnQosPublish,
		mqtt.OnSessionEstablished,
		mqtt.OnSelectSubscribers,
	}, b)
}

// OnConnectAuthenticate resolves the connection to a tenant. A connection
// that resolves to no tenant is refused: there is no such thing as an
// untenanted client.
func (h *Hook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	// A connection on the internal listener is trusted by virtue of having
	// reached it. Nothing is asked of it and nothing is asked about it later.
	if h.isInternal(cl) {
		bare := cl.ID
		cl.ID = mountClient(h.internalTenant, bare)
		h.onConnected(cl, tenant.Identity{Tenant: h.internalTenant, Superuser: true})
		h.claimSession(h.internalTenant, bare, cl.ID)

		if pk.Connect.Clean {
			h.dropSession(h.internalTenant, bare)
		}

		return true
	}

	identity, err := h.resolver.Resolve(context.Background(), tenant.Credentials{
		ClientID:        cl.ID,
		Username:        string(pk.Connect.Username),
		Password:        pk.Connect.Password,
		RemoteAddr:      cl.Net.Remote,
		ProtocolVersion: cl.Properties.ProtocolVersion,
		CleanStart:      pk.Connect.Clean,
	})
	if err != nil {
		h.log.Debug("authentication refused", "client", cl.ID, "error", err)
		h.count(func(m *obs.Metrics) { m.AuthFailures.WithLabelValues("refused").Inc() })

		return false
	}

	// Scope the id to its tenant before anything keys on it. mochi is about
	// to look up an existing session and add this client to a map, both by
	// id, and mast's own tenant and subscription maps follow suit.
	bare := cl.ID
	cl.ID = mountClient(identity.Tenant, bare)

	h.log.Debug("client authenticated",
		"client", cl.ID, "tenant", string(identity.Tenant), "superuser", identity.Superuser)
	h.onConnected(cl, identity)

	// Announce the claim so a copy of this client on another node stands
	// down. mochi has already displaced any local copy by this point.
	h.claimSession(identity.Tenant, bare, cl.ID)

	// A clean start means the durable state goes now; a resumed session is
	// put back in OnSessionEstablished, once mochi has actually added the
	// client and delivery has somewhere to land.
	if pk.Connect.Clean {
		h.dropSession(identity.Tenant, bare)
	}

	return true
}

// OnSessionEstablished restores a session that belongs to another node.
//
// It runs after mochi has added the client and sent the CONNACK, which is
// the earliest point a message can actually be delivered: restoring in
// OnConnectAuthenticate registered the subscriptions correctly and then
// published the backlog into a topic index whose subscriber was not yet in
// the client map, so every queued message was dropped on the floor.
//
// A session mochi inherited locally is left alone. It already carries
// subscriptions, and its inflight state is richer than anything the bucket
// holds. Its queue in the bucket is discarded instead: everything in it was
// written by OnQosPublish at the moment mochi put the same message into
// that inflight, which mochi is about to resend. Kept, the queue outlived
// the delivery and replayed it again the next time the device came up on
// another node.
func (h *Hook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	if pk.Connect.Clean {
		return
	}

	id, ok := h.tenantOf(cl)
	if !ok {
		return
	}

	bareID := unmountClient(id, cl.ID)

	if len(cl.State.Subscriptions.GetAll()) > 0 {
		h.discardOffline(id, bareID)

		return
	}

	h.restoreSession(cl, id, bareID)
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
	identity, ok := h.identityOf(cl)
	if !ok {
		return false
	}

	// A superuser is never asked about individual topics, exactly as EMQX
	// does it. Asking anyway would enforce rules the broker being replaced
	// has never applied to these connections.
	if identity.Superuser || h.isInternal(cl) {
		return true
	}

	id := identity.Tenant

	return h.policy.Allows(context.Background(), tenant.Access{
		Tenant: id,
		// Unmounted: a policy service is told the id the client sent, which
		// is also the only one it could have written a rule about.
		ClientID:   unmountClient(id, cl.ID),
		Username:   string(cl.Properties.Username),
		RemoteAddr: cl.Net.Remote,
		Topic:      bare(id, mountedTopic),
		Write:      write,
	})
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

	if pk.FixedHeader.Retain {
		h.storeRetained(subject, unmount(id, pk.TopicName), pk)
	}

	msg := nats.NewMsg(subject)
	msg.Data = pk.Payload
	msg.Header.Set(headerQoS, strconv.Itoa(int(pk.FixedHeader.Qos)))
	msg.Header.Set(headerID, nuid.Next())

	if pk.FixedHeader.Retain {
		msg.Header.Set(headerRetain, "1")
	}

	if err := h.nc.PublishMsg(msg); err != nil {
		h.log.Error("forwarding publish to nats", "subject", subject, "error", err)
	}

	h.count(func(m *obs.Metrics) { m.MessagesIn.WithLabelValues(string(id)).Inc() })

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

	h.persistSession(cl, id, unmountClient(id, cl.ID))
	h.deliverRetained(cl, id, pk, reasonCodes)
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
	h.persistSession(cl, id, unmountClient(id, cl.ID))
}

// OnDisconnect releases what the client held, unless its session outlives
// the connection.
//
// A persistent session keeps its NATS subscriptions open while the client is
// away. That is what makes an offline queue possible at all: messages have to
// keep arriving at a node for anything to queue them. mochi tells us which
// case this is through expire, which it computes from the clean flag and the
// v5 session expiry interval.
func (h *Hook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	// The connection is gone either way, so the gauge drops either way.
	// Only the session's other state depends on whether it expires — and
	// conflating the two is what made this gauge drift: a persistent
	// session reconnecting was counted twice, once per CONNECT, and never
	// decremented in between.
	h.onDisconnected(cl)

	if !expire {
		h.log.Debug("client away, session retained", "client", cl.ID)

		return
	}

	if id, ok := h.tenantOf(cl); ok {
		h.dropSession(id, unmountClient(id, cl.ID))
	}

	h.forget(cl.ID)
}

// OnWill forwards a last-will message onto the fabric and stops mochi
// delivering it locally.
//
// mochi builds the will packet itself and hands it straight to
// publishToSubscribers, bypassing OnPublish entirely, so a will would
// otherwise never reach the bridge: its topic would stay unmounted, match no
// subscriber's mounted filter, and never leave the node. Forwarding here and
// then pointing the packet at a topic no filter can match keeps every
// message on exactly one path — out to NATS and back — which is the same
// rule ordinary publishes follow.
func (h *Hook) OnWill(cl *mqtt.Client, will mqtt.Will) (mqtt.Will, error) {
	id, ok := h.tenantOf(cl)
	if !ok {
		return will, nil
	}

	subject, err := topic.EncodeTopic(string(id), will.TopicName)
	if err != nil {
		h.log.Warn("dropping will with unencodable topic",
			"client", cl.ID, "topic", will.TopicName, "error", err)

		return will, nil
	}

	if will.Retain {
		h.storeRetained(subject, will.TopicName, packets.Packet{
			FixedHeader: packets.FixedHeader{Qos: will.Qos, Retain: true},
			Payload:     will.Payload,
		})
	}

	msg := nats.NewMsg(subject)
	msg.Data = will.Payload
	msg.Header.Set(headerQoS, strconv.Itoa(int(will.Qos)))
	msg.Header.Set(headerID, nuid.Next())

	if err := h.nc.PublishMsg(msg); err != nil {
		h.log.Error("forwarding will to nats", "subject", subject, "error", err)
	}

	h.log.Debug("will forwarded", "client", cl.ID, "topic", will.TopicName)

	// Send mochi somewhere nothing is listening. Every filter is mounted
	// under a tenant, and a tenant cannot contain '$', so this matches none
	// of them and the local publish becomes a no-op.
	will.TopicName = droppedTopic
	will.Retain = false

	return will, nil
}

// droppedTopic is where a message goes when mochi must be handed something
// but nothing should receive it.
const droppedTopic = "$mast/dropped"

// OnClientExpired releases a session that outlived its client and has now
// run out of time.
func (h *Hook) OnClientExpired(cl *mqtt.Client) {
	h.log.Debug("session expired", "client", cl.ID)

	if id, ok := h.tenantOf(cl); ok {
		h.dropSession(id, unmountClient(id, cl.ID))
	}

	h.forget(cl.ID)
}

// OnSelectSubscribers narrows mochi's local fan-out to the subscribers the
// NATS copy being injected was meant for.
//
// A plain copy must not reach a shared group: whether this node serves the
// group is decided by the queue copy, which NATS may well have given to
// another node, and delivering here too puts the message in front of two
// members of one group. A queue copy must reach nobody but its own group, and
// only for the filter whose subject it arrived on, because "$share/g/a/#"
// and "$share/g/a/+" are two groups that each get a copy.
func (h *Hook) OnSelectSubscribers(subs *mqtt.Subscribers, pk packets.Packet) *mqtt.Subscribers {
	route := pk.Properties.ServerReference
	if route == routePlain {
		clear(subs.Shared)
		clear(subs.SharedSelected)

		return subs
	}

	fields := strings.Split(route, routeSep)

	const queueFields = 4
	if len(fields) != queueFields || fields[0] != "q" {
		return subs // not injected by the bridge
	}

	id, queue, subject := tenant.ID(fields[1]), fields[2], fields[3]

	clear(subs.Subscriptions)
	clear(subs.InlineSubscriptions)
	clear(subs.SharedSelected)

	for filter := range subs.Shared {
		if !servesQueue(id, filter, queue, subject) {
			delete(subs.Shared, filter)
		}
	}

	return subs
}

// tenantOf returns the tenant resolved for a client at CONNECT.
func (h *Hook) tenantOf(cl *mqtt.Client) (tenant.ID, bool) {
	identity, ok := h.identityOf(cl)

	return identity.Tenant, ok
}

// identityOf returns what authentication established about a client.
func (h *Hook) identityOf(cl *mqtt.Client) (tenant.Identity, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	identity, ok := h.tenants[cl.ID]

	return identity, ok
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
//
// A node can receive one message several times, once per NATS subscription it
// matches, and the copies are not interchangeable. Plain copies are
// duplicates of each other: the first goes through mochi's local fan-out,
// which already reaches every plain subscriber once, and the rest are
// dropped. A queue copy is different — NATS picked this node for one shared
// subscription group — so it goes to that group alone, and
// [Hook.OnSelectSubscribers] keeps it from everyone else.
func (h *Hook) onNATSMessage(msg *nats.Msg) {
	if h.server == nil {
		return
	}

	var queue string
	if msg.Sub != nil {
		queue = msg.Sub.Queue
	}

	if queue == "" && !h.seen.first(msg.Header.Get(headerID)) {
		return
	}

	tenantID, mqttTopic, err := topic.DecodeTopic(msg.Subject)
	if err != nil {
		h.log.Warn("undecodable subject from nats", "subject", msg.Subject, "error", err)

		return
	}

	mounted := mount(tenant.ID(tenantID), mqttTopic)

	route := routePlain

	if queue != "" {
		// mochi only asks the hook to choose when a shared subscriber
		// matches. Without one, the copy would fall through to the plain
		// subscribers, who have already had it.
		if len(h.server.Topics.Subscribers(mounted).Shared) == 0 {
			return
		}

		route = queueRoute(tenantID, queue, msg.Sub.Subject)
	}

	// Inject at the QoS the publisher used. mochi then downgrades per
	// subscription to the minimum of that and what each subscriber
	// negotiated, which is what the spec asks for. Without this every
	// subscriber silently received QoS 0 however it subscribed.
	// Retain is deliberately false here. A retained publish is stored in the
	// KV bucket by the ingress node and replayed from there on subscribe;
	// setting it on live delivery would both duplicate that and lie to a
	// subscriber that was already present, which MQTT says gets retain=0.
	qos := qosOf(msg)

	err = h.server.InjectPacket(h.injector, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
		TopicName:   mounted,
		Payload:     msg.Data,
		// mochi's own Publish does the same: an inline publish is never
		// acknowledged, but a QoS 1 or 2 packet still needs an id to be
		// valid.
		PacketID: uint16(qos),
		// ServerReference is only valid on CONNACK and DISCONNECT, so mochi
		// never encodes it on a PUBLISH. That makes it the one field that
		// can carry the route from here to OnSelectSubscribers without any
		// chance of reaching a client.
		Properties: packets.Properties{ServerReference: route},
	})
	if err != nil {
		h.log.Error("injecting message from nats", "subject", msg.Subject, "error", err)

		return
	}

	h.count(func(m *obs.Metrics) { m.MessagesOut.WithLabelValues(tenantID).Inc() })
}

// routePlain marks a message that arrived on a plain NATS subscription.
const routePlain = "p"

// routeSep separates the fields of a queue route. NUL can occur in none of
// them: not in a tenant, which the codec restricts, and not in a subject.
const routeSep = "\x00"

// queueRoute marks a message NATS delivered to one shared subscription group.
func queueRoute(tenantID, queue, subject string) string {
	return "q" + routeSep + tenantID + routeSep + queue + routeSep + subject
}

// servesQueue reports whether a mounted shared filter is the one a NATS queue
// subscription on subject exists for.
func servesQueue(id tenant.ID, filter, queue, subject string) bool {
	group, rest, shared := splitShare(filter)
	if !shared || queueName(id, group) != queue {
		return false
	}

	subjects, err := topic.FilterSubjects(string(id), unmount(id, rest))
	if err != nil {
		return false
	}

	return slices.Contains(subjects, subject)
}

// qosOf reads the QoS a message was published at, defaulting to 0 for
// anything that reached the subject without going through the bridge.
func qosOf(msg *nats.Msg) byte {
	raw := msg.Header.Get(headerQoS)
	if raw == "" {
		return 0
	}

	qos, err := strconv.Atoi(raw)
	if err != nil || qos < 0 || qos > 2 {
		return 0
	}

	return byte(qos)
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

// mountClient scopes a client id to its tenant.
//
// MQTT requires that a second connection with an existing client id
// disconnects the first, and mochi implements that by keying its client map
// on the id alone. Without a tenant prefix that rule reaches across the
// isolation boundary: any authenticated tenant could disconnect another
// tenant's device by guessing or reusing its client id, and keep it
// disconnected. The prefix makes "already connected" a question asked
// within a tenant, which is the only place it means anything.
//
// It is deliberately the same shape as [mount]. A client id and a topic are
// different things, but they are namespaced for the same reason and a
// reader should recognise the pattern.
//
// The prefix never reaches the client. mochi captures the assigned client
// identifier for a v5 client that sent an empty id before this hook runs,
// so the CONNACK still carries the unprefixed form.
func mountClient(id tenant.ID, clientID string) string {
	return mount(id, clientID)
}

// unmountClient removes the prefix that [mountClient] added, for the two
// places a client id leaves mast: an authorization request and a log line
// meant for a human.
func unmountClient(id tenant.ID, mounted string) string {
	return unmount(id, mounted)
}

// queueName scopes a shared-subscription group to its tenant, so two tenants
// using the same group name do not steal each other's messages.
func queueName(id tenant.ID, share string) string {
	return string(id) + "." + share
}

// storeRetained records a retained publish, or clears it when the payload is
// empty, which is what MQTT says a retained publish with no payload means.
func (h *Hook) storeRetained(subject, bareTopic string, pk packets.Packet) {
	if h.store == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	err := h.store.PutRetained(ctx, subject, store.Message{
		Topic:   bareTopic,
		Payload: pk.Payload,
		QoS:     pk.FixedHeader.Qos,
		Retain:  true,
	})
	if err != nil {
		h.log.Error("storing retained message", "subject", subject, "error", err)
	}
}

// deliverRetained replays the last known value of every topic a new
// subscription matches, to that client alone.
//
// mochi keeps its own retained index, but it is per node and holds only what
// that node happened to see, so a subscriber landing on a quiet node would
// get nothing. Reading the KV bucket instead makes retention a property of
// the cluster and survives a restart.
func (h *Hook) deliverRetained(cl *mqtt.Client, id tenant.ID, pk packets.Packet, reasonCodes []byte) {
	if h.store == nil {
		return
	}

	for i, filter := range pk.Filters {
		if i < len(reasonCodes) && reasonCodes[i] >= packets.ErrUnspecifiedError.Code {
			continue // not granted
		}

		// A shared subscription receives no retained messages when it is
		// first made. MQTT 5 section 4.8.2.
		if _, _, shared := splitShare(filter.Filter); shared {
			continue
		}

		if filter.RetainHandling == retainHandlingNever {
			continue
		}

		h.replayRetained(cl, id, filter)
	}
}

// replayRetained sends this client every retained message one filter matches.
func (h *Hook) replayRetained(cl *mqtt.Client, id tenant.ID, filter packets.Subscription) {
	subjects, err := topic.FilterSubjects(string(id), unmount(id, filter.Filter))
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	messages, err := h.store.MatchRetained(ctx, subjects)
	if err != nil {
		h.log.Error("reading retained messages", "tenant", string(id), "error", err)

		return
	}

	if len(messages) > 0 {
		h.count(func(m *obs.Metrics) {
			m.RetainedReplayed.WithLabelValues(string(id)).Add(float64(len(messages)))
		})
	}

	for _, msg := range messages {
		if err := h.writeRetained(cl, id, filter, msg); err != nil {
			h.log.Debug("delivering retained message",
				"client", cl.ID, "topic", msg.Topic, "error", err)
		}
	}
}

// writeRetained sends one retained message to a single client.
//
// It writes to that client directly rather than publishing, because a
// retained replay belongs to the subscriber that just arrived and not to
// everyone else already holding the same filter.
func (h *Hook) writeRetained(
	cl *mqtt.Client,
	id tenant.ID,
	filter packets.Subscription,
	msg store.Message,
) error {
	return h.deliverTo(cl, mount(id, msg.Topic), msg.Payload, min(msg.QoS, filter.Qos), true)
}

// deliverTo sends one message to one client and nobody else.
//
// It is the part of mochi's publishToClient that a targeted delivery needs,
// which mochi does not export. Two parts matter and were missing when this
// wrote the packet bare: the read ACL, which mochi applies to every message
// on its way out and a replay must not skip, and the inflight entry, without
// which a QoS 1 message lost to a dropped connection is simply gone — and for
// a drained queue it is gone from the bucket too.
func (h *Hook) deliverTo(cl *mqtt.Client, mountedTopic string, payload []byte, qos byte, retain bool) error {
	if !h.OnACLCheck(cl, mountedTopic, false) {
		return packets.ErrNotAuthorized
	}

	out := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Qos:    min(qos, h.server.Options.Capabilities.MaximumQos),
			Retain: retain,
		},
		TopicName: mountedTopic,
		Payload:   payload,
		Created:   time.Now().Unix(),
	}

	if out.FixedHeader.Qos > 0 {
		if cl.State.Inflight.Len() >= int(h.server.Options.Capabilities.MaximumInflight) {
			return packets.ErrQuotaExceeded
		}

		packetID, err := cl.NextPacketID()
		if err != nil {
			return fmt.Errorf("bridge: packet id: %w", err)
		}

		// NextPacketID returns uint32 but MQTT packet ids are 16 bit, and
		// mochi allocates them inside that range.
		out.PacketID = uint16(packetID & packetIDMask)

		if cl.State.Inflight.Set(out) {
			atomic.AddInt64(&h.server.Info.Inflight, 1)
			cl.State.Inflight.DecreaseSendQuota()
		}
	}

	if err := cl.WritePacket(out); err != nil {
		return fmt.Errorf("bridge: writing to %s: %w", cl.ID, err)
	}

	return nil
}

// forget drops every trace of a client: its NATS subscriptions and the
// tenant resolved for it.
func (h *Hook) forget(clientID string) {
	h.subs.releaseAll(clientID)
	h.releaseSession(clientID)

	h.mu.Lock()
	delete(h.tenants, clientID)
	h.mu.Unlock()
}

// onConnected records an established connection, whichever listener it
// arrived on.
//
// Both listeners come through here precisely because they did not before:
// the internal listener returned early, so a node holding thousands of
// connections on it reported none, and the gauge was blind to the one
// listener a load test is most likely to use.
func (h *Hook) onConnected(cl *mqtt.Client, identity tenant.Identity) {
	h.mu.Lock()
	h.tenants[cl.ID] = identity
	h.open[cl] = identity.Tenant
	h.mu.Unlock()

	h.count(func(m *obs.Metrics) {
		m.ConnectionsTotal.WithLabelValues(string(identity.Tenant)).Inc()
		m.ConnectionsOpen.WithLabelValues(string(identity.Tenant)).Inc()
	})
}

// onDisconnected records a connection going away. It is safe to call more
// than once for the same connection; only the first call counts.
func (h *Hook) onDisconnected(cl *mqtt.Client) {
	h.mu.Lock()
	id, counted := h.open[cl]
	delete(h.open, cl)
	h.mu.Unlock()

	if !counted {
		return
	}

	h.count(func(m *obs.Metrics) {
		m.ConnectionsOpen.WithLabelValues(string(id)).Dec()
	})
}

// count records a metric when metrics are enabled, so every call site stays
// a single line rather than a nil check.
func (h *Hook) count(record func(*obs.Metrics)) {
	if h.metrics != nil {
		record(h.metrics)
	}
}

// isInternal reports whether a client arrived on the unauthenticated
// listener.
func (h *Hook) isInternal(cl *mqtt.Client) bool {
	return h.internalListener != "" && cl.Net.Listener == h.internalListener
}
