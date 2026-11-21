package broker_test

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/admin"
	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
)

const adminToken = "test-admin-token-0123456789"

// adminGather is short so a test pays little for every list, and long
// enough that a node under a full -race run still answers inside it.
const adminGather = 300 * time.Millisecond

// withAdmin serves the admin API on a kernel-chosen port, with a token file.
func withAdmin(t *testing.T) func(*config.Config) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "admin-token")
	if err := os.WriteFile(path, []byte(adminToken+"\n"), 0o600); err != nil {
		t.Fatalf("writing the token: %v", err)
	}

	return func(cfg *config.Config) {
		cfg.Admin.Addr = anyPort
		cfg.Admin.TokenFile = path
		cfg.Admin.Gather = adminGather
	}
}

// startAdmin brings up an all-in-one node serving the admin API, and
// returns its MQTT and admin addresses.
func startAdmin(t *testing.T) (string, string) {
	t.Helper()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()
	withAdmin(t)(&cfg)

	node, err := broker.Start(t.Context(), cfg, byUsername{}, tenant.AllowAll{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	return cfg.MQTT.Addr, node.AdminAddr()
}

// call makes one admin request and returns the status and body.
func call(t *testing.T, method, addr, path string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+addr+path, nil)
	if err != nil {
		t.Fatalf("building %s %s: %v", method, path, err)
	}

	req.Header.Set("Authorization", "Bearer "+adminToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s %s: %v", method, path, err)
	}

	return resp.StatusCode, body
}

func decode(t *testing.T, body []byte, into any) {
	t.Helper()

	if err := json.Unmarshal(body, into); err != nil {
		t.Fatalf("decoding %s: %v", body, err)
	}
}

func getSession(t *testing.T, addr, tenantName, clientID string) (int, admin.Session) {
	t.Helper()

	status, body := call(t, http.MethodGet, addr,
		"/v1/tenants/"+tenantName+"/sessions/"+url.PathEscape(clientID))
	if status != http.StatusOK {
		return status, admin.Session{}
	}

	var s admin.Session
	decode(t, body, &s)

	return status, s
}

func stats(t *testing.T, addr, tenantName string) admin.Stats {
	t.Helper()

	status, body := call(t, http.MethodGet, addr, "/v1/tenants/"+tenantName+"/stats")
	if status != http.StatusOK {
		t.Fatalf("stats: %d %s", status, body)
	}

	var s admin.Stats
	decode(t, body, &s)

	return s
}

func list(t *testing.T, addr, tenantName, query string) admin.Page {
	t.Helper()

	status, body := call(t, http.MethodGet, addr, "/v1/tenants/"+tenantName+"/sessions"+query)
	if status != http.StatusOK {
		t.Fatalf("list: %d %s", status, body)
	}

	var page admin.Page
	decode(t, body, &page)

	return page
}

func clientIDs(page admin.Page) []string {
	out := make([]string, 0, len(page.Sessions))
	for _, s := range page.Sessions {
		out = append(out, s.ClientID)
	}

	return out
}

// TestAdminAPIOffByDefault is the promise the config makes: nothing listens
// unless an address is set.
func TestAdminAPIOffByDefault(t *testing.T) {
	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()

	node, err := broker.Start(t.Context(), cfg, byUsername{}, tenant.AllowAll{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	if node.AdminAddr() != "" {
		t.Fatalf("the admin api is listening on %s with no admin.addr set", node.AdminAddr())
	}
}

// TestAdminAPIRefusesToStartWithoutAToken holds that a configured listener
// with a token file that cannot be read stops the node rather than serving
// an API anyone can call.
func TestAdminAPIRefusesToStartWithoutAToken(t *testing.T) {
	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()
	cfg.Admin.Addr = anyPort
	cfg.Admin.TokenFile = filepath.Join(t.TempDir(), "absent")

	node, err := broker.Start(t.Context(), cfg, byUsername{}, tenant.AllowAll{}, slog.New(slog.DiscardHandler))
	if err == nil {
		node.Close()
		t.Fatal("the node started with an admin api whose token file does not exist")
	}
}

// TestAdminAPI walks a tenant through every endpoint on one node: two
// connected clients and a sleeping persistent session, beside another
// tenant using the same client id.
//
//nolint:gocognit,gocyclo // one scenario on one node, as the funlen and cyclop exclusions for tests already allow
func TestAdminAPI(t *testing.T) {
	mqttAddr, adminAddr := startAdmin(t)

	rover := persistent(t, mqttAddr, "rover", "acme")
	rover.subscribeQoS(t, "fleet/rover", 1)

	sleeper := persistent(t, mqttAddr, "sleeper", "acme")
	sleeper.subscribeQoS(t, "fleet/sleeper", 1)
	sleeper.client.Disconnect(100)

	probe := connect(t, mqttAddr, "probe", "acme")

	// The same client id in another tenant is another client, and must not
	// appear in acme's answers.
	other := persistent(t, mqttAddr, "rover", "other")
	other.subscribeQoS(t, "fleet/rover", 1)

	t.Run("list", func(t *testing.T) {
		var page admin.Page

		ok := waitForLeaf(func() bool {
			page = list(t, adminAddr, "acme", "")

			return slices.Equal(clientIDs(page), []string{"probe", "rover", "sleeper"}) &&
				page.Sessions[2].UpdatedAt != nil
		})
		if !ok {
			t.Fatalf("acme's sessions = %+v, want probe, rover and a stored sleeper", page)
		}

		if page.Nodes != 1 {
			t.Errorf("nodes = %d, want 1", page.Nodes)
		}

		if others := clientIDs(list(t, adminAddr, "other", "")); !slices.Equal(others, []string{"rover"}) {
			t.Errorf("other's sessions = %v, want its own rover only", others)
		}

		probeS, roverS, sleeperS := page.Sessions[0], page.Sessions[1], page.Sessions[2]

		if !probeS.Connected || probeS.Persistent {
			t.Errorf("probe = %+v, want connected and clean", probeS)
		}

		if !roverS.Connected || !roverS.Persistent || roverS.Node == "" ||
			!slices.Equal(roverS.Subscriptions, []admin.Subscription{{Filter: "fleet/rover", QoS: 1}}) {
			t.Errorf("rover = %+v, want connected, persistent, on fleet/rover", roverS)
		}

		if sleeperS.Connected || !sleeperS.Persistent ||
			!slices.Equal(sleeperS.Subscriptions, []admin.Subscription{{Filter: "fleet/sleeper", QoS: 1}}) {
			t.Errorf("sleeper = %+v, want away, persistent, on fleet/sleeper from its record", sleeperS)
		}
	})

	t.Run("pages", func(t *testing.T) {
		first := list(t, adminAddr, "acme", "?limit=2")
		if !slices.Equal(clientIDs(first), []string{"probe", "rover"}) || first.Next == "" {
			t.Fatalf("first page = %v next %q, want probe and rover and a cursor", clientIDs(first), first.Next)
		}

		second := list(t, adminAddr, "acme", "?limit=2&cursor="+url.QueryEscape(first.Next))
		if !slices.Equal(clientIDs(second), []string{"sleeper"}) || second.Next != "" {
			t.Fatalf("second page = %v next %q, want sleeper and no cursor", clientIDs(second), second.Next)
		}

		forged := "/v1/tenants/acme/sessions?cursor=%25%25"
		if status, _ := call(t, http.MethodGet, adminAddr, forged); status != http.StatusBadRequest {
			t.Errorf("a forged cursor: status %d, want 400", status)
		}
	})

	t.Run("get", func(t *testing.T) {
		if status, s := getSession(t, adminAddr, "acme", "rover"); status != http.StatusOK || !s.Connected || !s.Persistent {
			t.Errorf("rover: %d %+v, want connected and persistent", status, s)
		}

		if status, s := getSession(t, adminAddr, "acme", "sleeper"); status != http.StatusOK || s.Connected || !s.Persistent {
			t.Errorf("sleeper: %d %+v, want stored and away", status, s)
		}

		if status, _ := getSession(t, adminAddr, "acme", "nobody"); status != http.StatusNotFound {
			t.Errorf("an unknown client: status %d, want 404", status)
		}

		if status, _ := getSession(t, adminAddr, "other", "sleeper"); status != http.StatusNotFound {
			t.Errorf("acme's sleeper asked for under other: status %d, want 404", status)
		}
	})

	t.Run("stats count queued messages", func(t *testing.T) {
		tok := probe.client.Publish("fleet/sleeper", 1, false, "wake up")
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publishing: %v", tok.Error())
		}

		var s admin.Stats

		queued := func() bool {
			s = stats(t, adminAddr, "acme")

			return s.QueuedMessages >= 1
		}
		if !waitForLeaf(queued) {
			t.Fatalf("stats = %+v, want the sleeper's message queued", s)
		}

		if s.Connections != 2 || s.Subscriptions != 1 || s.StoredSessions != 2 || s.Nodes != 1 {
			t.Errorf("stats = %+v, want 2 connections, 1 subscription, 2 stored sessions, 1 node", s)
		}

		if _, sl := getSession(t, adminAddr, "acme", "sleeper"); sl.Queued != 1 {
			t.Errorf("sleeper queued = %d, want 1", sl.Queued)
		}
	})

	t.Run("disconnect", func(t *testing.T) {
		status, body := call(t, http.MethodPost, adminAddr, "/v1/tenants/acme/sessions/rover/disconnect")
		if status != http.StatusOK {
			t.Fatalf("disconnect: %d %s", status, body)
		}

		if !waitFor(func() bool { return !rover.client.IsConnected() }) {
			t.Fatal("rover is still connected after the admin api disconnected it")
		}

		if !other.client.IsConnected() {
			t.Fatal("disconnecting acme's rover disconnected other's rover")
		}

		// The session outlives the connection, as it would have had the
		// device gone by itself.
		var s admin.Session

		ok := waitFor(func() bool {
			var status int

			status, s = getSession(t, adminAddr, "acme", "rover")

			return status == http.StatusOK && !s.Connected && s.Persistent
		})
		if !ok {
			t.Fatalf("rover after disconnect = %+v, want its session kept and away", s)
		}

		again := "/v1/tenants/acme/sessions/rover/disconnect"
		if status, _ := call(t, http.MethodPost, adminAddr, again); status != http.StatusNotFound {
			t.Errorf("disconnecting a client no node holds: status %d, want 404", status)
		}
	})
}

// TestClusterAdminReachesEveryNode asks one node about a client held by
// another. Only the node holding a connection knows it, so an answer that
// is right here crossed the control plane.
//
// The client id has a slash in it, which a device may send and which has to
// reach the API as one escaped path segment.
func TestClusterAdminReachesEveryNode(t *testing.T) {
	const yardRover = "yard/rover"

	c := startCluster(t, withAdmin(t))

	rover := persistent(t, c.addrs[0], yardRover, "acme")
	rover.subscribeQoS(t, "fleet/rover", 1)

	holder := c.edges[0].NATS().ID()

	// Asked of the core, which holds no client at all.
	var s admin.Session

	ok := waitForLeaf(func() bool {
		var status int

		status, s = getSession(t, c.core.AdminAddr(), "acme", yardRover)

		return status == http.StatusOK && s.Connected
	})
	if !ok {
		t.Fatalf("the core's view of rover = %+v, want connected", s)
	}

	if s.Node != holder {
		t.Errorf("rover reported on node %q, want the edge holding it, %q", s.Node, holder)
	}

	if page := list(t, c.core.AdminAddr(), "acme", ""); page.Nodes != len(c.edges) {
		t.Errorf("%d nodes answered a list, want every edge (%d)", page.Nodes, len(c.edges))
	}

	if st := stats(t, c.edges[1].AdminAddr(), "acme"); st.Connections != 1 || st.Subscriptions != 1 {
		t.Errorf("stats from the other edge = %+v, want rover's one connection and filter", st)
	}

	// Disconnected from the other edge.
	status, body := call(t, http.MethodPost, c.edges[1].AdminAddr(),
		"/v1/tenants/acme/sessions/"+url.PathEscape(yardRover)+"/disconnect")
	if status != http.StatusOK {
		t.Fatalf("disconnect from the other edge: %d %s", status, body)
	}

	var by map[string]any
	if decode(t, body, &by); by["node"] != holder {
		t.Errorf("disconnect reported node %v, want %q", by["node"], holder)
	}

	if !waitForLeaf(func() bool { return !rover.client.IsConnected() }) {
		t.Fatal("a disconnect asked of one edge did not reach the client on the other")
	}
}

// TestAdminAPIAnswersForAClientTooBigForOneMessage holds one client whose
// filters alone are more than NATS carries in one message (max_payload,
// 1 MB by default), and a light client after it.
//
// The node's answer used to be refused by NATS: the get reported the
// connected client as unknown, and the list dropped the node, its clients
// and, through the cursor, every page's worth of them. Now the heavy client
// comes back with the first of its filters and says so, the list stops
// after it, and the next page reaches the light one.
func TestAdminAPIAnswersForAClientTooBigForOneMessage(t *testing.T) {
	const (
		filters = 5000
		batch   = 500
	)

	mqttAddr, adminAddr := startAdmin(t)

	heavy := connect(t, mqttAddr, "heavy", "acme")
	connect(t, mqttAddr, "light", "acme")

	// About 220 bytes of JSON per filter, so 5000 of them pass 1 MB.
	pad := strings.Repeat("x", 180)

	for start := 0; start < filters; start += batch {
		set := make(map[string]byte, batch)
		for i := start; i < start+batch; i++ {
			set[fmt.Sprintf("devices/heavy/%s/%05d/#", pad, i)] = 0
		}

		tok := heavy.client.SubscribeMultiple(set, nil)
		if !tok.WaitTimeout(30*time.Second) || tok.Error() != nil {
			t.Fatalf("subscribing filters %d to %d: %v", start, start+batch, tok.Error())
		}
	}

	status, s := getSession(t, adminAddr, "acme", "heavy")
	if status != http.StatusOK || !s.Connected {
		t.Fatalf("get heavy: status %d, %+v; want it connected", status, s.ClientID)
	}

	if !s.SubscriptionsTruncated || len(s.Subscriptions) == 0 || len(s.Subscriptions) >= filters {
		t.Errorf("heavy came back with %d of %d filters, truncated %v; want some, marked",
			len(s.Subscriptions), filters, s.SubscriptionsTruncated)
	}

	first := list(t, adminAddr, "acme", "?limit=1000")
	if !slices.Equal(clientIDs(first), []string{"heavy"}) || first.Next == "" || first.Nodes != 1 {
		t.Fatalf("first page = %v next %q nodes %d; want heavy alone, a cursor, and the node",
			clientIDs(first), first.Next, first.Nodes)
	}

	second := list(t, adminAddr, "acme", "?limit=1000&cursor="+url.QueryEscape(first.Next))
	if !slices.Equal(clientIDs(second), []string{"light"}) || second.Next != "" {
		t.Fatalf("second page = %v next %q; want light and the end", clientIDs(second), second.Next)
	}

	if st := stats(t, adminAddr, "acme"); st.Subscriptions != filters {
		t.Errorf("stats count %d filters, want every one of %d", st.Subscriptions, filters)
	}
}
