// Command drill checks what an MQTT cluster actually delivers while it is
// being broken. It counts by payload, not by volume: every message names its
// device and sequence, so the report says which acknowledged messages never
// arrived and which arrived twice.
//
//	drill setup   — connect N persistent devices, subscribe QoS 1, go to sleep
//	drill publish — publish M messages to each sleeping device, record the acked
//	drill verify  — wake every device on a different edge, count what it gets
//	drill live    — connected subscribers under steady QoS 1 traffic, for chaos
//	drill seed    — write N away sessions straight into the sessions bucket
//	drill seedpub — QoS 1 to random seeded sessions, then count what was queued
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

var (
	edges   = flag.String("edges", "edge-0:1883,edge-1:1883,edge-2:1883", "MQTT listeners")
	devices = flag.Int("devices", 1000, "persistent devices")
	perDev  = flag.Int("per-device", 5, "messages per device in publish")
	state   = flag.String("state", "/state", "directory for the acked list")
	wait    = flag.Duration("wait", 30*time.Second, "how long verify waits for stragglers")
	dur     = flag.Duration("duration", 60*time.Second, "live: how long to publish")
	rate    = flag.Int("rate", 500, "live: messages per second in total")
	pubs    = flag.Int("publishers", 20, "publishing clients")
	prefix  = flag.String("prefix", "drill", "client id and topic prefix")
	conc    = flag.Int("concurrency", 100, "parallel connects")
	topics  = flag.Int("topics", 2000, "retained: topics holding a retained message")
)

func main() {
	flag.CommandLine.Parse(os.Args[2:]) //nolint:errcheck
	switch os.Args[1] {
	case "setup":
		setup()
	case "publish":
		publish()
	case "verify":
		verify()
	case "live":
		live()
	case "retained":
		retained()
	case "seed":
		seed()
	case "seedpub":
		seedpub()
	default:
		fmt.Println("unknown phase", os.Args[1])
		os.Exit(2)
	}
}

func edgeList() []string { return strings.Split(*edges, ",") }

func devID(i int) string    { return fmt.Sprintf("%s-dev-%05d", *prefix, i) }
func devTopic(i int) string { return fmt.Sprintf("%s/dev/%05d/cmd", *prefix, i) }

func opts(id string, brokers []string, clean bool) *paho.ClientOptions {
	o := paho.NewClientOptions().SetClientID(id).SetCleanSession(clean).
		SetConnectTimeout(10 * time.Second).SetAutoReconnect(false).SetOrderMatters(false).
		SetKeepAlive(30 * time.Second).SetWriteTimeout(10 * time.Second)
	for _, b := range brokers {
		o.AddBroker("tcp://" + b)
	}

	return o
}

func parallel(n int, f func(i int)) {
	sem := make(chan struct{}, *conc)
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() { defer func() { <-sem }(); f(i) })
	}
	wg.Wait()
}

func connectRetry(o *paho.ClientOptions, tries int) (paho.Client, error) {
	var err error
	for range tries {
		c := paho.NewClient(o)
		t := c.Connect()
		if t.WaitTimeout(15*time.Second) && t.Error() == nil {
			return c, nil
		}
		err = t.Error()
		if err == nil {
			err = fmt.Errorf("connect timeout")
		}
		time.Sleep(500 * time.Millisecond)
	}

	return nil, err
}

func setup() {
	e := edgeList()
	var failed atomic.Int64
	start := time.Now()
	parallel(*devices, func(i int) {
		c, err := connectRetry(opts(devID(i), []string{e[i%len(e)]}, false), 5)
		if err != nil {
			failed.Add(1)
			return
		}
		t := c.Subscribe(devTopic(i), 1, nil)
		if !t.WaitTimeout(15*time.Second) || t.Error() != nil {
			failed.Add(1)
		}
		c.Disconnect(200)
	})
	fmt.Printf("setup: %d devices subscribed and asleep in %v, %d failed\n",
		*devices, time.Since(start).Round(time.Millisecond), failed.Load())
}

// publisher is one publishing client that moves to another edge when its
// own goes away.
type publisher struct {
	mu     sync.Mutex
	id     string
	brk    []string
	client paho.Client
}

func (p *publisher) get() paho.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil && p.client.IsConnectionOpen() {
		return p.client
	}
	for {
		rand.Shuffle(len(p.brk), func(a, b int) { p.brk[a], p.brk[b] = p.brk[b], p.brk[a] })
		c, err := connectRetry(opts(p.id, p.brk, true), 1)
		if err == nil {
			p.client = c
			return c
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// send publishes until the broker acknowledges, moving edges on failure.
// It returns false only if no edge would take the message.
func (p *publisher) send(topic, payload string) bool {
	for range 20 {
		c := p.get()
		t := c.Publish(topic, 1, false, payload)
		if t.WaitTimeout(10*time.Second) && t.Error() == nil {
			return true
		}
		c.Disconnect(0)
	}

	return false
}

func publish() {
	e := edgeList()
	ps := make([]*publisher, *pubs)
	for i := range ps {
		ps[i] = &publisher{id: fmt.Sprintf("%s-pub-%d", *prefix, i), brk: slices.Clone(e)}
	}

	f, err := os.Create(*state + "/acked.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var mu sync.Mutex
	w := bufio.NewWriter(f)

	var acked, refused atomic.Int64
	start := time.Now()
	jobs := make(chan [2]int, 1024)
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Go(func() {
			for j := range jobs {
				payload := fmt.Sprintf("%d:%d", j[0], j[1])
				if p.send(devTopic(j[0]), payload) {
					acked.Add(1)
					mu.Lock()
					fmt.Fprintln(w, payload)
					mu.Unlock()
				} else {
					refused.Add(1)
				}
			}
		})
	}
	for k := range *perDev {
		for i := range *devices {
			jobs <- [2]int{i, k}
		}
	}
	close(jobs)
	wg.Wait()
	w.Flush()
	fmt.Printf("publish: %d acked, %d never acked, in %v\n", acked.Load(), refused.Load(), time.Since(start).Round(time.Millisecond))
}

type tally struct {
	mu   sync.Mutex
	seen map[string]int
}

func (t *tally) add(payload string) {
	t.mu.Lock()
	t.seen[payload]++
	t.mu.Unlock()
}

func verify() {
	e := edgeList()
	acked := map[string]bool{}
	f, err := os.Open(*state + "/acked.txt")
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		acked[sc.Text()] = true
	}
	f.Close()

	tl := &tally{seen: map[string]int{}}
	var failed atomic.Int64
	clients := make([]paho.Client, *devices)
	start := time.Now()
	parallel(*devices, func(i int) {
		o := opts(devID(i), []string{e[(i+1)%len(e)]}, false)
		o.SetDefaultPublishHandler(func(_ paho.Client, m paho.Message) { tl.add(string(m.Payload())) })
		c, err := connectRetry(o, 5)
		if err != nil {
			failed.Add(1)
			return
		}
		clients[i] = c
	})
	connected := time.Since(start)

	deadline := time.Now().Add(*wait)
	for time.Now().Before(deadline) {
		tl.mu.Lock()
		got := 0
		for p := range acked {
			if tl.seen[p] > 0 {
				got++
			}
		}
		tl.mu.Unlock()
		if got == len(acked) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(2 * time.Second) // let late duplicates show

	tl.mu.Lock()
	lost, dups, extra := 0, 0, 0
	var lostDevs = map[string]int{}
	byK := map[string]int{}
	for p := range acked {
		n := tl.seen[p]
		if n == 0 {
			lost++
			lostDevs[strings.Split(p, ":")[0]]++
			byK[strings.Split(p, ":")[1]]++
		} else if n > 1 {
			dups += n - 1
		}
	}
	for p, n := range tl.seen {
		if !acked[p] {
			extra += n
		}
	}
	tl.mu.Unlock()
	for _, c := range clients {
		if c != nil {
			c.Disconnect(100)
		}
	}
	if lost > 0 {
		fmt.Printf("verify: lost by sequence: %v\n", byK)
	}
	fmt.Printf("verify: %d devices woke in %v (%d failed); acked=%d delivered=%d lost=%d duplicates=%d unacked-but-delivered=%d; devices with loss=%d\n",
		*devices, connected.Round(time.Millisecond), failed.Load(), len(acked), len(acked)-lost, lost, dups, extra, len(lostDevs))
}

// live runs connected persistent subscribers and steady QoS 1 traffic. The
// subscribers reconnect to any edge when theirs goes away, as devices do.
func live() {
	e := edgeList()
	tl := &tally{seen: map[string]int{}}
	var lat struct {
		mu sync.Mutex
		v  []time.Duration
	}
	var reconnects atomic.Int64

	subs := make([]paho.Client, *devices)
	parallel(*devices, func(i int) {
		brk := slices.Clone(e)
		rand.Shuffle(len(brk), func(a, b int) { brk[a], brk[b] = brk[b], brk[a] })
		o := opts(devID(i), brk, false)
		o.SetAutoReconnect(true).SetMaxReconnectInterval(2 * time.Second).SetConnectRetry(true)
		o.SetReconnectingHandler(func(paho.Client, *paho.ClientOptions) { reconnects.Add(1) })
		o.SetDefaultPublishHandler(func(_ paho.Client, m paho.Message) {
			p := string(m.Payload())
			tl.add(p)
			parts := strings.Split(p, ":")
			if len(parts) == 3 {
				var ns int64
				fmt.Sscan(parts[2], &ns)
				lat.mu.Lock()
				lat.v = append(lat.v, time.Since(time.Unix(0, ns)))
				lat.mu.Unlock()
			}
		})
		c, err := connectRetry(o, 5)
		if err != nil {
			fmt.Println("subscriber connect failed:", err)
			return
		}
		t := c.Subscribe(devTopic(i), 1, nil)
		t.WaitTimeout(15 * time.Second)
		subs[i] = c
	})
	fmt.Printf("live: %d subscribers up; publishing %d msg/s for %v\n", *devices, *rate, *dur)

	ps := make([]*publisher, *pubs)
	for i := range ps {
		ps[i] = &publisher{id: fmt.Sprintf("%s-livepub-%d", *prefix, i), brk: slices.Clone(e)}
	}
	var ackMu sync.Mutex
	acked := map[string]bool{}
	var refused, behind atomic.Int64
	var seq atomic.Int64

	// Fixed schedule: a stalled send shows up as lateness, not as a lower
	// rate.
	interval := time.Second / time.Duration(*rate)
	jobs := make(chan int64, *rate*10)
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Go(func() {
			for n := range jobs {
				dev := int(n) % *devices
				payload := fmt.Sprintf("%d:%d:%d", dev, n, time.Now().UnixNano())
				if p.send(devTopic(dev), payload) {
					ackMu.Lock()
					acked[payload] = true
					ackMu.Unlock()
				} else {
					refused.Add(1)
				}
			}
		})
	}
	start := time.Now()
	ticker := time.NewTicker(interval)
	for time.Since(start) < *dur {
		<-ticker.C
		select {
		case jobs <- seq.Add(1):
		default:
			behind.Add(1)
		}
	}
	ticker.Stop()
	close(jobs)
	wg.Wait()

	// Stragglers: redelivery and offline queues drain on reconnect.
	deadline := time.Now().Add(*wait)
	for time.Now().Before(deadline) {
		ackMu.Lock()
		tl.mu.Lock()
		missing := 0
		for p := range acked {
			if tl.seen[p] == 0 {
				missing++
			}
		}
		tl.mu.Unlock()
		ackMu.Unlock()
		if missing == 0 {
			break
		}
		time.Sleep(time.Second)
	}

	lost, dups := 0, 0
	tl.mu.Lock()
	for p := range acked {
		if n := tl.seen[p]; n == 0 {
			lost++
		} else if n > 1 {
			dups += n - 1
		}
	}
	tl.mu.Unlock()
	lat.mu.Lock()
	sort.Slice(lat.v, func(a, b int) bool { return lat.v[a] < lat.v[b] })
	pc := func(q float64) time.Duration {
		if len(lat.v) == 0 {
			return 0
		}
		return lat.v[int(q*float64(len(lat.v)-1))].Round(time.Millisecond)
	}
	fmt.Printf("live: acked=%d lost=%d duplicates=%d never-acked=%d dropped-by-generator=%d subscriber-reconnects=%d latency p50=%v p99=%v p999=%v max=%v\n",
		len(acked), lost, dups, refused.Load(), behind.Load(), reconnects.Load(), pc(0.5), pc(0.99), pc(0.999), pc(1))
	lat.mu.Unlock()
	for _, c := range subs {
		if c != nil {
			c.Disconnect(100)
		}
	}
}

// retained stores a retained message on many topics, then has every device
// connect and subscribe to a wildcard over all of them at once: the shape
// of a fleet restart where each device asks for its last known state.
func retained() {
	e := edgeList()
	c, err := connectRetry(opts(*prefix+"-retainer", []string{e[0]}, true), 5)
	if err != nil {
		panic(err)
	}
	for i := range *topics {
		t := c.Publish(fmt.Sprintf("%s/state/%05d", *prefix, i), 1, true, fmt.Sprintf("v%d", i))
		t.WaitTimeout(10 * time.Second)
	}
	c.Disconnect(100)
	time.Sleep(2 * time.Second)

	var got, done atomic.Int64
	var mu sync.Mutex
	var took []time.Duration
	start := time.Now()
	parallel(*devices, func(i int) {
		var n atomic.Int64
		finished := make(chan struct{})
		o := opts(fmt.Sprintf("%s-reader-%05d", *prefix, i), []string{e[i%len(e)]}, true)
		t0 := time.Now()
		o.SetDefaultPublishHandler(func(_ paho.Client, m paho.Message) {
			got.Add(1)
			if n.Add(1) == int64(*topics) {
				close(finished)
			}
		})
		cl, err := connectRetry(o, 3)
		if err != nil {
			return
		}
		cl.Subscribe(*prefix+"/state/#", 0, nil).WaitTimeout(30 * time.Second)
		select {
		case <-finished:
			done.Add(1)
			mu.Lock()
			took = append(took, time.Since(t0))
			mu.Unlock()
		case <-time.After(60 * time.Second):
		}
		cl.Disconnect(50)
	})
	sort.Slice(took, func(a, b int) bool { return took[a] < took[b] })
	pc := func(q float64) time.Duration {
		if len(took) == 0 {
			return 0
		}
		return took[int(q*float64(len(took)-1))].Round(time.Millisecond)
	}
	fmt.Printf("retained: %d readers x %d topics in %v; complete=%d replayed=%d per-reader p50=%v p99=%v max=%v\n",
		*devices, *topics, time.Since(start).Round(time.Millisecond), done.Load(), got.Load(), pc(0.5), pc(0.99), pc(1))
}
