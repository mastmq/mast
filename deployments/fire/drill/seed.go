package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// seed and seedpub answer a question a laptop cannot answer with sockets:
// what the core tier does with a fleet-sized number of sleeping sessions.
// Three hundred thousand MQTT connections from one host run out of
// ephemeral ports long before the broker runs out of anything, so seed
// writes the session records straight into the sessions bucket, in the
// shape the bridge writes them, marked away. Every queuer then has to load
// them all and match every QoS 1/2 message against them, which is the cost
// being measured.

var (
	sessions = flag.Int("sessions", 300000, "seed: away sessions to write")
	natsURL  = flag.String("nats", "nats://core-0:4222", "seed: a core's NATS client port")
	tenantID = flag.String("tenant", "default", "seed: the tenant the records belong to")
	present  = flag.Bool("present", false, "seed: mark the sessions held, so no queuer keeps anything for them")
)

func seedID(i int) string    { return fmt.Sprintf("%s-seed-%06d", *prefix, i) }
func seedTopic(i int) string { return fmt.Sprintf("%s/seed/%06d/cmd", *prefix, i) }

type seedSub struct {
	Filter string `json:"filter"`
	QoS    byte   `json:"qos"`
}

type seedSession struct {
	ClientID      string    `json:"client_id"`
	Tenant        string    `json:"tenant"`
	Subscriptions []seedSub `json:"subscriptions"`
	UpdatedAt     time.Time `json:"updated_at"`
	Present       bool      `json:"present,omitempty"`
}

func seed() {
	nc, err := nats.Connect(*natsURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(4096))
	if err != nil {
		panic(err)
	}

	start := time.Now()
	now := time.Now().UTC()

	for i := range *sessions {
		// Two filters each, one of them a wildcard, so the index holds
		// what a device fleet holds rather than one literal per device.
		rec := seedSession{
			ClientID: seedID(i),
			Tenant:   *tenantID,
			Subscriptions: []seedSub{
				{Filter: seedTopic(i), QoS: 1},
				{Filter: fmt.Sprintf("%s/seed/%06d/cfg/#", *prefix, i), QoS: 1},
			},
			UpdatedAt: now,
			Present:   *present,
		}

		body, _ := json.Marshal(rec)
		for {
			_, err := js.PublishAsync("$KV.mast_sessions."+*tenantID+"."+seedID(i), body)
			if err == nil {
				break
			}
			// A replicated stream acknowledges slower than one client can
			// write; let the outstanding ones land and carry on.
			if !errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
				panic(err)
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(time.Minute):
				// The cores stopped answering; a seed that waits forever
				// hides that, which is how a core killed for memory once
				// looked like a slow seed.
				panic("seed: the stream stopped acknowledging writes")
			}
		}

		if i%50000 == 49999 {
			fmt.Printf("seed: %d written in %v\n", i+1, time.Since(start).Round(time.Millisecond))
		}
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(2 * time.Minute):
		panic("seed: writes never completed")
	}

	fmt.Printf("seed: %d away sessions in %v\n", *sessions, time.Since(start).Round(time.Millisecond))
}

// seedpub publishes QoS 1 to random seeded devices for --duration at
// --rate, and reports how many the stream holds for them afterwards.
func seedpub() {
	brokers := edgeList()
	n := *pubs
	cs := make([]paho.Client, n)
	parallel(n, func(i int) {
		c, err := connectRetry(opts(fmt.Sprintf("%s-seedpub-%d", *prefix, i), []string{brokers[i%len(brokers)]}, true), 5)
		if err != nil {
			panic(err)
		}
		cs[i] = c
	})

	var sent, failed atomic.Int64
	tick := time.Second / time.Duration(max(*rate/n, 1))
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	done := make(chan struct{})
	for i := range n {
		go func() {
			defer func() { done <- struct{}{} }()
			t := time.NewTicker(tick)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				tok := cs[i].Publish(seedTopic(rand.IntN(*sessions)), 1, false, "x")
				if tok.WaitTimeout(10*time.Second) && tok.Error() == nil {
					sent.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	for range n {
		<-done
	}
	fmt.Printf("seedpub: %d acked, %d failed in %v\n", sent.Load(), failed.Load(), *dur)

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)

	// Each queued message is written once per queuer and deduplicated, so
	// the stream should settle at the number acked.
	for range 60 {
		s, err := js.Stream(context.Background(), "mast_offline")
		if err == nil {
			fmt.Printf("seedpub: mast_offline holds %d\n", s.CachedInfo().State.Msgs)
			if int64(s.CachedInfo().State.Msgs) >= sent.Load() {
				return
			}
		}
		time.Sleep(time.Second)
	}
}
