package tenant_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/mastmq/mast/internal/domain/tenant"
)

func TestLimitsWithOverridesOnlyWhatIsSet(t *testing.T) {
	t.Parallel()

	base := tenant.Limits{MaxConnections: 10, PublishRate: 5, PublishBurst: 0, OfflineQueue: 100}
	queue := 20

	got := base.With(tenant.Overrides{OfflineQueue: &queue})

	want := tenant.Limits{MaxConnections: 10, PublishRate: 5, PublishBurst: 0, OfflineQueue: 20}
	if got != want {
		t.Errorf("With = %+v, want %+v", got, want)
	}
}

func TestOverridesWireFormat(t *testing.T) {
	t.Parallel()

	var o tenant.Overrides
	if err := json.Unmarshal([]byte(`{"max_connections":0,"publish_rate":2.5}`), &o); err != nil {
		t.Fatalf("decoding: %v", err)
	}

	// An explicit zero is "unlimited", which is a decision, and must not be
	// confused with a field the service left out.
	if o.MaxConnections == nil || *o.MaxConnections != 0 {
		t.Errorf("max_connections = %v, want an explicit 0", o.MaxConnections)
	}

	if o.PublishRate == nil || *o.PublishRate != 2.5 {
		t.Errorf("publish_rate = %v, want 2.5", o.PublishRate)
	}

	if o.OfflineQueue != nil {
		t.Errorf("offline_queue = %v, want unset", *o.OfflineQueue)
	}
}

func TestOverridesValidate(t *testing.T) {
	t.Parallel()

	negative := -1
	if err := (tenant.Overrides{PublishBurst: &negative}).Validate(); !errors.Is(err, tenant.ErrBadLimit) {
		t.Errorf("a negative burst validated: %v", err)
	}

	if err := (tenant.Overrides{}).Validate(); err != nil {
		t.Errorf("no overrides failed to validate: %v", err)
	}
}

func TestBurstDefaultsToOneSecond(t *testing.T) {
	t.Parallel()

	cases := []struct {
		limits tenant.Limits
		want   float64
	}{
		{tenant.Limits{PublishRate: 50}, 50},
		{tenant.Limits{PublishRate: 50, PublishBurst: 7}, 7},
		// Below one a second, a one-second bucket would never hold a whole
		// token and would refuse everything.
		{tenant.Limits{PublishRate: 0.2}, 1},
	}

	for _, tc := range cases {
		if got := tc.limits.Burst(); got != tc.want {
			t.Errorf("Burst(%+v) = %v, want %v", tc.limits, got, tc.want)
		}
	}
}
