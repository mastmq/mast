package mqttd

import "errors"

// ErrIncompleteTLS is returned when only one half of the TLS keypair is set.
var ErrIncompleteTLS = errors.New("mqttd: mqtt.tls_cert and mqtt.tls_key must both be set")
