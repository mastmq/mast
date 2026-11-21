package broker

// Kill stops a node the way SIGKILL does, as far as the rest of the cluster
// can tell: the fabric goes first and without a word, so nothing the node
// does on its way down — recording its sessions as away, above all — ever
// reaches the core. Its MQTT clients then lose their connections.
//
// Close cannot stand in for it. It closes MQTT first, which records every
// lasting session as away, and that is the clean shutdown the queuer has
// always handled.
func (b *Broker) Kill() {
	b.nats.Kill()
	b.obs.Close()

	if b.server != nil {
		_ = b.server.Close()
	}

	b.shutdown()
	b.wg.Wait()
	b.stopQueuer()

	if b.store != nil {
		b.store.Close()
	}
}
