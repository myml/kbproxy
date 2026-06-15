package main

type loadBalancer interface {
	Pick(backends []*backendStats) *backendStats
}

type leastConnectionsLB struct{}

func (leastConnectionsLB) Pick(backends []*backendStats) *backendStats {
	if len(backends) == 0 {
		return nil
	}
	selected := backends[0]
	min := selected.activeConns.Load()
	for _, b := range backends[1:] {
		if n := b.activeConns.Load(); n < min {
			min = n
			selected = b
		}
	}
	return selected
}

type leastBandwidthLB struct{}

func (leastBandwidthLB) Pick(backends []*backendStats) *backendStats {
	if len(backends) == 0 {
		return nil
	}
	selected := backends[0]
	min := selected.rateOut.rate()
	for _, b := range backends[1:] {
		if r := b.rateOut.rate(); r < min {
			min = r
			selected = b
		}
	}
	return selected
}

func newLoadBalancer(strategy string) loadBalancer {
	switch strategy {
	case "least_bandwidth":
		return leastBandwidthLB{}
	case "least_conn":
		fallthrough
	default:
		return leastConnectionsLB{}
	}
}
