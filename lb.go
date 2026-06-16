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
	min := float64(selected.activeConns.Load()) / float64(selected.weight)
	for _, b := range backends[1:] {
		if v := float64(b.activeConns.Load()) / float64(b.weight); v < min {
			min = v
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
	min := selected.rateOut.rate() / float64(selected.weight)
	for _, b := range backends[1:] {
		if v := b.rateOut.rate() / float64(b.weight); v < min {
			min = v
			selected = b
		}
	}
	return selected
}

func newLoadBalancer(strategy string) loadBalancer {
	switch strategy {
	case "least_conn":
		return leastConnectionsLB{}
	case "least_bandwidth":
		fallthrough
	default:
		return leastBandwidthLB{}
	}
}
