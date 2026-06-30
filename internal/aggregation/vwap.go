package aggregation

// VWAPAggregator maintains the cumulative VWAP state.
// Typically resets daily.
// Utilizes Kahan summation to prevent float64 drift over large cumulative sums.
type VWAPAggregator struct {
	state *VWAP
	cPriceVol float64 // Kahan compensation for Price*Volume
	cVol      float64 // Kahan compensation for Volume
}

func NewVWAPAggregator() *VWAPAggregator {
	return &VWAPAggregator{}
}

// AddTick updates the VWAP running sums.
// Returns a copy of the state for real-time publishing.
func (v *VWAPAggregator) AddTick(tick Tick) VWAP {
	if v.state == nil {
		v.state = &VWAP{
			Symbol: tick.Symbol,
			Start:  tick.Timestamp,
		}
	}

	pv := tick.Price * tick.Volume
	
	// Kahan summation for Price*Volume
	y1 := pv - v.cPriceVol
	t1 := v.state.CumulativePriceVolume + y1
	v.cPriceVol = (t1 - v.state.CumulativePriceVolume) - y1
	v.state.CumulativePriceVolume = t1

	// Kahan summation for Volume
	y2 := tick.Volume - v.cVol
	t2 := v.state.CumulativeVolume + y2
	v.cVol = (t2 - v.state.CumulativeVolume) - y2
	v.state.CumulativeVolume = t2
	
	if v.state.CumulativeVolume > 0 {
		v.state.VWAP = v.state.CumulativePriceVolume / v.state.CumulativeVolume
	}

	// Return a copy (by value) so downstream doesn't mutate
	return *v.state
}

// Reset clears the VWAP (e.g. at start of trading day)
func (v *VWAPAggregator) Reset() {
	v.state = nil
	v.cPriceVol = 0
	v.cVol = 0
}
