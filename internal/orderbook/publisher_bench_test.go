package orderbook

import (
	"testing"
)

func BenchmarkPublisherJSON(b *testing.B) {
	pub := NewRedisPublisher()
	inc := OrderBookIncrement{
		Symbol: "BTC-USD",
		Updates: []LevelUpdate{
			{Action: ActionInsert, Side: BidSide, Price: 50000.50, Size: 1.5},
			{Action: ActionUpdate, Side: AskSide, Price: 50001.00, Size: 0.5},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pub.PublishIncrement(inc)
	}
}
