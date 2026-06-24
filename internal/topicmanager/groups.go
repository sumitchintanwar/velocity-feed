package topicmanager

// TopicGroup maps a symbol to its Redis channel group. This enables
// topic group routing: related symbols share a Redis channel, reducing
// the total channel count while keeping traffic efficient.
//
// Example: AAPL, MSFT, GOOG, AMZN all map to "equities".
// Redis channels: market:equities (not market:AAPL, market:MSFT, etc.)
//
// When a client subscribes to AAPL, the gateway subscribes to the
// "equities" group channel. All equity updates flow through one channel,
// and the local TopicManager filters to deliver only relevant symbols.
type TopicGroup struct {
	Name    string   // group name (e.g., "equities")
	Channel string   // Redis channel (e.g., "market:equities")
	Symbols []string // symbols in this group
}

// DefaultTopicGroups defines the standard market data topic groups.
// Each group maps to a single Redis channel prefix + group name.
var DefaultTopicGroups = []TopicGroup{
	{
		Name:    "equities",
		Channel: "market:equities",
		Symbols: []string{
			"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "NFLX",
			"JPM", "V", "MA", "UNH", "JNJ", "WMT", "PG", "HD",
		},
	},
	{
		Name:    "crypto",
		Channel: "market:crypto",
		Symbols: []string{
			"BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "DOGEUSD", "XRPUSD",
		},
	},
	{
		Name:    "forex",
		Channel: "market:forex",
		Symbols: []string{
			"EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
		},
	},
	{
		Name:    "futures",
		Channel: "market:futures",
		Symbols: []string{
			"ES", "NQ", "YM", "RTY", "CL", "GC", "SI",
		},
	},
}

// SymbolToGroup returns the group name for a symbol, or "" if unknown.
func SymbolToGroup(symbol string) string {
	for _, g := range DefaultTopicGroups {
		for _, s := range g.Symbols {
			if s == symbol {
				return g.Name
			}
		}
	}
	return ""
}

// SymbolToChannel returns the Redis channel for a symbol, or "" if unknown.
func SymbolToChannel(symbol string, prefix string) string {
	group := SymbolToGroup(symbol)
	if group == "" {
		return ""
	}
	for _, g := range DefaultTopicGroups {
		if g.Name == group {
			return prefix + group
		}
	}
	return ""
}

// GroupChannel returns the full Redis channel name for a group.
func GroupChannel(groupName string, prefix string) string {
	return prefix + groupName
}

// AllGroupChannels returns all Redis channel names for all groups.
func AllGroupChannels(prefix string) []string {
	channels := make([]string, len(DefaultTopicGroups))
	for i, g := range DefaultTopicGroups {
		channels[i] = prefix + g.Name
	}
	return channels
}

// GroupSymbols returns all symbols in a group.
func GroupSymbols(groupName string) []string {
	for _, g := range DefaultTopicGroups {
		if g.Name == groupName {
			return g.Symbols
		}
	}
	return nil
}
