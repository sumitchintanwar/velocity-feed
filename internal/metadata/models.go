package metadata

// TradingStatus represents the current state of an instrument.
type TradingStatus string

const (
	StatusTrading    TradingStatus = "Trading"
	StatusHalted     TradingStatus = "Halted"
	StatusAuction    TradingStatus = "Auction"
	StatusPreMarket  TradingStatus = "Pre-Market"
	StatusPostMarket TradingStatus = "Post-Market"
	StatusSuspended  TradingStatus = "Suspended"
	StatusClosed     TradingStatus = "Closed"
)

// AssetClass categorizes the type of financial instrument.
type AssetClass string

const (
	AssetClassEquity AssetClass = "Equity"
	AssetClassCrypto AssetClass = "Crypto"
	AssetClassForex  AssetClass = "Forex"
	AssetClassFuture AssetClass = "Future"
	AssetClassOption AssetClass = "Option"
)

// Instrument represents the complete canonical reference data for a symbol.
type Instrument struct {
	CanonicalSymbol string        `json:"canonical_symbol"`
	ExchangeSymbol  string        `json:"exchange_symbol"`
	Exchange        string        `json:"exchange"`
	AssetClass      AssetClass    `json:"asset_class"`
	BaseCurrency    string        `json:"base_currency"`
	QuoteCurrency   string        `json:"quote_currency"`
	TradingStatus   TradingStatus `json:"trading_status"`
	TickSize        float64       `json:"tick_size"`
	LotSize         float64       `json:"lot_size"`
	InstrumentName  string        `json:"instrument_name"`
	Extensions      map[string]any `json:"extensions,omitempty"`
}
