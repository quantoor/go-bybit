package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// PerpWebsocketV1PublicV2Service :
type PerpWebsocketV1PublicV2Service struct {
	connection *websocket.Conn

	paramOrderBookL2Map    map[string]func(PerpWebsocketV1PublicV2OrderBookL2Response) error
	paramTradeMap          map[string]func(PerpWebsocketV1PublicV2TradeResponse) error
	paramInstrumentInfoMap map[string]func(PerpWebsocketV1PublicV2InstrumentInfoResponse) error
}

const (
	// PerpWebsocketV1PublicV2Path :
	PerpWebsocketV1PublicV2Path = "/realtime_public"
)

// PerpWebsocketV1PublicV2Event :
type PerpWebsocketV1PublicV2Event string

const (
	// PerpWebsocketV1PublicV2EventSubscribe :
	PerpWebsocketV1PublicV2EventSubscribe = "subscribe"
	// PerpWebsocketV1PublicV2EventUnsubscribe :
	PerpWebsocketV1PublicV2EventUnsubscribe = "unsubscribe"
)

// PerpWebsocketV1PublicV2Topic :
type PerpWebsocketV1PublicV2Topic string

const (
	// PerpWebsocketV1PublicV2TopicTrade :
	PerpWebsocketV1PublicV2TopicOrderBookL2    = PerpWebsocketV1PublicV2Topic("orderBookL2_25")
	PerpWebsocketV1PublicV2TopicTrade          = PerpWebsocketV1PublicV2Topic("trade")
	PerpWebsocketV1PublicV2TopicInstrumentInfo = PerpWebsocketV1PublicV2Topic("instrument_info")
)

// PerpWebsocketV1PublicV2OrderBookL2Response :
type PerpWebsocketV1PublicV2OrderBookL2Response struct {
	Topic       PerpWebsocketV1PublicV2Topic              `json:"topic"`
	Type        string                                    `json:"type"`
	Data        PerpWebsocketV1PublicV2OrderBookL2Content `json:"data"`
	CrossSeq    string                                    `json:"cross_seq"`
	TimestampE6 string                                    `json:"timestamp_e6"`
}

// PerpWebsocketV1PublicV2OrderBookL2Content :
type PerpWebsocketV1PublicV2OrderBookL2Content struct {
	// shapshot
	OrderBook []OrderBookItem `json:"order_book"`
	// delta update
	Delete []OrderBookItem `json:"delete"`
	Update []OrderBookItem `json:"update"`
	Insert []OrderBookItem `json:"insert"`
}

// OrderBookItem :
type OrderBookItem struct {
	Price  string  `json:"price"`
	Symbol string  `json:"symbol"`
	Side   string  `json:"side"`
	Size   float64 `json:"size"`
}

// Key :
func (p *PerpWebsocketV1PublicV2OrderBookL2Response) Key() string {
	return string(p.Topic)
}

// addParamOrderBookL2Func :
func (s *PerpWebsocketV1PublicV2Service) addParamOrderBookL2Func(params []string, f func(PerpWebsocketV1PublicV2OrderBookL2Response) error) error {
	for _, param := range params {
		if _, exist := s.paramInstrumentInfoMap[param]; exist {
			return errors.New("already registered for this param")
		}
		s.paramOrderBookL2Map[param] = f
	}
	return nil
}

// removeParamOrderBookL2Func :
func (s *PerpWebsocketV1PublicV2Service) removeParamOrderBookL2Func(key string) {
	delete(s.paramInstrumentInfoMap, key)
}

// retrieveOrderBookL2Func :
func (s *PerpWebsocketV1PublicV2Service) retrieveOrderBookL2Func(key string) (func(PerpWebsocketV1PublicV2OrderBookL2Response) error, error) {
	f, exist := s.paramOrderBookL2Map[key]
	if !exist {
		return nil, errors.New("func not found")
	}
	return f, nil
}

// SubscribeOrderBookL2 :
func (s *PerpWebsocketV1PublicV2Service) SubscribeOrderBookL2(symbols []SymbolPerp, f func(response PerpWebsocketV1PublicV2OrderBookL2Response) error) (func() error, error) {

	var args []string
	for _, symbol := range symbols {
		args = append(args, "orderBookL2_25."+string(symbol))
	}

	param := PerpWebsocketV1PublicV2Params{
		Op:   PerpWebsocketV1PublicV2EventSubscribe,
		Args: args,
	}
	if err := s.addParamOrderBookL2Func(param.Key(), f); err != nil {
		return nil, err
	}
	buf, err := json.Marshal(param)
	if err != nil {
		return nil, err
	}
	if err := s.connection.WriteMessage(websocket.TextMessage, []byte(buf)); err != nil {
		return nil, err
	}

	return func() error {
		param.Op = PerpWebsocketV1PublicV2EventUnsubscribe
		buf, err := json.Marshal(param)
		if err != nil {
			return err
		}
		if err := s.connection.WriteMessage(websocket.TextMessage, []byte(buf)); err != nil {
			return err
		}
		for _, key := range param.Key() {
			s.removeParamTradeFunc(key)
		}
		return nil
	}, nil
}

// PerpWebsocketV1PublicV2TradeParamKey :
type PerpWebsocketV1PublicV2TradeParamKey struct {
	//Symbol SymbolPerp
	Topic PerpWebsocketV1PublicV2Topic
}

// PerpWebsocketV1PublicV2TradeResponse :
type PerpWebsocketV1PublicV2TradeResponse struct {
	Topic PerpWebsocketV1PublicV2Topic          `json:"topic"`
	Data  []PerpWebsocketV1PublicV2TradeContent `json:"data"`
}

// PerpWebsocketV1PublicV2TradeResponseParams :
type PerpWebsocketV1PublicV2TradeResponseParams struct {
	Symbol     SymbolPerp `json:"symbol"`
	SymbolName string     `json:"symbolName"`
	Binary     string     `json:"binary"`
}

// PerpWebsocketV1PublicV2TradeContent :
type PerpWebsocketV1PublicV2TradeContent struct {
	Symbol        string  `json:"symbol"`
	TickDirection string  `json:"tick_direction"`
	Price         string  `json:"price"`
	Size          float64 `json:"size"`
	Timestamp     string  `json:"timestamp"`
	TradeTimeMs   string  `json:"trade_time_ms"`
	Side          string  `json:"side"`
	TradeID       string  `json:"trade_id"`
	IsBlockTrade  string  `json:"is_block_trade"`
}

// Key :
func (p *PerpWebsocketV1PublicV2TradeResponse) Key() string {
	return string(p.Topic)
}

// PerpWebsocketV1PublicV2Params :
type PerpWebsocketV1PublicV2Params struct {
	Op   PerpWebsocketV1PublicV2Topic `json:"op"`
	Args []string                     `json:"args"`
}

// Key :
func (p *PerpWebsocketV1PublicV2Params) Key() []string {
	return p.Args
}

// addParamTradeFunc :
func (s *PerpWebsocketV1PublicV2Service) addParamTradeFunc(params []string, f func(PerpWebsocketV1PublicV2TradeResponse) error) error {
	for _, param := range params {
		if _, exist := s.paramTradeMap[param]; exist {
			return errors.New("already registered for this param")
		}
		s.paramTradeMap[param] = f
	}
	return nil
}

// removeParamTradeFunc :
func (s *PerpWebsocketV1PublicV2Service) removeParamTradeFunc(key string) {
	delete(s.paramTradeMap, key)
}

// retrieveTradeFunc :
func (s *PerpWebsocketV1PublicV2Service) retrieveTradeFunc(key string) (func(PerpWebsocketV1PublicV2TradeResponse) error, error) {
	f, exist := s.paramTradeMap[key]
	if !exist {
		return nil, errors.New("func not found")
	}
	return f, nil
}

// judgeTopic :
func (s *PerpWebsocketV1PublicV2Service) judgeTopic(respBody []byte) (PerpWebsocketV1PublicV2Topic, error) {
	result := struct {
		Topic PerpWebsocketV1PublicV2Topic `json:"topic"`
		Event PerpWebsocketV1PublicV2Event `json:"event"`
	}{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", err
	}
	if result.Event == PerpWebsocketV1PublicV2EventSubscribe {
		return "", nil
	}
	return result.Topic, nil
}

// parseResponse :
func (s *PerpWebsocketV1PublicV2Service) parseResponse(respBody []byte, response interface{}) error {
	if err := json.Unmarshal(respBody, &response); err != nil {
		return err
	}
	return nil
}

// SubscribeTrade :
func (s *PerpWebsocketV1PublicV2Service) SubscribeTrade(symbols []SymbolPerp, f func(response PerpWebsocketV1PublicV2TradeResponse) error) (func() error, error) {

	var args []string
	for _, symbol := range symbols {
		args = append(args, "trade."+string(symbol))
	}

	param := PerpWebsocketV1PublicV2Params{
		Op:   PerpWebsocketV1PublicV2EventSubscribe,
		Args: args,
	}
	if err := s.addParamTradeFunc(param.Key(), f); err != nil {
		return nil, err
	}
	buf, err := json.Marshal(param)
	if err != nil {
		return nil, err
	}
	if err := s.connection.WriteMessage(websocket.TextMessage, []byte(buf)); err != nil {
		return nil, err
	}

	return func() error {
		param.Op = PerpWebsocketV1PublicV2EventUnsubscribe
		buf, err := json.Marshal(param)
		if err != nil {
			return err
		}
		if err := s.connection.WriteMessage(websocket.TextMessage, []byte(buf)); err != nil {
			return err
		}
		for _, key := range param.Key() {
			s.removeParamTradeFunc(key)
		}
		return nil
	}, nil
}

// PerpWebsocketV1PublicV2InstrumentInfoResponse :
type PerpWebsocketV1PublicV2InstrumentInfoResponse struct {
	Topic       PerpWebsocketV1PublicV2Topic                 `json:"topic"`
	Type        string                                       `json:"type"`
	Data        PerpWebsocketV1PublicV2InstrumentInfoContent `json:"data"`
	CrossSeq    string                                       `json:"cross_seq"`
	TimestampE6 string                                       `json:"timestamp_e6"`
}

// PerpWebsocketV1PublicV2InstrumentInfoContent :
type PerpWebsocketV1PublicV2InstrumentInfoContent struct {
	ID        int    `json:"id"`
	Symbol    string `json:"symbol"`
	LastPrice string `json:"last_price"`
	//"last_tick_direction": "ZeroPlusTick",
	//"prev_price_24h": "331960000",
	//"price_24h_pcnt_e6": "-27126",
	//"high_price_24h": "333120000",
	//"low_price_24h": "315940000",
	//"prev_price_1h": "319490000",
	//"price_1h_pcnt_e6": "10845",
	MarkPrice string `json:"mark_price"`
	//"index_price": "323106800",
	//"open_interest_e8": "1430451600000",
	//"total_turnover_e8": "5297934997553700000",
	//"turnover_24h_e8": "243143978993099700",
	//"total_volume_e8": "1184936057899924",
	//"volume_24h_e8": "7511238100000",
	FundingRateE6          string `json:"funding_rate_e6"`
	PredictedFundingRateE6 string `json:"predicted_funding_rate_e6"`
	//"cross_seq": "6501157651",
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at"`
	NextFundingTime     string `json:"next_funding_time"`
	CountDownHour       string `json:"count_down_hour"`
	FundingRateInterval string `json:"funding_rate_interval"`
	Bid1Price           string `json:"bid1_price"`
	Ask1Price           string `json:"ask1_price"`
}

// Key :
func (p *PerpWebsocketV1PublicV2InstrumentInfoResponse) Key() string {
	return string(p.Topic)
}

// addParamInstrumentInfoFunc :
func (s *PerpWebsocketV1PublicV2Service) addParamInstrumentInfoFunc(params []string, f func(PerpWebsocketV1PublicV2InstrumentInfoResponse) error) error {
	for _, param := range params {
		if _, exist := s.paramInstrumentInfoMap[param]; exist {
			return errors.New("already registered for this param")
		}
		s.paramInstrumentInfoMap[param] = f
	}
	return nil
}

// removeParamInstrumentInfoFunc :
func (s *PerpWebsocketV1PublicV2Service) removeParamInstrumentInfoFunc(key string) {
	delete(s.paramInstrumentInfoMap, key)
}

// retrieveInstrumentInfoFunc :
func (s *PerpWebsocketV1PublicV2Service) retrieveInstrumentInfoFunc(key string) (func(PerpWebsocketV1PublicV2InstrumentInfoResponse) error, error) {
	f, exist := s.paramInstrumentInfoMap[key]
	if !exist {
		return nil, errors.New("func not found")
	}
	return f, nil
}

// SubscribeInstrumentInfo :
func (s *PerpWebsocketV1PublicV2Service) SubscribeInstrumentInfo(symbols []SymbolPerp, f func(response PerpWebsocketV1PublicV2InstrumentInfoResponse) error) (func() error, error) {

	var args []string
	for _, symbol := range symbols {
		args = append(args, "instrument_info.100ms."+string(symbol))
	}

	param := PerpWebsocketV1PublicV2Params{
		Op:   PerpWebsocketV1PublicV2EventSubscribe,
		Args: args,
	}
	if err := s.addParamInstrumentInfoFunc(param.Key(), f); err != nil {
		return nil, err
	}
	buf, err := json.Marshal(param)
	if err != nil {
		return nil, err
	}
	if err := s.connection.WriteMessage(websocket.TextMessage, []byte(buf)); err != nil {
		return nil, err
	}

	return func() error {
		param.Op = PerpWebsocketV1PublicV2EventUnsubscribe
		buf, err := json.Marshal(param)
		if err != nil {
			return err
		}
		if err := s.connection.WriteMessage(websocket.TextMessage, []byte(buf)); err != nil {
			return err
		}
		for _, key := range param.Key() {
			s.removeParamTradeFunc(key)
		}
		return nil
	}, nil
}

// Start :
func (s *PerpWebsocketV1PublicV2Service) Start(ctx context.Context) {
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			if err := s.Run(); err != nil {
				if IsErrWebsocketClosed(err) {
					return
				}
				log.Println(err)
				return
			}
		}
	}()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := s.Ping(); err != nil {
				return
			}
		case <-ctx.Done():
			log.Println("interrupt")

			if err := s.Close(); err != nil {
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}

// Run :
func (s *PerpWebsocketV1PublicV2Service) Run() error {
	_, message, err := s.connection.ReadMessage()
	if err != nil {
		return err
	}

	topicFull, err := s.judgeTopic(message) // e.g. trade.BTCUSDT
	if err != nil {
		return err
	}

	var topic PerpWebsocketV1PublicV2Topic
	topicSplit := strings.Split(string(topicFull), ".")
	if len(topicSplit) > 0 {
		topic = PerpWebsocketV1PublicV2Topic(topicSplit[0])
	}

	switch topic {
	case PerpWebsocketV1PublicV2TopicOrderBookL2:
		var resp PerpWebsocketV1PublicV2OrderBookL2Response
		if err := s.parseResponse(message, &resp); err != nil {
			return err
		}
		f, err := s.retrieveOrderBookL2Func(resp.Key())
		if err != nil {
			return err
		}
		if err := f(resp); err != nil {
			return err
		}
	case PerpWebsocketV1PublicV2TopicTrade:
		var resp PerpWebsocketV1PublicV2TradeResponse
		if err := s.parseResponse(message, &resp); err != nil {
			return err
		}
		f, err := s.retrieveTradeFunc(resp.Key())
		if err != nil {
			return err
		}
		if err := f(resp); err != nil {
			return err
		}
	case PerpWebsocketV1PublicV2TopicInstrumentInfo:
		var resp PerpWebsocketV1PublicV2InstrumentInfoResponse
		if err := s.parseResponse(message, &resp); err != nil {
			return err
		}
		f, err := s.retrieveInstrumentInfoFunc(resp.Key())
		if err != nil {
			return err
		}
		if err := f(resp); err != nil {
			return err
		}
	}
	return nil
}

// Ping :
func (s *PerpWebsocketV1PublicV2Service) Ping() error {
	if err := s.connection.WriteMessage(websocket.PingMessage, nil); err != nil {
		return err
	}
	return nil
}

// Close :
func (s *PerpWebsocketV1PublicV2Service) Close() error {
	if err := s.connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		return err
	}
	return nil
}
