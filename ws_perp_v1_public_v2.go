package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
)

// PerpWebsocketV1PublicV2Service :
type PerpWebsocketV1PublicV2Service struct {
	connection *websocket.Conn

	paramTradeMap map[PerpWebsocketV1PublicV2TradeParamKey]func(PerpWebsocketV1PublicV2TradeResponse) error
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
	PerpWebsocketV1PublicV2TopicTrade = PerpWebsocketV1PublicV2Topic("trade")
)

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
func (p *PerpWebsocketV1PublicV2TradeResponse) Key() PerpWebsocketV1PublicV2TradeParamKey {
	return PerpWebsocketV1PublicV2TradeParamKey{
		//Symbol: p.Params.Symbol,
		Topic: p.Topic,
	}
}

// PerpWebsocketV1PublicV2TradeParamChild :
type PerpWebsocketV1PublicV2TradeParamChild struct {
	Symbol SymbolPerp `json:"symbol"`
	Binary bool       `json:"binary"`
}

// PerpWebsocketV1PublicV2TradeParam :
type PerpWebsocketV1PublicV2TradeParam struct {
	Op   PerpWebsocketV1PublicV2Topic `json:"op"`
	Args []string                     `json:"args"` //PerpWebsocketV1PublicV2TradeParamChild `json:"args"`
}

// Key :
func (p *PerpWebsocketV1PublicV2TradeParam) Key() PerpWebsocketV1PublicV2TradeParamKey {
	return PerpWebsocketV1PublicV2TradeParamKey{PerpWebsocketV1PublicV2Topic(p.Args[0])} // todo
}

// addParamTradeFunc :
func (s *PerpWebsocketV1PublicV2Service) addParamTradeFunc(param PerpWebsocketV1PublicV2TradeParamKey, f func(PerpWebsocketV1PublicV2TradeResponse) error) error {
	if _, exist := s.paramTradeMap[param]; exist {
		return errors.New("already registered for this param")
	}
	s.paramTradeMap[param] = f
	return nil
}

// removeParamTradeFunc :
func (s *PerpWebsocketV1PublicV2Service) removeParamTradeFunc(key PerpWebsocketV1PublicV2TradeParamKey) {
	delete(s.paramTradeMap, key)
}

// retrieveTradeFunc :
func (s *PerpWebsocketV1PublicV2Service) retrieveTradeFunc(key PerpWebsocketV1PublicV2TradeParamKey) (func(PerpWebsocketV1PublicV2TradeResponse) error, error) {
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
func (s *PerpWebsocketV1PublicV2Service) SubscribeTrade(symbol SymbolPerp, f func(response PerpWebsocketV1PublicV2TradeResponse) error) (func() error, error) {
	param := PerpWebsocketV1PublicV2TradeParam{
		Op:   PerpWebsocketV1PublicV2EventSubscribe,
		Args: []string{"trade." + string(symbol)},
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
		s.removeParamTradeFunc(param.Key())
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

	topic, err := s.judgeTopic(message)
	if err != nil {
		return err
	}
	switch topic {
	case "trade.BTCUSDT": //PerpWebsocketV1PublicV2TopicTrade:
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
