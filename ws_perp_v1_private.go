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

// PerpWebsocketV1PrivateService :
type PerpWebsocketV1PrivateService struct {
	client     *WebSocketClient
	connection *websocket.Conn

	paramOrderEventMap map[string]func(PerpWebsocketV1PrivateV2OrderEventResponse) error
}

const (
	// PerpWebsocketV1PrivatePath :
	PerpWebsocketV1PrivatePath = "/realtime_private"
)

// PerpWebsocketV1PrivateEventType :
type PerpWebsocketV1PrivateEventType string

const (
	// PerpWebsocketV1PrivateEventTypeOrder :
	PerpWebsocketV1PrivateEventTypeOrder = "order"
)

// PerpWebsocketV1PrivateParamKey :
type PerpWebsocketV1PrivateParamKey struct {
	EventType PerpWebsocketV1PrivateEventType
}

type perpWebsocketV1PrivateEventJudge struct {
	Topic PerpWebsocketV1PrivateEventType `json:"topic"`
}

func (r *perpWebsocketV1PrivateEventJudge) UnmarshalJSON(data []byte) error {
	parsedData := map[string]interface{}{}
	if err := json.Unmarshal(data, &parsedData); err == nil {
		if event, ok := parsedData["e"].(string); ok {
			r.Topic = PerpWebsocketV1PrivateEventType(event)
		}
		if authStatus, ok := parsedData["auth"].(string); ok {
			if authStatus != "success" {
				return errors.New("auth failed")
			}
		}
		return nil
	}

	parsedArrayData := []map[string]interface{}{}
	if err := json.Unmarshal(data, &parsedArrayData); err != nil {
		return err
	}
	if len(parsedArrayData) != 1 {
		return errors.New("unexpected response")
	}
	r.Topic = PerpWebsocketV1PrivateEventType(parsedArrayData[0]["e"].(string))
	return nil
}

// judgeEventType :
func (s *PerpWebsocketV1PrivateService) judgeTopic(respBody []byte) (PerpWebsocketV1PrivateEventType, error) {
	//result := perpWebsocketV1PrivateEventJudge{}
	result := struct {
		Topic PerpWebsocketV1PrivateEventType `json:"topic"`
	}{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", err
	}
	return result.Topic, nil
}

// parseResponse :
func (s *PerpWebsocketV1PrivateService) parseResponse(respBody []byte, response interface{}) error {
	if err := json.Unmarshal(respBody, &response); err != nil {
		return err
	}
	return nil
}

// Authenticate :
func (s *PerpWebsocketV1PrivateService) Authenticate() error {
	param, err := s.client.buildAuthParam()
	if err != nil {
		return err
	}
	if err := s.connection.WriteMessage(websocket.TextMessage, param); err != nil {
		return err
	}
	return nil
}

// PerpWebsocketV1PrivateV2OrderEventResponse :
type PerpWebsocketV1PrivateV2OrderEventResponse struct {
	Topic  PerpWebsocketV1PublicV2Topic                `json:"topic"`
	Action string                                      `json:"action"`
	Data   []PerpWebsocketV1PrivateV2OrderEventContent `json:"data"`
}

// PerpWebsocketV1PrivateV2OrderEventContent :
type PerpWebsocketV1PrivateV2OrderEventContent struct {
	OrderID string `json:"order_id"`
	//OrderID string `json:"order_link_id"`
	Symbol            string  `json:"symbol"`
	Side              string  `json:"side"`
	OrderType         string  `json:"order_type"`
	Price             float64 `json:"price"`
	Quantity          float64 `json:"qty"`
	RemainingQuantity float64 `json:"leaves_qty"`
	//OrderID string `json:"last_exec_price"`
	//OrderID string `json:"cum_exec_qty"`
	//OrderID string `json:"cum_exec_value"`
	//OrderID string `json:"cum_exec_fee"`
	TimeInForce string `json:"time_in_force"`
	CreateType  string `json:"create_type"`
	CancelType  string `json:"cancel_type"`
	OrderStatus string `json:"order_status"`
	//OrderID string `json:"take_profit"`
	//OrderID string `json:"stop_loss"`
	//OrderID string `json:"trailing_stop"`
	CreateTime string `json:"create_time"`
	UpdateTime string `json:"update_time"`
	ReduceOnly bool   `json:"reduce_only"`
	//OrderID string `json:"close_on_trigger"`
	//OrderID string `json:"position_idx"`
}

// Key :
func (p *PerpWebsocketV1PrivateV2OrderEventResponse) Key() string {
	return string(p.Topic)
}

// addParamOrderEventFunc :
func (s *PerpWebsocketV1PrivateService) addParamOrderEventFunc(params []string, f func(PerpWebsocketV1PrivateV2OrderEventResponse) error) error {
	for _, param := range params {
		if _, exist := s.paramOrderEventMap[param]; exist {
			return errors.New("already registered for this param")
		}
		s.paramOrderEventMap[param] = f
	}
	return nil
}

// removeParamOrderEventFunc :
func (s *PerpWebsocketV1PrivateService) removeParamOrderEventFunc(key string) {
	delete(s.paramOrderEventMap, key)
}

// retrieveOrderEventFunc :
func (s *PerpWebsocketV1PrivateService) retrieveOrderEventFunc(key string) (func(PerpWebsocketV1PrivateV2OrderEventResponse) error, error) {
	f, exist := s.paramOrderEventMap[key]
	if !exist {
		return nil, errors.New("func not found")
	}
	return f, nil
}

// SubscribeOrderEvent :
func (s *PerpWebsocketV1PrivateService) SubscribeOrderEvent(f func(response PerpWebsocketV1PrivateV2OrderEventResponse) error) (func() error, error) {

	param := PerpWebsocketV1PublicV2Params{
		Op:   "subscribe",
		Args: []string{"order"},
	}
	if err := s.addParamOrderEventFunc(param.Key(), f); err != nil {
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
		param.Op = "unsubscribe"
		buf, err := json.Marshal(param)
		if err != nil {
			return err
		}
		if err := s.connection.WriteMessage(websocket.TextMessage, []byte(buf)); err != nil {
			return err
		}
		for _, key := range param.Key() {
			s.removeParamOrderEventFunc(key)
		}
		return nil
	}, nil
}

// Start :
func (s *PerpWebsocketV1PrivateService) Start(ctx context.Context) {
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
func (s *PerpWebsocketV1PrivateService) Run() error {
	_, message, err := s.connection.ReadMessage()
	if err != nil {
		return err
	}

	topic, err := s.judgeTopic(message)
	if err != nil {
		return err
	}

	switch topic {
	case PerpWebsocketV1PrivateEventTypeOrder:
		var resp PerpWebsocketV1PrivateV2OrderEventResponse
		if err := s.parseResponse(message, &resp); err != nil {
			return err
		}
		f, err := s.retrieveOrderEventFunc(resp.Key())
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
func (s *PerpWebsocketV1PrivateService) Ping() error {
	if err := s.connection.WriteMessage(websocket.PingMessage, nil); err != nil {
		return err
	}
	return nil
}

// Close :
func (s *PerpWebsocketV1PrivateService) Close() error {
	if err := s.connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		return err
	}
	return nil
}
