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

	paramExecutionEventMap map[string]func(PerpWebsocketV1PrivateExecutionEventResponse) error
	paramOrderEventMap     map[string]func(PerpWebsocketV1PrivateOrderEventResponse) error
}

const (
	// PerpWebsocketV1PrivatePath :
	PerpWebsocketV1PrivatePath = "/realtime_private"
)

// PerpWebsocketV1PrivateEventType :
type PerpWebsocketV1PrivateEventType string

const (
	// PerpWebsocketV1PrivateEventTypeExecution :
	PerpWebsocketV1PrivateEventTypeExecution = "execution"
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

// PerpWebsocketV1PrivateExecutionEventResponse :
type PerpWebsocketV1PrivateExecutionEventResponse struct {
	Topic  PerpWebsocketV1PublicV2Topic              `json:"topic"`
	Action string                                    `json:"action"`
	Data   []PerpWebsocketV1PrivateOrderEventContent `json:"data"`
}

// PerpWebsocketV1PrivateExecutionEventContent :
type PerpWebsocketV1PrivateExecutionEventContent struct {
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
func (p *PerpWebsocketV1PrivateExecutionEventResponse) Key() string {
	return string(p.Topic)
}

// addParamExecutionEventFunc :
func (s *PerpWebsocketV1PrivateService) addParamExecutionEventFunc(params []string, f func(PerpWebsocketV1PrivateExecutionEventResponse) error) error {
	for _, param := range params {
		if _, exist := s.paramExecutionEventMap[param]; exist {
			return errors.New("already registered for this param")
		}
		s.paramExecutionEventMap[param] = f
	}
	return nil
}

// removeParamExecutionEventFunc :
func (s *PerpWebsocketV1PrivateService) removeParamExecutionEventFunc(key string) {
	delete(s.paramOrderEventMap, key)
}

// retrieveExecutionEventFunc :
func (s *PerpWebsocketV1PrivateService) retrieveExecutionEventFunc(key string) (func(PerpWebsocketV1PrivateExecutionEventResponse) error, error) {
	f, exist := s.paramExecutionEventMap[key]
	if !exist {
		return nil, errors.New("func not found")
	}
	return f, nil
}

// SubscribeExecutionEvent :
func (s *PerpWebsocketV1PrivateService) SubscribeExecutionEvent(f func(response PerpWebsocketV1PrivateExecutionEventResponse) error) (func() error, error) {

	param := PerpWebsocketV1PublicV2Params{
		Op:   "subscribe",
		Args: []string{"execution"},
	}
	if err := s.addParamExecutionEventFunc(param.Key(), f); err != nil {
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

// PerpWebsocketV1PrivateOrderEventResponse :
type PerpWebsocketV1PrivateOrderEventResponse struct {
	Topic  PerpWebsocketV1PublicV2Topic              `json:"topic"`
	Action string                                    `json:"action"`
	Data   []PerpWebsocketV1PrivateOrderEventContent `json:"data"`
}

// PerpWebsocketV1PrivateOrderEventContent :
type PerpWebsocketV1PrivateOrderEventContent struct {
	OrderID        string    `json:"order_id"`
	OrderLinkID    string    `json:"order_link_id"`
	Symbol         string    `json:"symbol"`
	Side           string    `json:"side"`
	OrderType      string    `json:"order_type"`
	Price          float64   `json:"price"`
	Qty            float64   `json:"qty"`
	LeavesQty      float64   `json:"leaves_qty"`
	LastExecPrice  float64   `json:"last_exec_price"`
	CumExecQty     float64   `json:"cum_exec_qty"`
	CumExecValue   float64   `json:"cum_exec_value"`
	CumExecFee     float64   `json:"cum_exec_fee"`
	TimeInForce    string    `json:"time_in_force"`
	CreateType     string    `json:"create_type"`
	CancelType     string    `json:"cancel_type"`
	OrderStatus    string    `json:"order_status"`
	TakeProfit     float64   `json:"take_profit"`
	StopLoss       float64   `json:"stop_loss"`
	TrailingStop   float64   `json:"trailing_stop"`
	CreateTime     time.Time `json:"create_time"`
	UpdateTime     time.Time `json:"update_time"`
	ReduceOnly     bool      `json:"reduce_only"`
	CloseOnTrigger bool      `json:"close_on_trigger"`
	PositionIdx    string    `json:"position_idx"`
}

// Key :
func (p *PerpWebsocketV1PrivateOrderEventResponse) Key() string {
	return string(p.Topic)
}

// addParamOrderEventFunc :
func (s *PerpWebsocketV1PrivateService) addParamOrderEventFunc(params []string, f func(PerpWebsocketV1PrivateOrderEventResponse) error) error {
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
func (s *PerpWebsocketV1PrivateService) retrieveOrderEventFunc(key string) (func(PerpWebsocketV1PrivateOrderEventResponse) error, error) {
	f, exist := s.paramOrderEventMap[key]
	if !exist {
		return nil, errors.New("func not found")
	}
	return f, nil
}

// SubscribeOrderEvent :
func (s *PerpWebsocketV1PrivateService) SubscribeOrderEvent(f func(response PerpWebsocketV1PrivateOrderEventResponse) error) (func() error, error) {

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
	case PerpWebsocketV1PrivateEventTypeExecution:
		var resp PerpWebsocketV1PrivateExecutionEventResponse
		if err := s.parseResponse(message, &resp); err != nil {
			return err
		}
		f, err := s.retrieveExecutionEventFunc(resp.Key())
		if err != nil {
			return err
		}
		if err := f(resp); err != nil {
			return err
		}
	case PerpWebsocketV1PrivateEventTypeOrder:
		var resp PerpWebsocketV1PrivateOrderEventResponse
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
