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

	paramOutboundAccountInfoMap map[PerpWebsocketV1PrivateParamKey]func(PerpWebsocketV1PrivateOutboundAccountInfoResponse) error
}

const (
	// PerpWebsocketV1PrivatePath :
	PerpWebsocketV1PrivatePath = "/perp/ws"
)

// PerpWebsocketV1PrivateEventType :
type PerpWebsocketV1PrivateEventType string

const (
	// PerpWebsocketV1PrivateEventTypeOutboundAccountInfo :
	PerpWebsocketV1PrivateEventTypeOutboundAccountInfo = "outboundAccountInfo"
)

// PerpWebsocketV1PrivateParamKey :
type PerpWebsocketV1PrivateParamKey struct {
	EventType PerpWebsocketV1PrivateEventType
}

// PerpWebsocketV1PrivateOutboundAccountInfoResponse :
type PerpWebsocketV1PrivateOutboundAccountInfoResponse struct {
	Content PerpWebsocketV1PrivateOutboundAccountInfoResponseContent
}

// PerpWebsocketV1PrivateOutboundAccountInfoResponseContent :
type PerpWebsocketV1PrivateOutboundAccountInfoResponseContent struct {
	EventType            PerpWebsocketV1PrivateEventType                                        `json:"e"`
	Timestamp            string                                                                 `json:"E"`
	AllowTrade           bool                                                                   `json:"T"`
	AllowWithdraw        bool                                                                   `json:"W"`
	AllowWDeposit        bool                                                                   `json:"D"`
	WalletBalanceChanges []PerpWebsocketV1PrivateOutboundAccountInfoResponseWalletBalanceChange `json:"B"`
}

// PerpWebsocketV1PrivateOutboundAccountInfoResponseWalletBalanceChange :
type PerpWebsocketV1PrivateOutboundAccountInfoResponseWalletBalanceChange struct {
	SymbolName       string `json:"a"`
	AvailableBalance string `json:"f"`
	ReservedBalance  string `json:"l"`
}

// UnmarshalJSON :
func (r *PerpWebsocketV1PrivateOutboundAccountInfoResponse) UnmarshalJSON(data []byte) error {
	parsedArrayData := []map[string]interface{}{}
	if err := json.Unmarshal(data, &parsedArrayData); err != nil {
		return err
	}
	if len(parsedArrayData) != 1 {
		return errors.New("unexpected response")
	}
	buf, err := json.Marshal(parsedArrayData[0])
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, &r.Content); err != nil {
		return err
	}
	return nil
}

// MarshalJSON :
func (r *PerpWebsocketV1PrivateOutboundAccountInfoResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Content)
}

// Key :
func (r *PerpWebsocketV1PrivateOutboundAccountInfoResponse) Key() PerpWebsocketV1PrivateParamKey {
	return PerpWebsocketV1PrivateParamKey{
		EventType: r.Content.EventType,
	}
}

// addParamOutboundAccountInfoFunc :
func (s *PerpWebsocketV1PrivateService) addParamOutboundAccountInfoFunc(param PerpWebsocketV1PrivateParamKey, f func(PerpWebsocketV1PrivateOutboundAccountInfoResponse) error) error {
	if _, exist := s.paramOutboundAccountInfoMap[param]; exist {
		return errors.New("already registered for this param")
	}
	s.paramOutboundAccountInfoMap[param] = f
	return nil
}

// retrieveOutboundAccountInfoFunc :
func (s *PerpWebsocketV1PrivateService) retrieveOutboundAccountInfoFunc(key PerpWebsocketV1PrivateParamKey) (func(PerpWebsocketV1PrivateOutboundAccountInfoResponse) error, error) {
	f, exist := s.paramOutboundAccountInfoMap[key]
	if !exist {
		return nil, errors.New("func not found")
	}
	return f, nil
}

type perpWebsocketV1PrivateEventJudge struct {
	EventType PerpWebsocketV1PrivateEventType
}

func (r *perpWebsocketV1PrivateEventJudge) UnmarshalJSON(data []byte) error {
	parsedData := map[string]interface{}{}
	if err := json.Unmarshal(data, &parsedData); err == nil {
		if event, ok := parsedData["e"].(string); ok {
			r.EventType = PerpWebsocketV1PrivateEventType(event)
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
	r.EventType = PerpWebsocketV1PrivateEventType(parsedArrayData[0]["e"].(string))
	return nil
}

// judgeEventType :
func (s *PerpWebsocketV1PrivateService) judgeEventType(respBody []byte) (PerpWebsocketV1PrivateEventType, error) {
	var result perpWebsocketV1PrivateEventJudge
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", err
	}
	return result.EventType, nil
}

// parseResponse :
func (s *PerpWebsocketV1PrivateService) parseResponse(respBody []byte, response interface{}) error {
	if err := json.Unmarshal(respBody, &response); err != nil {
		return err
	}
	return nil
}

// Subscribe :
func (s *PerpWebsocketV1PrivateService) Subscribe() error {
	param, err := s.client.buildAuthParam()
	if err != nil {
		return err
	}
	if err := s.connection.WriteMessage(websocket.TextMessage, param); err != nil {
		return err
	}
	return nil
}

// RegisterFuncOutboundAccountInfo :
func (s *PerpWebsocketV1PrivateService) RegisterFuncOutboundAccountInfo(f func(PerpWebsocketV1PrivateOutboundAccountInfoResponse) error) error {
	key := PerpWebsocketV1PrivateParamKey{
		EventType: PerpWebsocketV1PrivateEventTypeOutboundAccountInfo,
	}
	if err := s.addParamOutboundAccountInfoFunc(key, f); err != nil {
		return err
	}
	return nil
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

	topic, err := s.judgeEventType(message)
	if err != nil {
		return err
	}
	switch topic {
	case PerpWebsocketV1PrivateEventTypeOutboundAccountInfo:
		var resp PerpWebsocketV1PrivateOutboundAccountInfoResponse
		if err := s.parseResponse(message, &resp); err != nil {
			return err
		}
		f, err := s.retrieveOutboundAccountInfoFunc(resp.Key())
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
