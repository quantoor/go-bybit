package bybit

import (
	"github.com/gorilla/websocket"
)

// PerpWebsocketV1Service :
type PerpWebsocketV1Service struct {
	client *WebSocketClient
}

// PublicV1 :
func (s *PerpWebsocketV1Service) PublicV1() (*PerpWebsocketV1PublicV1Service, error) {
	url := s.client.baseURL + SpotWebsocketV1PublicV1Path
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	return &PerpWebsocketV1PublicV1Service{
		connection:    c,
		paramTradeMap: map[PerpWebsocketV1PublicV1TradeParamKey]func(PerpWebsocketV1PublicV1TradeResponse) error{},
	}, nil
}

// PublicV2 :
func (s *PerpWebsocketV1Service) PublicV2() (*PerpWebsocketV1PublicV2Service, error) {
	url := s.client.baseURL + PerpWebsocketV1PublicV2Path
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	return &PerpWebsocketV1PublicV2Service{
		connection:             c,
		paramTradeMap:          map[string]func(PerpWebsocketV1PublicV2TradeResponse) error{},
		paramInstrumentInfoMap: map[string]func(response PerpWebsocketV1PublicV2InstrumentInfoResponse) error{},
	}, nil
}

// Private :
func (s *PerpWebsocketV1Service) Private() (*PerpWebsocketV1PrivateService, error) {
	url := s.client.baseURL + SpotWebsocketV1PrivatePath
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	return &PerpWebsocketV1PrivateService{
		client:                      s.client,
		connection:                  c,
		paramOutboundAccountInfoMap: map[PerpWebsocketV1PrivateParamKey]func(PerpWebsocketV1PrivateOutboundAccountInfoResponse) error{},
	}, nil
}
