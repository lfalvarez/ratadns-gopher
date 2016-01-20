package servers
import (
	"errors"
	"encoding/json"
)

type Message struct {
	ServerId  string `json:"serverId"`
	Type      string `json:"type"`
	Payload   interface{} `json:"-"`
	TimeStamp int64 `json:"timeStamp"`
}

func (m *Message) UnmarshalJSON(bs []byte) (err error) {
	err = nil
	type Alias Message
	alias := (*Alias)(m)
	err = json.Unmarshal(bs, &struct{ *Alias }{Alias: alias })
	if err != nil { return }
	switch m.Type {
	case "QueriesSummary":
		qs := &QueriesSummary{}
		err = json.Unmarshal(bs, &struct{ Summary *QueriesSummary `json:"data"` }{Summary : qs })
		if err != nil { return }
		m.Payload = qs
	case "QueryWithUndercoredName":
		qwun := &QueriesWithUnderscoredName{}
		err = json.Unmarshal(bs, &struct{ Queries *QueriesWithUnderscoredName `json:"data"` }{Queries : qwun })
		if err != nil { return }
		m.Payload = qwun
	}
	return
}

func (m *Message) MarshalJSON() (bs []byte, err error) {
	type Alias Message
	switch m.Type {
	case "QueriesSummary":
		qs := m.Payload.(*QueriesSummary)
		return json.Marshal(&struct {
			*Alias
			Summary *QueriesSummary `json:"data"`
		}{
			Alias: (*Alias)(m),
			Summary: qs,
		})
	default:
		return nil, errors.New("Message type not supported")
	}
}

type QueriesSummary []*struct {
	Ip       string `json:"ip"`
	Queries  map[string][]string `json:"queries"`
	Location Location `json:"location"`
}

type Location struct {
	Longitude   float64 `json:"longitude"`
	Latitude    float64 `json:"latitude"`
	CountryName string `json:"country_name"`
}

type QueryWithUnderscoredName struct {
	Sender string
	Server string
	Query  string
	Qtype  int32
}

type QueriesWithUnderscoredName map[string][]*QueryWithUnderscoredName