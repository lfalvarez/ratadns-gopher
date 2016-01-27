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
	case "QueryWithUnderscoredName":
		qwun := &QueriesWithUnderscoredName{}
		err = json.Unmarshal(bs, &struct{ Queries *QueriesWithUnderscoredName `json:"data"` }{Queries : qwun })
		if err != nil { return }
		m.Payload = qwun
	case "QueryNameCounter":
		qnm := &QueryNameCounter{}
		err = json.Unmarshal(bs, &struct{ Queries *QueryNameCounter `json:"data"`}{Queries : qnm})
		if err != nil { return }
		m.Payload = qnm
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
	case "QueryNameCounter":
		qnc := m.Payload.(*QueryNameCounter)
		return json.Marshal(&struct {
			*Alias
			Summary *QueryNameCounter `json:"data"`
		}{
			Alias: (*Alias)(m),
			Summary: qnc,
		})
	case "nameCountTopK:"+m.ServerId+":60", "nameCountTopK:"+m.ServerId+":300", "nameCountTopK:"+m.ServerId+":900","malformedTopK:"+m.ServerId+":60", "malformedTopK:"+m.ServerId+":300", "malformedTopK:"+m.ServerId+":900":
		qs := m.Payload.([][]string)
		return json.Marshal(&struct {
			*Alias
			Summary [][]string `json:"data"`
		}{
			Alias: (*Alias)(m),
			Summary: qs,
		})
	default:
		return nil, errors.New("Message type not supported " + m.Type)
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
	Sender string `json:"sender"`
	Server string `json:"server"`
	Query  string `json:"query"`
	QType  int32  `json:"qtype"`
}

type QueriesWithUnderscoredName map[string][]*QueryWithUnderscoredName

type QueryNameCounter map[string]int

type QueryCounter struct{
	Query string
	Counter int
}

func (qc QueryCounter) MarshalBinary() (bs []byte, err error){
	type Alias QueryCounter
	return json.Marshal(&struct{Alias}{Alias: (Alias)(qc)})
}

type QueriesCounter []QueryCounter

func (qc QueriesCounter) MarshalBinary() (bs []byte, err error){
	type Alias QueriesCounter
	return json.Marshal(&struct{Alias}{Alias: (Alias)(qc)})
}

func (qc QueriesCounter) Len() int           { return len(qc) }
func (qc QueriesCounter) Swap(i, j int)      { qc[i], qc[j] = qc[j], qc[i] }
func (qc QueriesCounter) Less(i, j int) bool { return qc[i].Counter < qc[j].Counter }