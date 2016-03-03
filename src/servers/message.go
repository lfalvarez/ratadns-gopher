//Package servers provides functions to process data of different redis channels adn send data via
//HTML5 SSE.
package servers

import (
	"errors"
	"encoding/json"
)

//Message struct is used to unmarshal the redis messages into an object.
//Payload is blank because that part of the message depends of the type of the message.
//This struct implements the interface Unmarshaler and Marshaler
type Message struct {
	ServerId  string `json:"serverId"`
	Type      string `json:"type"`
	Payload   interface{} `json:"-"`
	TimeStamp int64 `json:"timeStamp"`
}

//UnmarshallJSON function to implement the interface Unmarshaler.
//The function unmarshall the data of the redis message in different objects depending on the type
//of the message.
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

//MarshallJSON function to implement the interface marshaler.
//The function marshall the data of the redis message in different objects depending on the type
//of the message.
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

//QueriesSummary struct to marshal/unmarshal redis messages of type QueriesSummary.
//The struct is a slice, and every element of the slice has the Ip of the sender (in hexadecimal), a map
//of query type to an slice of urls and the location (the location is only for the marshaling).
type QueriesSummary []*struct {
	Ip       string `json:"ip"`
	Queries  map[string][]string `json:"queries"`
	Location Location `json:"location"`
}

//Location struct to marshal the location of QueriesSummary.
//The struct has a longitude, a latitude and the name of the country of that location.
type Location struct {
	Longitude   float64 `json:"longitude"`
	Latitude    float64 `json:"latitude"`
	CountryName string `json:"country_name"`
}

//QueryWithUnderscoredName is an element of QueriesWithUnderscoredName.
//The struct has the IP of the sender in hexadecimal, the IP of the receiver in hexadecimal, the malformed query and
//the query type.
type QueryWithUnderscoredName struct {
	Sender string `json:"sender"`
	Server string `json:"server"`
	Query  string `json:"query"`
	QType  int32  `json:"qtype"`
}

//QueriesWithUnderscoredName struct to marshal/unmarshal redis messages of type QueryWithUnderscoredName.
//The struct is a map of the malformed query to a slice of QueryWithUnderscoredName.
type QueriesWithUnderscoredName map[string][]*QueryWithUnderscoredName

//QueryNameCounter struct to marshal/unmarshal redis messages of type QueryNameCounter.
//The struct is a map of the url to the times it was called.
type QueryNameCounter map[string]int

//QueryCounter struct that has a query and the times that query was called.
//It differs with QueryNameCounter at that this struct is used to create an ordered list in QueriesCounter.
type QueryCounter struct{
	Query string
	Counter int
}

//MarshalBinary function to implement interface BinaryMarshaler.
func (qc QueryCounter) MarshalBinary() (bs []byte, err error){
	type Alias QueryCounter
	return json.Marshal(&struct{Alias}{Alias: (Alias)(qc)})
}

//QueiresCounter struct is a slice of QueryCounter.
type QueriesCounter []QueryCounter

//MarshalBinary function to implement interface BinaryMarshaler
func (qc QueriesCounter) MarshalBinary() (bs []byte, err error){
	type Alias QueriesCounter
	return json.Marshal(&struct{Alias}{Alias: (Alias)(qc)})
}

//Len, Swap and Less are functions to order a slice of QueriesCounter
func (qc QueriesCounter) Len() int           { return len(qc) }
func (qc QueriesCounter) Swap(i, j int)      { qc[i], qc[j] = qc[j], qc[i] }
func (qc QueriesCounter) Less(i, j int) bool { return qc[i].Counter < qc[j].Counter }