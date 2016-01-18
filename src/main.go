package main

import (
    "net/http"
    "gopkg.in/redis.v3"
    "./sse"
    "./servers"
    "encoding/json"
    "io/ioutil"
    "errors"
    "sync"
    "./util"
)


type Message struct {
    ServerId string `json:"serverId"`
    Type string `json:"type"`
    Payload interface {} `json:"-"`
    TimeStamp int64 `json:"timeStamp"`
}

func (m *Message) UnmarshalJSON(bs []byte) (err error) {
    err = nil
    type Alias Message
    alias := (*Alias)(m)
    err = json.Unmarshal(bs, &struct { *Alias }{ Alias: alias })
    if err != nil { return }
    switch m.Type {
    case "QueriesSummary":
        qs := &QueriesSummary {}
        err = json.Unmarshal(bs, &struct { Summary *QueriesSummary `json:"data"` }{ Summary : qs })
        if err != nil { return }
        m.Payload = qs
    case "QueryWithUndercoredName":
        qwun := &QueriesWithUnderscoredName {}
        err = json.Unmarshal(bs, &struct { Queries *QueriesWithUnderscoredName `json:"data"` }{ Queries : qwun })
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
    Ip string `json:"ip"`
    Queries map[string][]string `json:"queries"`
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
    Query string
    Qtype int32
}

type QueriesWithUnderscoredName map[string][]*QueryWithUnderscoredName

func GeoEvent(eventManager *sse.EventManager, client *redis.Client){
    /*
    malformed, err := client.Subscribe("QueriesWithUnderscoredName")
    if err != nil {
        panic(err)
    }
    */

    summary, err := client.Subscribe("QueriesSummary")
    if err != nil {
        panic(err)
    }
    clientsNumber := 0
    c1 := make(chan bool)
    // c2 := make(chan bool)
    lock := &sync.Mutex {}
    go func() {
        for {
            if clientsNumber > 0 {
                jsonMsg, err := summary.ReceiveMessage()
                if err != nil {
                    panic(err)
                }
                var msg Message
                err = msg.UnmarshalJSON([]byte(jsonMsg.Payload))
                if err != nil {panic(err)}
                for _, summaryEntry := range *msg.Payload.(*QueriesSummary) {
                    ip := util.HexToIp(summaryEntry.Ip)
                    summaryEntry.Ip = ip
                    res, err := http.Get("http://172.17.66.212:8080/json/" + ip)
                    if err != nil {
                        panic(err)
                    }
                    body, err := ioutil.ReadAll(res.Body)
                    var geoData Location
                    json.Unmarshal(body, &geoData)
                    summaryEntry.Location = geoData
                }
                outputBytes, err := msg.MarshalJSON()
                if err != nil {panic(err)}
                lock.Lock()
                if clientsNumber >0 {
                    eventManager.InputChannel <- outputBytes
                }
                lock.Unlock()
            } else {
                <-c1
            }
        }
    }()

    /*
    go func() {
        for {
            if clientsNumber >0 {
                jsonMsg, err := malformed.ReceiveMessage()
                if err != nil {
                    panic(err)
                }
                var malformedMsg Message
                err = malformedMsg.UnmarshalJSON([]byte(jsonMsg.Payload))
                outputMsg := &QueriesSummary {}
                for _, queries := range malformedMsg.Payload.(*QueriesWithUnderscoredName) {
                    for _, query := range queries {
                        ip := util.HexToIp(queries)

                        res, err := http.Get("http://172.17.66.212:8080/json/" + ip)
                        if err != nil {
                            panic(err)
                        }
                        body, err := ioutil.ReadAll(res.Body)

                        var geoData Location
                        json.Unmarshal(body, &geoData)

                        append(outputMsg, &{Ip: ip, Queries: nil, Location: geodata })

                    }

                }
                if err != nil {panic(err)}
                lock.Lock()
                if clientsNumber >0 {
                    eventManager.InputChannel <-[]byte(malformedMsg.Payload)
                }
                lock.Unlock()
            } else {
                <-c2
            }
        }
    }()
    */
    go util.SynchronizeNbOfClients(lock, &clientsNumber, eventManager.ClientConnected, c1)
}

func NewRedisClient() (client *redis.Client) {
    client = redis.NewClient(&redis.Options{
        Addr:     "172.17.66.212:6379", // TODO: Exportar a archivo de configuración
    })

    return
}

func serversLocation(rw http.ResponseWriter, rq *http.Request) {
    // TODO: This is a mock, we must do a well structured way to do this.
    // Maybe a configuration file?
    servers := make(map[string]Location)
    servers["beaucheff"] = Location{ Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }
    servers["blanco"] = Location{ Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }

    encoder := json.NewEncoder(rw)
    encoder.Encode(servers)
}

func main() {
    redisClient := NewRedisClient()

    //servData
    servDataManager := sse.NewEventManager()
    go servDataManager.Listen()
    servers.ServDataEvent(servDataManager, redisClient)

    servDataServer := sse.NewSSEServer(servDataManager, func(in []byte) []byte {
        return in
    })

    //geo
    geoManager := sse.NewEventManager()
    go geoManager.Listen()
    GeoEvent(geoManager, redisClient)

    geoServer := sse.NewSSEServer(geoManager, func(in []byte) []byte {
        return in
    })

    http.HandleFunc("/serversLocation", serversLocation)
    http.Handle("/servData", servDataServer)
    http.Handle("/geo", geoServer)
    http.ListenAndServe(":8080", nil)
}
