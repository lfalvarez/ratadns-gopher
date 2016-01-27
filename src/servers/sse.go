package servers

import (
	"../sse"
	"gopkg.in/redis.v3"
	"fmt"
	"sort"
	"time"
	"strconv"
	"encoding/json"
	"reflect"
)

func TopKEvent(eventManager *sse.EventManager, client *redis.Client) {
	topk, err := client.Subscribe("topk")
	if err != nil {
		panic(err)
	}
	redisWriter := redis.NewClient(&redis.Options{
		Addr:     "172.17.66.212:6379", // TODO: Exportar a archivo de configuración
	})
	malformed, err := client.Subscribe("QueriesWithUnderscoredName")
	if err != nil {
		panic(err)
	}
	times := []string{"60", "300", "900"}
	script := "local old_jsons = redis.call('zrangebyscore', KEYS[1], '-inf' , ARGV[1]);" +
	"redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1]);" +
	"return old_jsons;"
	go obtainTopK(times, script, "nameCount", processTopK, topk, redisWriter, eventManager)

	go obtainTopK(times, script, "malformed", processMalformed, malformed, redisWriter, eventManager)
}

func processMalformed(malformed *redis.PubSub) (orderedValues QueriesCounter, malformedMsg Message, err error){
	err = nil
	jsonMsg, err := malformed.ReceiveMessage()
	if err != nil {
		return
	}
	err = malformedMsg.UnmarshalJSON([]byte(jsonMsg.Payload))
	if err != nil {
		return
	}
	orderedValues = make(QueriesCounter, len(*malformedMsg.Payload.(*QueriesWithUnderscoredName)))
	counter := 0
	for key, query := range *malformedMsg.Payload.(*QueriesWithUnderscoredName) {
		orderedValues[counter] = QueryCounter{key, len(query)}
		counter++
	}
	sort.Sort(sort.Reverse(orderedValues))
	return
}

func processTopK(topk *redis.PubSub) (orderedValues QueriesCounter, msg Message, err error){
	jsonMsg, err := topk.ReceiveMessage()
	if err != nil {
		return
	}
	err = msg.UnmarshalJSON([]byte(jsonMsg.Payload))
	if err != nil {
		return
	}
	orderedValues = make(QueriesCounter, len(*msg.Payload.(*QueryNameCounter)))
	counter := 0
	for value, i := range *msg.Payload.(*QueryNameCounter) {
		orderedValues[counter] = QueryCounter{value, i}
		counter++
	}
	sort.Sort(sort.Reverse(orderedValues))
	return
}

func obtainTopK(times []string, script string, name string, process func(*redis.PubSub) (QueriesCounter, Message, error), redisSubscribe *redis.PubSub, redisWriter *redis.Client, eventManager *sse.EventManager){
	for {
		orderedValues, msg, err:= process(redisSubscribe)
		if err != nil{
			fmt.Println(err)
			continue
		}
		for _, seconds := range times {
			//TODO: launch the process for every second in a different thread?
			historicChannel := "historicNameCounts:" + msg.ServerId + ":" + seconds
			serverChannel := name + ":" + msg.ServerId + ":" + seconds
			globalChannel := name + ":GLOBAL:" + msg.ServerId + ":" + seconds
			multi, err := redisWriter.Watch(historicChannel,
				serverChannel,
				globalChannel)
			if err != nil {
				panic(err)
			}
			now := float64(time.Now().UnixNano() / 1000000)
			zAdd := redisWriter.ZAdd(historicChannel, redis.Z{now, orderedValues})
			if zAdd.Err() != nil {
				panic(zAdd.Err())
			}
			for i := 0; i < len(orderedValues); i++ {
				serverZIncrBy := multi.ZIncrBy(serverChannel, float64(orderedValues[i].Counter), orderedValues[i].Query)
				if serverZIncrBy.Err() != nil {
					panic(serverZIncrBy.Err())
				}
				globalZIncrBy := multi.ZIncrBy(globalChannel, float64(orderedValues[i].Counter), orderedValues[i].Query)
				if globalZIncrBy.Err() != nil {
					panic(globalZIncrBy.Err())
				}
			}
			_, err = multi.Exec(func() error {
				return nil
			})
			if err != nil {
				panic(err)
			}

			floatSeconds, err := strconv.ParseFloat(seconds, 64)
			if err != nil {
				panic(err)
			}
			timeRange := strconv.FormatFloat(now - floatSeconds * 1000, 'g', -1, 64)
			scriptEval := redisWriter.Eval(script, []string{historicChannel}, []string{timeRange})
			if scriptEval.Err() != nil {
				panic(scriptEval.Err())
			}
			s := scriptEval.Val().([]interface{})
			for i := 0; i < len(s); i++ {
				var oldQueries []interface{}
				if err = json.Unmarshal([]byte(s[i].(string)), &oldQueries); err != nil {
					fmt.Println("Couldn't unmarshall values")
					continue
				}
				for j := 0; j < len(oldQueries); j++ {
					url := reflect.ValueOf(oldQueries[j]).Index(0).Interface().(string)
					counter := -1 * reflect.ValueOf(oldQueries[j]).Index(1).Interface().(float64)
					if serverZIncrBy := multi.ZIncrBy(serverChannel, counter, url); serverZIncrBy.Err() != nil {
						panic(serverZIncrBy.Err())
					}
					if globalZIncrBy := multi.ZIncrBy(globalChannel, counter, url); globalZIncrBy.Err() != nil {
						panic(globalZIncrBy.Err())
					}
				}
				multi.ZRemRangeByScore(serverChannel, "-inf", "0")
				multi.ZRemRangeByScore(globalChannel, "-inf", "0")
			}
			if _, err := multi.Exec(func() error {
				return nil
			}); err != nil {
				panic(err)
			}
			serverTopK := multi.ZRevRangeWithScores(serverChannel, 0, 4)
			if serverTopK.Err() != nil {
				panic(err)
			}
			globalTopK := multi.ZRevRangeWithScores(globalChannel, 0, 4)
			if globalTopK.Err() != nil {
				panic(err)
			}
			_, err = multi.Exec(func() error {
				return nil
			})
			if err != nil {
				panic(err)
			}
			serverTopKValues := make([][]string, len(serverTopK.Val()))
			for i, value := range serverTopK.Val() {
				serverTopKValues[i] = []string{value.Member.(string), strconv.FormatFloat(value.Score, 'f', -1, 64)}
			}
			serverTopkMessage := Message{ServerId:msg.ServerId, Type: name + "TopK:" + msg.ServerId + ":" + seconds,
				TimeStamp:time.Now().UnixNano() / 1000000, Payload:serverTopKValues}
			serverOutputMsg, err := serverTopkMessage.MarshalJSON()
			if err != nil {
				panic(err)
			}
			globalTopKValues := make([][]string, len(globalTopK.Val()))
			for i, value := range globalTopK.Val() {
				globalTopKValues[i] = []string{value.Member.(string), strconv.FormatFloat(value.Score, 'f', -1, 64)}
			}
			globalTopkMessage := Message{ServerId:"GLOBAL", Type: name + "TopK:GLOBAL:" + seconds,
				TimeStamp:time.Now().UnixNano() / 1000000, Payload:globalTopKValues}
			globalOutputMsg, err := globalTopkMessage.MarshalJSON()
			if err != nil {
				panic(err)
			}
			eventManager.InputChannel <- []byte(serverOutputMsg)
			eventManager.InputChannel <- []byte(globalOutputMsg)
			multi.Close()
		}
	}
}