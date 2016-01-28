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
		manageError(err)
		for _, seconds := range times {
			//TODO: launch the process for every second in a different thread?
			historicChannel := "historicNameCounts:" + msg.ServerId + ":" + seconds
			serverChannel := name + ":" + msg.ServerId + ":" + seconds
			globalChannel := name + ":GLOBAL:" + msg.ServerId + ":" + seconds
			multi, err := redisWriter.Watch(historicChannel,
				serverChannel,
				globalChannel)
			if manageError(err){continue}
			now := float64(time.Now().UnixNano() / 1000000)
			zAdd := redisWriter.ZAdd(historicChannel, redis.Z{now, orderedValues})
			if manageError(zAdd.Err()){continue}
			for i := 0; i < len(orderedValues); i++ {
				zIncrBy(multi, serverChannel, orderedValues[i])
				zIncrBy(multi, globalChannel, orderedValues[i])
			}
			execMulti(multi)
			floatSeconds, err := strconv.ParseFloat(seconds, 64)
			if manageError(err){continue}
			timeRange := strconv.FormatFloat(now - floatSeconds * 1000, 'g', -1, 64)
			scriptEval := redisWriter.Eval(script, []string{historicChannel}, []string{timeRange})
			if manageError(scriptEval.Err()){continue}
			s := scriptEval.Val().([]interface{})
			for i := 0; i < len(s); i++ {
				var oldQueries []interface{}
				err = json.Unmarshal([]byte(s[i].(string)), &oldQueries)
				if manageError(err){continue}
				for j := 0; j < len(oldQueries); j++ {
					url := reflect.ValueOf(oldQueries[j]).Index(0).Interface().(string)
					counter := -1 * reflect.ValueOf(oldQueries[j]).Index(1).Interface().(float64)
					serverZIncrBy := multi.ZIncrBy(serverChannel, counter, url)
					if manageError(serverZIncrBy.Err()){continue}
					globalZIncrBy := multi.ZIncrBy(globalChannel, counter, url)
					if manageError(globalZIncrBy.Err()){continue}
				}
				multi.ZRemRangeByScore(serverChannel, "-inf", "0")
				multi.ZRemRangeByScore(globalChannel, "-inf", "0")
			}
			execMulti(multi)
			serverTopK := multi.ZRevRangeWithScores(serverChannel, 0, 4)
			if manageError(serverTopK.Err()){continue}
			globalTopK := multi.ZRevRangeWithScores(globalChannel, 0, 4)
			if manageError(globalTopK.Err()){continue}
			execMulti(multi)
			eventManager.InputChannel <- []byte(getOutputMessage(serverTopK.Val(), msg.ServerId, name, seconds))
			eventManager.InputChannel <- []byte(getOutputMessage(globalTopK.Val(), "GLOBAL", name, seconds))
			multi.Close()
		}
	}
}

func zIncrBy(multi *redis.Multi, channel string, qc QueryCounter){
	incrBy := multi.ZIncrBy(channel, float64(qc.Counter), qc.Query)
	if incrBy.Err() != nil {
		panic(incrBy.Err())
	}
}

func getOutputMessage(values []redis.Z, serverId string, name string, seconds string) []byte{
	serverTopKValues := make([][]string, len(values))
	for i, value := range values {
		serverTopKValues[i] = []string{value.Member.(string), strconv.FormatFloat(value.Score, 'f', -1, 64)}
	}
	serverTopkMessage := Message{ServerId:serverId, Type: name + "TopK:" + serverId + ":" + seconds,
		TimeStamp:time.Now().UnixNano() / 1000000, Payload:serverTopKValues}
	outputMessage, err := serverTopkMessage.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return outputMessage
}

func execMulti(multi *redis.Multi){
	if _, err := multi.Exec(func() error {
		return nil
	}); err != nil {
		fmt.Println(err)//TODO:logger
	}
}

func manageError(err error) bool{
	if err != nil{
		fmt.Println(err)
		return true
	}
	return false
}