import queue
from typing import Mapping, Tuple, Any

import redis
import threading
import json

import socket
import struct
import urllib.request
import datetime
import time as Time


class EventConsumer(object):
    def __init__(self):
        self.queue = queue.Queue()

    def consume(self, data):
        self.queue.put(data)

    def get_data(self):
        print("Queue size={}".format(self.queue.qsize()))
        return self.queue.get()


class EventProcessor(threading.Thread):
    """One event processor for each URL"""

    def __init__(self, r: redis.StrictRedis):
        threading.Thread.__init__(self)
        self.pubsub = r.pubsub(ignore_subscribe_messages=True)
        self.consumers = []

    def register_consumer(self, event_consumer: EventConsumer):
        self.consumers.append(event_consumer)

    def unregister_consumer(self, event_consumer: EventConsumer):
        self.consumers.remove(event_consumer)

    def subscribe(self, channel: str):
        self.pubsub.subscribe(channel)

    def run(self):
        for serialized_item in self.pubsub.listen():
            item = json.loads(str(serialized_item['data'], "utf-8"))
            (do_consume, processed_item) = self.process(item)
            if do_consume:  # processed items must be published
                for consumer in self.consumers:
                    consumer.consume(processed_item)

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        pass


class ServerDataEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis):
        super().__init__(r)
        self.subscribe("QueriesPerSecond")
        self.subscribe("AnswersPerSecond")

    def process(self, item):
        return (True, item)


def hex_to_ip(ip_hex: str):
    if len(ip_hex) == 8 or len(ip_hex) == 7:
        ip = int(ip_hex, 16)
        return socket.inet_ntoa(struct.pack(">L", ip))
    else:  # IPV6 Not supported! (yet)
        return None  # Refactor this!


class QueriesSummaryEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.subscribe("QueriesSummary")
        self.config = config

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        for summary_entry in item['data']:
            ip = hex_to_ip(summary_entry['ip'])

            if ip == None:
                continue

            summary_entry['ip'] = ip
            url = "http://" + self.config['freegeoip']['address'] + ":" + str(self.config['freegeoip']['port']) + \
                  "/json/" + ip
            with urllib.request.urlopen(url) as freegeoip_server:
                location = json.loads(str(freegeoip_server.read(), "utf-8"))
                summary_entry['location'] = location

        return (True, item)


def order_name_count(item: Mapping[str, int]) -> list:
    ordered_data = []

    for name_counter in item:
        ordered_data.append(({'name': name_counter}, item[name_counter]))

    ordered_data = sorted(ordered_data, key=get_count)
    return ordered_data


def order_malformed_data(item: Mapping[str, Any]) -> list:
    ordered_data = {}

    for name_counter in item:
        if name_counter in ordered_data:
            number = ordered_data[name_counter][1] + 1
            ordered_data[name_counter] = ({'name': name_counter, 'data': item[name_counter]}, number)
        else:
            ordered_data[name_counter] = ({'name': name_counter, 'data': item[name_counter]}, 1)

    data_list = list(ordered_data.values())
    data_list = sorted(data_list, key=get_count)

    return data_list


def get_count(item: Tuple[str, int])-> int:
    return item[1]


def format_redis_data(l: list)->list:
    return list(map(lambda x: (x[0].decode("utf-8"), int(x[1])), l))


def redis_server_set(server: str, time: int) -> str:
    return "topk_{}_{}".format(time, server)


class TopCountEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any], config_data: Mapping[str, Any]):
        super().__init__(r)
        self.subscribe(config_data['channel'])
        self.redis = r
        self.name = config_data['redis_set']
        self.order_fun = config_data['order_fun']
        self.config = config

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        top_data = {}
        server_name = item['serverId']
        item.pop('serverId')
        name_tuples = self.order_fun(item['data'])

        now = Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

        for time_index in range(0, len(self.config[self.name]['times'])):
            time = self.config[self.name]['times'][time_index]*60
            server_set = self.name + ":" + redis_server_set(server_name, time)
            global_set = self.name + ":global_{}".format(time)
            historic_set = self.name + ":historic_jsons_" + server_name + "_" + str(time)

            self.redis.zadd(historic_set, now, json.dumps(name_tuples))

            multi = self.redis.pipeline()
            for element in name_tuples:
                multi.zincrby(server_set, element[0]['name'], element[1])
                multi.zincrby(global_set, element[0]['name'], element[1])

            multi.execute()

            script = """local old_jsons = redis.call('zrangebyscore', KEYS[1], '-inf' , ARGV[1]);
                        redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1]) ;
                        return old_jsons;"""

            get_json = self.redis.register_script(script)
            jsons = get_json(keys=[historic_set], args=[now - time * 1000.0])
          
            for i in range(0, len(jsons)):
                old_queries = json.loads(jsons[i].decode("utf-8"))

                for j in range(0, len(old_queries)):
                    print(old_queries[j])
                    multi.zincrby(server_set, old_queries[j][0]['name'], -1 * old_queries[j][1])
                    multi.zincrby(global_set, old_queries[j][0]['name'], -1 * old_queries[j][1])

                multi.zremrangebyscore(server_set, "-inf", 0)
                multi.zremrangebyscore(global_set, 0, 0)

            multi.execute()

            time_data = {}
            for server in self.config['servers']:
                time_data[server['name']] = format_redis_data(
                        self.redis.zrevrange(self.name + ":" + redis_server_set(server['name'], time),
                                             0, 4, withscores=True))

            time_data['global'] = format_redis_data(self.redis.zrevrange(global_set, 0, 4, withscores=True))
            top_data[self.config[self.name]['times'][time_index]] = time_data

        item['data'] = json.dumps(top_data)
        return (True,item)


class TopKEventProcessor(TopCountEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        topk_data = {'channel': "topk", "redis_set": "topk", "order_fun": order_name_count}
        super().__init__(r, config, topk_data)


class MalformedPacketsEventProcessor(TopCountEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        malformed_data = {'channel': "QueriesWithUnderscoredName", "redis_set": "malformed",
                          "order_fun": order_malformed_data}
        super().__init__(r, config, malformed_data)










