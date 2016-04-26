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


class WindowAlgorithmEventProcessor(EventProcessor):
    def __init__(self, name: str, r: redis.StrictRedis):
        super().__init__(r)
        self.name = name
        self.redis = r

    def order_data(self,  item: Mapping[str, int]) -> list:
        pass

    def timespan_set(self, server, time):
        pass

    def increase_set(self, element_list: Mapping[str, int], set_list: list, time_diff: int, server: str, time_index: int):
        multi = self.redis.pipeline()

        for element in element_list:
            for set in set_list:
                multi.zincrby(set, element[0]['name'], element[1])

        multi.execute()

    def increase_timespan(self, current_time, timestamp_set, data, total):
        self.redis.zadd(timestamp_set, current_time, data)

    def get_top(self, time_index, time, server_name):
        time_data = {}
        servers_total = 0

        for server in self.config['servers']:
            servers_total += self.total[server['name']][time_index]
            time_data[server['name']] = self.format_data(self.get_top_data(server['name'], time), self.total[server_name][time_index])
        return time_data

    def format_data(self):
        pass

    def server_list(self, server: str, time: int) -> list:
        pass

    def get_top_data(self, server: str, time: int):
        pass

    def cleanup_old_data(self, set_list, timespan_set, time_diff):
        multi = self.redis.pipeline()
        old_data = self.get_old_data(timespan_set, time_diff)

        for i in range(0, len(old_data)):
            old_queries = self.parse_old_data(old_data[i])

            for j in range(0, len(old_queries)):
                for k in range(0, len(set_list)):
                    multi.zincrby(set_list[k], old_queries[j][0]['name'], -1 * old_queries[j][1])

            for k in range(0, len(set_list)):
                multi.zremrangebyscore(set_list[i], "-inf", 0)

        multi.execute()

    def get_old_data(self, historic_set, time_diff):
        pass

    def parse_old_data(self, old_data):
        pass

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        data = {}
        ordered_data = self.order_data(item['data'])
        server_name = item['serverId']
        now = Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

        for time_index in range(0, len(self.config[self.name]['times'])):
            time = self.config[self.name]['times'][time_index]*60

            self.increase_timespan(now, self.timespan_set(server_name, time), json.dumps(ordered_data[0]), ordered_data[1])
            self.increase_set(ordered_data[0], self.server_list(server_name, time), now - time*1000.0, server_name, time_index)
            self.cleanup_old_data(self.server_list(server_name, time)[0:1], self.timespan_set(server_name, time), now-time*1000.0)

            data[time] = self.get_top(time_index, time, server_name)

        item['data'] = json.dumps(data)
        return (True,item)


class TopCountEventProcessor(WindowAlgorithmEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any], config_data: Mapping[str, Any]):
        super().__init__(config_data['redis_set'], r),
        self.subscribe(config_data['channel'])
        self.redis = r
        self.name = config_data['redis_set']
        self.config = config
        self.total = {}

        n = len(self.config[self.name]['times'])
        for server in self.config['servers']:
            self.total[server['name']] = [0]*n

    def timespan_set(self, server, time):
        return [self.name + ":historic_jsons_{}_{}".format(server, time),  self.name + ":total_{}_{}".format(server, time)]

    def increase_timespan(self, current_time, set_list, data, total):
        super().increase_timespan(current_time, set_list[0], data[0], total)
        self.redis.zadd(set_list[1], current_time, total)

    def increase_set(self, element_list: Mapping[str, int], set_list: list, time_diff: int, server: str, time_index: int):
        super().increase_set(element_list, set_list[:len(set_list)-2], time_diff, server, time_index)
        server_total = 0

        total_set = set_list[len(set_list)-1]
        total_res = self.redis.zrangebyscore(total_set, time_diff, "inf")

        for i in range(0, len(total_res)):
            server_total += int(total_res[i])

        self.redis.zremrangebyscore(total_set, "-inf", time_diff)
        self.total[server][time_index] = server_total


    def format_redis_data(l: list, total: int)->list:
        return list(map(lambda x: (x[0].decode("utf-8"), int(x[1]), (x[1]/total) if total > 0 else 0), l))

    def server_list(self, server: str, time: int):
        return [self.name + ":{}_{}".format(server, time), self.name + ":global_{}".format(time),
                self.name + ":total_{}_{}".format(server, time)]

    def order_data(self,  item: Mapping[str, int]):
        pass

    def format_data(self, l: list, total: int) -> list:
        return list(map(lambda x: (x[0].decode("utf-8"), int(x[1]), (x[1]/total) if total > 0 else 0), l))

    def get_old_data(self, historic_set, time_diff):
        script = """local old_jsons = redis.call('zrangebyscore', KEYS[1], '-inf' , ARGV[1]);
                        redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1]) ;
                        return old_jsons;"""

        get_json = self.redis.register_script(script)
        jsons = get_json(keys=[historic_set], args=[time_diff])

        return jsons

    def parse_old_data(self, old_data):
        return json.loads(old_data.decode("utf-8"))

    def get_top_data(self, server, time):
        return self.redis.zrevrange(self.redis_server_set(server, time), 0, 4, withscores=True)

    def redis_server_set(self, server, time):
        return self.name + ":{}_{}".format(server, time)


class TopKEventProcessor(TopCountEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        topk_data = {'channel': "topk", "redis_set": "topk"}
        super().__init__(r, config, topk_data)

    def order_data(self, item: Mapping[str, int]) -> list:
        ordered_data = []
        total = 0

        for name_counter in item:
            ordered_data.append(({'name': name_counter}, item[name_counter]))
            total += item[name_counter]

        ordered_data = sorted(ordered_data, key=lambda x: x[1])
        return [ordered_data, total]


class MalformedPacketsEventProcessor(TopCountEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        malformed_data = {'channel': "QueriesWithUnderscoredName", "redis_set": "malformed"}
        super().__init__(r, config, malformed_data)

    def order_data(self,  item: Mapping[str, int]):
        ordered_data = {}

        for name_counter in item:
            if name_counter in ordered_data:
                number = ordered_data[name_counter][1] + 1
                ordered_data[name_counter] = ({'name': name_counter, 'data': item[name_counter]}, number)
            else:
                ordered_data[name_counter] = ({'name': name_counter, 'data': item[name_counter]}, 1)

        data_list = list(ordered_data.values())
        data_list = sorted(data_list, key=lambda x: x[1])

        return [data_list, len(item)]









