import queue
from typing import Mapping, Tuple, Any

import redis
import threading
import json

import socket
import struct
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


class WindowAlgorithmEventProcessor(EventProcessor):
    def __init__(self, name: str, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.config = config
        self.name = name
        self.redis = r
        self.total = {}

        n = len(self.config[self.name]['times'])
        for server in self.config['servers']:
            self.total[server['name']] = [0]*n

    def order_data(self,  item: Mapping[str, int]) -> list:
        pass

    def timespan_set(self, server, time):
        pass

    def increase_set(self, element_list: Mapping[str, int], set_list: list, current_time: int, server: str, time: int,
                     time_index: int):
        time_diff = current_time - time*1000.0
        multi = self.redis.pipeline()

        for element in element_list:
            for set in set_list:
                multi.zincrby(set, element[0]['name'], element[1])

        multi.execute()

        self.increase_total(set_list[len(set_list)-1], time_diff, server, time_index)

    def increase_total(self, total_set, time_diff, server, time_index):
        server_total = 0
        total_res = self.redis.zrangebyscore(total_set, time_diff, "inf")

        for i in range(0, len(total_res)):
            server_total += int(total_res[i])

        self.redis.zremrangebyscore(total_set, "-inf", time_diff)
        self.total[server][time_index] = server_total

    def increase_timespan(self, current_time, timestamp_set, data, total):
        self.redis.zadd(timestamp_set[0], current_time, json.dumps(data)[0])
        self.redis.zadd(timestamp_set[1], current_time, total)

    def get_top(self, time_index, time, server_name):
        time_data = {}
        servers_total = 0

        for server in self.config['servers']:
            servers_total += self.total[server['name']][time_index]
            time_data[server['name']] = self.format_data(self.get_top_data(server['name'], time), self.total[server['name']][time_index])

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

            self.increase_timespan(now, self.timespan_set(server_name, time), ordered_data[0], ordered_data[1])

            self.increase_set(ordered_data[0], self.server_list(server_name, time), now, server_name, time, time_index)
            self.cleanup_old_data(self.server_list(server_name, time), self.timespan_set(server_name, time), now-time*1000.0)

            data[time] = self.get_top(time_index, time, server_name)

        item['data'] = json.dumps(data)
        return (True,item)


class QueriesSummaryEventProcessor(WindowAlgorithmEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__('summary', r, config)
        self.subscribe("QueriesSummary")

    def server_list(self, server: str, time: int):
        return ["summary:ip_{}_{}".format(server, time), "summary:ip_size_{}_{}".format(server, time),
                "summary:historic_{}_{}".format(server, time), self.name + ":total_{}_{}".format(server, time)]

    def timespan_set(self, server, time):
        return ["summary:ip_{}_{}".format(server, time), "summary:total_{}_{}".format(server, time)]

    def order_data(self,  item: Mapping[str, int]):
        total = 0
        for ip in item:
            queries = ip['queries']
            for type in queries:
                total += len(queries[type])

        return [item, total]


    def increase_timespan(self, current_time, timestamp_set, item, total):
        for element in item:
            super().increase_timespan(current_time, timestamp_set, element['ip'], total)

    def increase_set(self, element_list: Mapping[str, int], historic_set: list, current_time: int, server: str, time: int,
                     time_index: int):
        time_diff = current_time - time*1000.0
        super().increase_total(historic_set[len(historic_set)-1], time_diff, server, time_index)
        for element in element_list:
            ip = element['ip']
            queries_list = element['queries']
            old_json = self.redis.hget("summary:historic_{}_{}".format(server, time), ip)
            total = 0

            if old_json is not None:
                old_queries = json.loads(old_json.decode("utf-8"))
                for i in range(0, len(old_queries)):
                    query = old_queries[i]
                    if query[1] < time_diff:
                        old_queries = old_queries[0:i]
                        break
                    else:
                        total += len(query[0])
                old_queries.insert(0, (queries_list, current_time))

                self.redis.hset("summary:historic_{}_{}".format(server, time), ip, json.dumps(old_queries))
                self.redis.zadd("summary:ip_size_{}_{}".format(server, time), total, ip)
            else:
                new_queries = [(queries_list, current_time)]
                total = len(queries_list)

                self.redis.hset("summary:historic_{}_{}".format(server, time), ip, json.dumps(new_queries))
                self.redis.zadd("summary:ip_size_{}_{}".format(server, time), total, ip)

    def cleanup_old_data(self, set_list, timespan_set, time_diff):
        old_elements = self.redis.zrangebyscore(timespan_set, "-inf", time_diff)

        for element in old_elements:
            self.redis.zrem(set_list[0], element)
            self.redis.zrem(set_list[1], element)
            self.redis.hdel(set_list[2], element)

    def get_top_data(self, server: str, time: int):
        top_elements = self.redis.zrevrange("summary:ip_size_{}_{}".format(server, time), 0, 4, withscores=True)
        top_list = []

        for element in top_elements:
            ip = element[0].decode("utf-8")
            queries = json.loads(self.redis.hget("summary:historic_{}_{}".format(server, time), ip).decode("utf-8"))
            top_list.append([ip, [x[0] for x in queries], element[1]])

        return top_list

    def format_data(self, l: list, total: int) -> list:
        return list(map(lambda x: (x[0], x[1], int(x[2]), (x[2]/total) if total > 0 else 0), l))


class TopCountEventProcessor(WindowAlgorithmEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any], config_data: Mapping[str, Any]):
        super().__init__(config_data['redis_set'], r, config)
        self.subscribe(config_data['channel'])
        self.redis = r
        self.name = config_data['redis_set']

    def timespan_set(self, server, time):
        return [self.name + ":historic_jsons_{}_{}".format(server, time), self.name + ":total_{}_{}".format(server, time)]

    def increase_set(self, element_list: Mapping[str, int], set_list: list, current_time: int, server: str, time: int,
                     time_index: int):
        super().increase_set(element_list, set_list[:len(set_list)], current_time, server, time, time_index)

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









