import queue
from typing import Mapping, Tuple, Any, Sequence, TypeVar

import redis
import threading
import json

import datetime
import time as Time


class EventConsumer(object):
    """
    # What's the objective of this class?
    """
    def __init__(self):
        self.queue = queue.Queue()

    def consume(self, data):
        self.queue.put(data)

    def get_data(self):
        return self.queue.get()


class EventProcessor(threading.Thread):
    """
    One event processor for each URL
    """
    def __init__(self, r: redis.StrictRedis):
        threading.Thread.__init__(self)
        self.pubsub = r.pubsub(ignore_subscribe_messages=True)
        self.consumers = []

    def register_consumer(self, event_consumer: EventConsumer):
        self.consumers.append(event_consumer)

    def deregister_consumer(self, event_consumer: EventConsumer):
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
    """
    Process of QueriesPerSecond and AnswersPerSecond events (which doesn't need processing in Gopher)
    """
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.subscribe("QueriesPerSecond")
        self.subscribe("AnswersPerSecond")

    def process(self, item):
        return True, item

T = TypeVar('T')
class QueriesSummaryWithoutRedisEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.subscribe("QueriesSummary")
        self.config = config

        # Mapping[str, Sequence[Tuple[Mapping, int]]
        # Dictionary which maps an IP with a list of <queries, timestamp> tuples.
        self.queries_by_client_dict = {}

    def add_data_to_dict(self, data: Sequence[Mapping[T, Any]], key: T, current_timestamp: float):
        for queries_by_key in data:
            query_key = queries_by_key[key]
            current_queries = (queries_by_key, current_timestamp)
            if query_key in self.queries_by_client_dict:
                self.queries_by_client_dict[query_key].append(current_queries)
            else:
                self.queries_by_client_dict[query_key] = [current_queries]

    def remove_old_data_from_dict(self, previous_time: float):
        keys_to_remove = []
        for key in self.queries_by_client_dict.keys():
            queries_to_remove = 0
            for query, timestamp in self.queries_by_client_dict[key]:
                if timestamp > previous_time:
                    break
                queries_to_remove += 1
            if queries_to_remove > 0:
                keys_to_remove.append((key, queries_to_remove))

        for key, queries_to_remove in keys_to_remove:
            if len(self.queries_by_client_dict[key]) == queries_to_remove:
                del self.queries_by_client_dict[key]
            else:
                self.queries_by_client_dict[key] = self.queries_by_client_dict[key][queries_to_remove:]

    @staticmethod
    def timestamp_in_time_span(timestamp: float, now: float, time_span: float) -> bool:
        return timestamp > now - time_span*60*1000

    def merge_data_from_dict(self, time_spans: Sequence[float], current_timestamp: float) -> Sequence[Mapping[str, Any]]:
        result = []
        for time_span in time_spans:
            time_span_result = []
            total_queries = 0

            for ip, queries_by_ip in self.queries_by_client_dict.items():
                merged_queries = {}
                ip_total_queries = 0
                time_span_queries_by_ip = [q for q, ts in queries_by_ip
                                           if self.timestamp_in_time_span(ts, current_timestamp, time_span)]

                for queries_and_ip in time_span_queries_by_ip:
                    queries = queries_and_ip['queries']
                    for qtype, qnames in queries.items():
                        qnames_length = len(qnames)
                        if qtype in merged_queries:
                            merged_queries[qtype] += qnames_length  # Concatenate each queries lists
                        else:
                            merged_queries[qtype] = qnames_length
                        ip_total_queries += qnames_length
                        total_queries += qnames_length

                if ip_total_queries > 0:
                    time_span_result.append({"ip": ip, "queries": merged_queries, "queries_count": ip_total_queries})

            for queries_by_ip in time_span_result:
                queries_by_ip["percentage"] = (queries_by_ip["queries_count"] / total_queries) * 100

            result.append({"time_span": time_span, "merged_data": time_span_result})

        return result

    def get_top_k_by_key(self, d, k, key) -> Sequence[Mapping[str, Any]]:
        top_k_queries = d["merged_data"][:k]
        top_k_queries.sort(key=lambda query: query[key], reverse=True)
        for query in d["merged_data"][k:]:
            i = 0
            while i < k and query[key] > top_k_queries[-1 * (i + 1)][key]:
                i += 1
            if i > 0:
                top_k_queries.insert(k - i, query)
                del top_k_queries[-1]

        return {"time_span": d["time_span"], "data": top_k_queries}

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        data = item['data']
        current_timestamp = Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

        # Add data to Dictionary
        self.add_data_to_dict(data, "ip", current_timestamp)

        # Remove old data from Dictionary from window time
        # It will be stored the max quantity of data according to window times
        max_window_time = max(self.config["summary"]["times"]) * 60
        current_window_start_timestamp = current_timestamp - max_window_time * 1000.0
        self.remove_old_data_from_dict(current_window_start_timestamp)

        # Merge data (leave all the queries made by an ip together by type)
        merged_data = self.merge_data_from_dict(self.config["summary"]["times"], current_timestamp)

        # Get TopK queries_count ip's
        k = self.config["summary"]["output_limit"]

        top_k_queries = list(map(lambda o: self.get_top_k_by_key(o, k, "queries_count"), merged_data))
        return True, top_k_queries


class WindowAlgorithmEventProcessor(EventProcessor):
    """
    Abstract representation of the algorithm that accumulates data on a certain period of time (timespan)
    and delete part of them as the time goes on (basically, a time window)
    """
    def __init__(self, name: str, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.config = config
        self.name = name
        self.redis = r
        self.total = {}

        n = len(self.config[self.name]['times'])
        for server in self.config['servers_info']:
            self.total[server['name']] = [0] * n  # What does here? What's the purpose?

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:  # What means each parameter?
        """
        Receives a json with data and accumulates it on a time window
        """
        data = {}
        ordered_data = self.order_data(item['data'])
        server_name = item['serverId']
        now = Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

        for time_index in range(0, len(self.config[self.name]['times'])):
            time = self.config[self.name]['times'][time_index] * 60

            self.increase_timespan(now, self.timespan_set(server_name, time), ordered_data[0], ordered_data[1])

            self.increase_set(ordered_data[0], self.server_list(server_name, time), now, server_name, time, time_index)
            self.cleanup_old_data(self.server_list(server_name, time), self.timespan_set(server_name, time),
                                  now - time * 1000.0)

            data[time] = self.get_top(time_index, time)

        item['data'] = data
        return True, item

    def increase_set(self, element_list: Mapping[str, int], set_list: list, current_time: int, server: str, time: int,
                     time_index: int):  # What means each parameter?
        """
        Takes a list of elements and updates all relevant redis sets with this values
        """
        time_diff = current_time - time * 1000.0  # What means time*1000?
        multi = self.redis.pipeline()

        for element in element_list:
            for set in set_list:
                multi.zincrby(set, element[0]['name'], element[1])  # Add all elements in all sets?

        multi.execute()
        self.increase_total(set_list[len(set_list) - 1], time_diff, server, time_index)

    def increase_total(self, total_set, time_diff, server, time_index):  # What means each parameter?
        """
        Takes the total number of elements received in this json and inserts that value the total redis set
        """
        server_total = 0
        total_res = self.redis.zrangebyscore(total_set, time_diff, "inf")

        for i in range(0, len(total_res)):
            server_total += int(total_res[i])

        self.redis.zremrangebyscore(total_set, "-inf", time_diff)
        self.total[server][time_index] = server_total

    def increase_timespan(self, current_time, timestamp_set, data, total):  # What means each parameter?
        """
        Takes some data and saves it in a redis set, with current time (in millis) as score
        """
        # TODO: Fix bug: json.dumps(data)[0] is a ' " '. Should be the IP (just data?)
        self.redis.zadd(timestamp_set[0], current_time, json.dumps(data)[0])
        self.redis.zadd(timestamp_set[1], current_time, total)

    def get_top(self, time_index, time):  # What means each parameter?
        """
        Get top values in this window of time and formats it to be sent, then returns it as a dictionary
        """
        time_data = {}
        servers_total = 0

        for server in self.config['servers_info']:
            servers_total += self.total[server['name']][time_index]
            time_data[server['name']] = self.format_data(self.get_top_data(server['name'], time),
                                                         self.total[server['name']][time_index])

        return time_data

    def cleanup_old_data(self, set_list, timespan_set, time_diff):  # What means each parameter?
        """
        Deletes from redis all data that is outside the time window, updating information as necessary
        """
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

    def order_data(self, item: Mapping[str, int]) -> list:
        pass

    def timespan_set(self, server, time):
        pass

    def format_data(self):
        pass

    def server_list(self, server: str, time: int) -> list:
        pass

    def get_top_data(self, server: str, time: int):
        pass

    def get_old_data(self, historic_set, time_diff):
        pass

    def parse_old_data(self, old_data):
        pass


class QueriesSummaryEventProcessor(WindowAlgorithmEventProcessor):
    """
    Receives queries summary information and processes it using the time window algorithm
    """
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__('summary', r, config)
        self.subscribe("QueriesSummary")

    def increase_set(self, element_list: Mapping[str, int], historic_set: list, current_time: int, server: str,
                     time: int,
                     time_index: int):  # What means each parameter?
        """
        Takes the queries summary information received and saves every (queries, timestamp) pair in relation to their
        ip. If the ip is already in the set, first all queries outside the window are erased (since they are ordered
        in decreasing timestamp order, all queries after the first outside the window will be outside also) and then
        the new pair is inserted in first place
        """
        # TODO: refactor names!
        # TODO: add new_queries length to total
        time_diff = current_time - time * 1000.0
        super().increase_total(historic_set[len(historic_set) - 1], time_diff, server, time_index)
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
                        for type in query[0]:
                            total += len(query[0][type])

                old_queries.insert(0, (queries_list, current_time))

                self.redis.hset("summary:historic_{}_{}".format(server, time), ip, json.dumps(old_queries))
                self.redis.zadd("summary:ip_size_{}_{}".format(server, time), total, ip)
            else:
                new_queries = [(queries_list, current_time)]
                for type in queries_list:
                    total += len(queries_list[type])

                self.redis.hset("summary:historic_{}_{}".format(server, time), ip, json.dumps(new_queries))
                self.redis.zadd("summary:ip_size_{}_{}".format(server, time), total, ip)

    def increase_timespan(self, current_time, timestamp_set, item, total):  # What means each parameter?
        """
        For every sender ip, saves the queries data and current time
        """
        for element in item:
            super().increase_timespan(current_time, timestamp_set, element['ip'], total)

    def get_top_data(self, server: str, time: int):  # What means each parameter?
        """
        Gets ips with more queries in this time window, obtains related information from redis, formats data and
        returns it
        """
        # TODO: refactor set names
        top_elements = self.redis.zrevrange("summary:ip_size_{}_{}".format(server, time), 0, 4, withscores=True)
        top_list = []

        for element in top_elements:
            ip = element[0].decode("utf-8")
            queries = json.loads(self.redis.hget("summary:historic_{}_{}".format(server, time), ip).decode("utf-8"))
            top_list.append((ip, self.collapse_by_type(queries), element[1]))

        return top_list

    def cleanup_old_data(self, set_list, timespan_set, time_diff):  # What means each parameter?
        """
        Gets all ips outside the time window and erases them from all sets
        """
        old_elements = self.redis.zrangebyscore(timespan_set, "-inf", time_diff)

        for element in old_elements:
            self.redis.zrem(set_list[0], element)
            self.redis.zrem(set_list[1], element)
            self.redis.hdel(set_list[2], element)

    def order_data(self, item: Mapping[str, int]):  # What means each parameter?
        """
        Takes the information received and calculates the total number of queries that the summary represents
        """
        total = 0

        for ip in item:
            queries = ip['queries']
            for type in queries:
                total += len(queries[type])

        return [item, total]

    def timespan_set(self, server, time):  # What means each parameter?
        """
        Redis set names associated to timestamp (so that old information can be erased accordingly)
        """
        return ["summary:ip_{}_{}".format(server, time), "summary:total_{}_{}".format(server, time)]

    def format_data(self, l: list, total: int) -> list:  # What means each parameter?
        """
        Takes the list containing top information, and each pair is converted to a tuple containing:
        (ip, queries, total number of queries, percentage of queries respect all queries in the window)
        """
        return list(map(lambda x: (x[0], x[1], int(x[2]), (x[2] / total) if total > 0 else 0), l))

    def server_list(self, server: str, time: int):  # What means each parameter?
        """
        Redis set names list associated to this type of information
        """
        return ["summary:ip_{}_{}".format(server, time), "summary:ip_size_{}_{}".format(server, time),
                "summary:historic_{}_{}".format(server, time), self.name + ":total_{}_{}".format(server, time)]

    @staticmethod
    def collapse_by_type(queries):  # What means each parameter?
        """
        Takes a list of queries, timestamp pairs and returns a new dictionary with queries grouped by type
        """
        queries_by_type = {}
        for element in queries:
            query = element[0]
            for type in query:
                queries_by_type.setdefault(type, [])
                queries_by_type[type] = queries_by_type[type] + query[type]

        return queries_by_type


class TopCountEventProcessor(WindowAlgorithmEventProcessor):
    """
    # What's the objective of this class?
    """
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any], config_data: Mapping[str, Any]):
        super().__init__(config_data['redis_set'], r, config)
        self.subscribe(config_data['channel'])
        self.redis = r
        self.name = config_data['redis_set']

    def increase_set(self, element_list: Mapping[str, int], set_list: list, current_time: int, server: str, time: int,
                     time_index: int):  # What means each parameter?
        """
        Takes a list of elements and updates all relevant redis sets with this values
        """
        super().increase_set(element_list, set_list[:len(set_list)], current_time, server, time, time_index)

    def get_top_data(self, server, time):  # What means each parameter?
        """
        Returns the names with higher score in this time window
        """
        return self.redis.zrevrange(self.redis_server_set(server, time), 0, 4, withscores=True)

    def order_data(self, item: Mapping[str, int]):  # What means each parameter?
        pass

    def timespan_set(self, server, time):  # What means each parameter?
        """
        Redis set names associated to timestamp (so that old information can be erased accordingly)
        """
        return [self.name + ":historic_jsons_{}_{}".format(server, time),
                self.name + ":total_{}_{}".format(server, time)]

    def format_data(self, l: list, total: int) -> list:  # What means each parameter?
        """
        Takes the list containing top information, and each pair is converted to a tuple containing:
        (name, total number of queries, percentage of queries respect all queries in the window)
        """
        return list(map(lambda x: (x[0].decode("utf-8"), int(x[1]), (x[1] / total) if total > 0 else 0), l))

    def server_list(self, server: str, time: int):  # What means each parameter?
        """
        Redis set names list associated to this type of information
        """
        return [self.name + ":{}_{}".format(server, time), self.name + ":global_{}".format(time),
                self.name + ":total_{}_{}".format(server, time)]

    def get_old_data(self, historic_set, time_diff):  # What means each parameter?
        """
        Returns data outside the time window and remove it from the set
        """
        jsons = self.redis.zrangebyscore(historic_set, "-inf", time_diff)
        self.redis.zremrangebyscore(historic_set, "-inf", time_diff)

        return jsons

    def parse_old_data(self, old_data):  # What means each parameter?
        """
        Parses data to json in utf-8 encoding
        """
        return json.loads(old_data.decode("utf-8"))

    def redis_server_set(self, server, time):  # What means each parameter?
        """
        Returns the redis set name that keeps queries count
        """
        return self.name + ":{}_{}".format(server, time)


class TopKEventProcessor(TopCountEventProcessor):
    """
    # What's the objective of this class?
    """
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        topk_data = {'channel': "topk", "redis_set": "topk"}
        super().__init__(r, config, topk_data)

    def order_data(self, item: Mapping[str, int]) -> list:  # What means each parameter?
        """
        Receives a list of queries, orders the list and calculates how many queries are in the list
        """
        ordered_data = []
        total = 0

        for name_counter in item:
            ordered_data.append(({'name': name_counter}, item[name_counter]))
            total += item[name_counter]

        ordered_data = sorted(ordered_data, key=lambda x: x[1])
        return [ordered_data, total]


class MalformedPacketsEventProcessor(TopCountEventProcessor):
    """
    # What's the objective of this class?
    """
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        malformed_data = {'channel': "QueriesWithUnderscoredName", "redis_set": "malformed"}
        super().__init__(r, config, malformed_data)

    def order_data(self, item: Mapping[str, int]):  # What means each parameter?
        """
        Receives a list of malformed queries, orders the list and calculates how many queries are in the list
        """
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
