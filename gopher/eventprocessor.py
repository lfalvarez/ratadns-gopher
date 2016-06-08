import queue
from typing import Mapping, Tuple, Any, Sequence

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


class MovingWindow(object):
    def __init__(self):
        self.item_timestamp_list = []

    def add_item(self, item: Any, now_timestamp: float):
        self.item_timestamp_list.append((item, now_timestamp))

    def remove_old_data(self, lower_timestamp_limit):
        items_to_remove = 0
        for item, ts in self.item_timestamp_list:
            if ts > lower_timestamp_limit:
                break
            items_to_remove += 1
        del self.item_timestamp_list[:items_to_remove]

    def get_items_after_limit(self, timestamp_limit: float):
        return [item for item, ts in self.item_timestamp_list if ts > timestamp_limit]


def get_topk(l: Sequence[Any], k: int, key):
    top_k_items = l[:k]
    top_k_items.sort(key=key, reverse=True)
    for item in l[k:]:
        i = 0
        while i < k and key(item) > key(top_k_items[-1 * (i + 1)]):
            i += 1
        if i > 0:
            top_k_items.insert(k - i, item)
            del top_k_items[-1]

    return top_k_items


class WindowedEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.moving_window = MovingWindow()
        self.time_spans = None

    def merge_data(self, current_timestamp: float) -> Sequence[Mapping]:
        pass

    def select_item(self, data):
        return data

    def filter_item(self, data):
        return True

    def process(self, item: Mapping[str, Any]):
        current_timestamp = Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

        # Add data to Dictionary
        self.moving_window.add_item(item, current_timestamp)

        # Remove old data from Dictionary from window time
        # It will be stored the max quantity of data according to window times
        max_window_time = max(self.time_spans) * 60
        current_window_start_timestamp = current_timestamp - max_window_time * 1000.0
        self.moving_window.remove_old_data(current_window_start_timestamp)

        # Merge data (leave all the queries made by an ip together by type)
        merged_data = self.merge_data(current_timestamp)

        selected_items = [self.select_item(item) for item in merged_data if self.filter_item(item)]

        return True, selected_items


class QueriesSummaryEventProcessor(WindowedEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r, config)
        self.subscribe("QueriesSummary")
        self.summary_config = config["summary"]
        self.time_spans = self.summary_config["times"]

    def select_item(self, data):
        # Get TopK queries_count ip's
        k = self.summary_config["output_limit"]

        def key_fn(ip_data):
            return ip_data["queries_count"]

        for server_data in data["servers_data"]:
            server_data["clients_data"] = get_topk(server_data["clients_data"], k, key_fn)

        return data

    def merge_data(self, current_timestamp: float):
        result = []
        for time_span in self.time_spans:
            accumulator = {}
            total_queries = 0

            fievel_windows = self.moving_window.get_items_after_limit(current_timestamp - time_span*60*1000)
            for fievel_window in fievel_windows:
                server_id = fievel_window["serverId"]
                window_data = fievel_window["data"]

                if server_id not in accumulator:
                    accumulator[server_id] = {}

                server_accumulator = accumulator[server_id]

                for queries_by_ip in window_data:
                    ip = queries_by_ip["ip"]
                    queries = queries_by_ip["queries"]

                    if ip not in accumulator:
                        server_accumulator[ip] = {"queries_count": 0, "queries": {}}

                    for qtype, qnames in queries.items():
                        if qtype in server_accumulator[ip]["queries"]:
                            server_accumulator[ip]["queries"][qtype] += len(qnames)
                        else:
                            server_accumulator[ip]["queries"][qtype] = len(qnames)

                        server_accumulator[ip]["queries_count"] += len(qnames)
                        total_queries += len(qnames)

            time_span_result = {
                "time_span": time_span,
                "servers_data": []
            }

            for server_id, server_data in accumulator.items():
                server_result = {
                    "server_id": server_id,
                    "clients_data": [{
                        "ip": ip,
                        "queries": ip_data["queries"],
                        "queries_count": ip_data["queries_count"],
                        "percentage": 100*ip_data["queries_count"]/total_queries
                    } for ip, ip_data in server_data.items()]
                }
                time_span_result["servers_data"].append(server_result)

            result.append(time_span_result)

        return result


class ServerDataEventProcessor(WindowedEventProcessor):
    """
    Process of QueriesPerSecond and AnswersPerSecond events (which doesn't need processing in Gopher)
    """
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r, config)
        self.subscribe("QueriesPerSecond")
        self.subscribe("AnswersPerSecond")
        self.server_data_config = config["server_data"]
        self.time_spans = self.server_data_config["times"]

    def merge_data(self, current_timestamp: float):
        result = []
        for time_span in self.time_spans:
            accumulator = {}

            fievel_windows = self.moving_window.get_items_after_limit(current_timestamp - time_span*60*1000)
            for fievel_window in fievel_windows:
                server_id = fievel_window["serverId"]
                window_type = fievel_window["type"]
                window_data = fievel_window["data"]

                if server_id not in accumulator:
                    accumulator[server_id] = {
                        "queries_per_second": [],
                        "answers_per_second": []
                    }

                server_accumulator = accumulator[server_id]
                if window_type == "QueriesPerSecond":
                    server_accumulator["queries_per_second"].append(window_data)
                elif window_type == "AnswersPerSecond":
                    server_accumulator["answers_per_second"].append(window_data)

            time_span_result = {
                "time_span": time_span,
                "servers_data": []
            }

            for server_id, server_accumulator in accumulator.items():
                qps = server_accumulator["queries_per_second"]
                aps = server_accumulator["answers_per_second"]
                qps_avg = sum(qps) / float(len(qps)) if len(qps) != 0 else 0
                aps_avg = sum(aps) / float(len(aps)) if len(aps) != 0 else 0
                server_result = {
                    "server_id": server_id,
                    "queries_per_second": qps_avg,
                    "answers_per_second": aps_avg
                }
                time_span_result["servers_data"].append(server_result)

            result.append(time_span_result)

        return result


class TopQNamesEventProcessor(WindowedEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r, config)
        self.subscribe("topk")
        self.top_qnames_config = config["top_qnames"]
        self.time_spans = self.top_qnames_config["times"]

    def select_item(self, data):
        k = self.top_qnames_config["output_limit"]

        def key_fn(qnames_data):
            return qnames_data["qname_count"]

        for server_data in data["servers_data"]:
            server_data["top_qnames"] = get_topk(server_data["top_qnames"], k, key_fn)

        return data

    def merge_data(self, current_timestamp: float):
        result = []
        for time_span in self.time_spans:
            accumulator = {}

            fievel_windows = self.moving_window.get_items_after_limit(current_timestamp - time_span * 60 * 1000)
            for fievel_window in fievel_windows:
                server_id = fievel_window["serverId"]
                window_data = fievel_window["data"]

                if server_id not in accumulator:
                    accumulator[server_id] = {}

                server_accumulator = accumulator[server_id]

                for qname, qname_count in window_data.items():
                    if qname not in server_accumulator:
                        server_accumulator[qname] = qname_count
                    else:
                        server_accumulator[qname] += qname_count

            time_span_result = {
                "time_span": time_span,
                "servers_data": []
            }

            for server_id, server_accumulator in accumulator.items():
                server_result = {
                    "server_id": server_id,
                    "top_qnames": [{
                        "qname": qname,
                        "qname_count": qname_count
                    } for qname, qname_count in server_accumulator.items()],
                }
                time_span_result["servers_data"].append(server_result)

            result.append(time_span_result)

        return result


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
            server_total += float(total_res[i])

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
