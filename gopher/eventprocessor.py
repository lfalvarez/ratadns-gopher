import queue
from typing import Mapping, Tuple, Any, Sequence

import redis
import threading
import json

import datetime
import time as Time

import hashlib
import uuid


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


def hex2ip(hex_ip: str) -> str:
    if ":" in hex_ip: # Assume IPV6
        return hex_ip

    return ".".join([str(int(hex_ip[i:i+2],16)) for i in range(0,8,2)])


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

    # Static variable
    qtypes_dict = {'32769': 'DLV', '32768': 'TA', '56': 'NINFO', '51': 'NSEC3PARAM', '45': 'IPSECKEY', '43': 'DS',
                   '60': 'CDNSKEY', '61': 'CDNSKEY', '62': 'CSYNC', '49': 'DHCID', '252': 'AXFR', '253': 'MAILB',
                   '250': 'TSIG', '251': 'IXFR', '256': 'URI', '257': 'CAA', '254': 'MAILA', '255': '*', '24': 'SIG',
                   '25': 'KEY', '26': 'PX', '27': 'GPOS', '20': 'ISDN', '21': 'RT', '22': 'NSAP', '23': 'NSAP-PTR',
                   '46': 'RRSIG', '249': 'TKEY', '44': 'SSHFP', '48': 'DNSKEY', '42': 'APL', '29': 'LOC', '40': 'SINK',
                   '41': 'OPT', '1': 'A', '3': 'MD', '2': 'NS', '5': 'CNAME', '4': 'MF', '7': 'MB', '6': 'SOA',
                   '9': 'MR', '8': 'MG', '59': 'CDS', '52': 'TLSA', '28': 'AAAA', '13': 'HINFO', '99': 'SPF',
                   '47': 'NSEC', '108': 'EUI48', '38': 'A6', '17': 'RP', '102': 'GID', '103': 'UNSPEC', '100': 'UINFO',
                   '101': 'UID', '106': 'L64', '107': 'LP', '104': 'NID', '105': 'L32', '11': 'WKS', '10': 'NULL',
                   '39': 'DNAME', '12': 'PTR', '15': 'MX', '58': 'TALINK', '14': 'MINFO', '16': 'TXT', '33': 'SRV',
                   '32': 'NIMLOC', '31': 'EID', '30': 'NXT', '37': 'CERT', '36': 'KX', '35': 'NAPTR', '34': 'ATMA',
                   '19': 'X25', '55': 'HIP', '109': 'EUI64', '18': 'AFSDB', '57': 'RKEY', '50': 'NSEC3'}

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

            fievel_windows = self.moving_window.get_items_after_limit(current_timestamp - time_span * 60 * 1000)
            for fievel_window in fievel_windows:
                server_id = fievel_window["serverId"]
                window_data = fievel_window["data"]

                if server_id not in accumulator:
                    accumulator[server_id] = {}

                server_accumulator = accumulator[server_id]

                for queries_by_ip in window_data:
                    ip = queries_by_ip["ip"]
                    queries = queries_by_ip["queries"]

                    if ip not in server_accumulator:
                        server_accumulator[ip] = {"queries_count": 0, "queries": {}}

                    for qtype_dec, qnames in queries.items():
                        qtype = self.qtypes_dict[qtype_dec]
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

            for server_id, server_accumulator in accumulator.items():
                server_result = {
                    "server_id": server_id,
                    "clients_data": [{
                                         "ip": hex2ip(ip),
                                         "queries": [{"qtype": qtype, "count": count}
                                                     for qtype, count in ip_data["queries"].items()],
                                         "queries_count": ip_data["queries_count"],
                                         "percentage": 100 * ip_data["queries_count"] / total_queries
                                     } for ip, ip_data in server_accumulator.items()]
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

            fievel_windows = self.moving_window.get_items_after_limit(current_timestamp - time_span * 60 * 1000)
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


class ServerDataV2EventProcessor(WindowedEventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r, config)
        self.subscribe("QueriesPerSecond")
        self.subscribe("AnswersPerSecond")
        self.server_data_config = config["server_data"]
        self.time_spans = self.server_data_config["times"]
        self.salt = uuid.uuid4().hex.encode()  # TODO: Change salt diary

    def merge_data(self, current_timestamp: float):
        result = []

        for time_span in self.time_spans:
            total_qps = 0
            total_aps = 0
            accumulator = {}

            fievel_windows = self.moving_window.get_items_after_limit(current_timestamp - time_span * 60 * 1000)
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
                total_qps += qps_avg
                total_aps += aps_avg
                server_result = {
                    "server_id": hashlib.md5(self.salt + server_id.encode()).hexdigest(),
                    "queries_per_second": qps_avg,
                    "answers_per_second": aps_avg
                }
                time_span_result["servers_data"].append(server_result)

            time_span_result["servers_data"].insert(0,{
                "server_id": "total",
                "queries_per_second": total_qps,
                "answers_per_second": total_aps});
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
            total_queries = 0

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
                    total_queries += qname_count

            time_span_result = {
                "time_span": time_span,
                "servers_data": []
            }

            for server_id, server_accumulator in accumulator.items():
                server_result = {
                    "server_id": server_id,
                    "top_qnames": [{
                                       "qname": qname,
                                       "qname_count": qname_count,
                                       "percentage": 100 * qname_count / total_queries
                                   } for qname, qname_count in server_accumulator.items()],
                }
                time_span_result["servers_data"].append(server_result)

            result.append(time_span_result)

        return result
