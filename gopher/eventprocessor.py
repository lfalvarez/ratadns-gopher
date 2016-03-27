import queue
from typing import Mapping, Tuple

import redis
import threading
import json

import socket
import struct
import urllib.request


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

    def process(self, item: Mapping[str, any]) -> Tuple[bool, any]:
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
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, any]):
        super().__init__(r)
        self.subscribe("QueriesSummary")
        self.config = config

    def process(self, item: Mapping[str, any]) -> Tuple[bool, any]:
        for summary_entry in item['data']:
            ip = hex_to_ip(summary_entry['ip'])

            if ip == None:
                continue

            summary_entry['ip'] = ip
            url = "http://" + self.config['freegeoip']['address'] + ":" + self.config['freegeoip']['port'] + \
                  "/json/" + ip
            with urllib.request.urlopen(url) as freegeoip_server:
                location = json.loads(str(freegeoip_server.read(), "utf-8"))
                summary_entry['location'] = location

        return (True, item)


class EventConsumer(object):
    def __init__(self):
        self.queue = queue.Queue()

    def consume(self, data):
        self.queue.put(data)

    def get_data(self):
        return self.queue.get()
