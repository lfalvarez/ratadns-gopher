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

    def register_consumer(self, event_consumer):
        self.consumers.append(event_consumer)

    def unregister_consumer(self, event_consumer):
        self.consumers.remove(event_consumer)

    def subscribe(self, channel):
        self.pubsub.subscribe(channel)

    def run(self):
        for serialized_item in self.pubsub.listen():
            item = json.loads(str(serialized_item['data'], "utf-8"))
            processed_item = self.process(item)
            for consumer in self.consumers:
                consumer.consume(processed_item)

    def process(self, item):
        pass


class ServerDataEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis):
        super().__init__(r)
        self.subscribe("QueriesPerSecond")
        self.subscribe("AnswersPerSecond")

    def process(self, item):
        return item

def hex_to_ip(ip_hex: str):
    if len(ip_hex) == 8 or len(ip_hex) == 7:
        ip = int(ip_hex, 16)
        return socket.inet_ntoa(struct.pack(">L", ip))
    else: # IPV6 Not supported! (yet)
        return None # Refactor this!


class QueriesSummaryEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis):
        super().__init__(r)
        self.subscribe("QueriesSummary")

    def process(self, item):
        for summaryEntry in item['data']:
            ip = hex_to_ip(summaryEntry['ip'])

            if ip == None:
                continue

            summaryEntry['ip'] = ip
            with urllib.request.urlopen("http://172.17.66.212:8080/json/" + ip) as geoServer:
                location = json.loads(str(geoServer.read(), "utf-8"))
                summaryEntry['location'] = location

        return item


class EventConsumer(object):
    def __init__(self):
        self.cond = threading.Condition()
        self.data = None

    def consume(self, data):
        self.cond.acquire()
        self.data = data
        self.cond.notify()
        self.cond.release()

    def get_data(self):
        self.cond.acquire()
        while self.data == None:
            self.cond.wait()
        data = self.data
        self.data = None
        self.cond.release()
        return data



