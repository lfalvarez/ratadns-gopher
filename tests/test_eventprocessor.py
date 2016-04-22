from gopher.eventprocessor import EventConsumer, hex_to_ip, ServerDataEventProcessor
import random, threading, redis, redis.client
from unittest.mock import Mock, patch
import unittest

class TestEventProcessor(unittest.TestCase):
    def test1(self):
        ec = EventConsumer()
        input = random.random()
        ec.consume(input)
        output = ec.get_data()
        self.assertEqual(input, output)

class TestHexToIP(unittest.TestCase):
    def test1(self):
        ip_hex = "08080808"
        expected_ip = "8.8.8.8"
        ip = hex_to_ip(ip_hex)
        self.assertEqual(expected_ip, ip)

        ip_hex = "C0A80001"
        expected_ip = "192.168.0.1"
        ip = hex_to_ip(ip_hex)
        self.assertEqual(expected_ip, ip)

        ip_hex = "a0a0a0a"
        expected_ip = "10.10.10.10"
        ip = hex_to_ip(ip_hex)
        self.assertEqual(expected_ip, ip)

    def test2(self):
        self.assertIsNone(hex_to_ip("a"))
        self.assertIsNone(hex_to_ip("bababa"))


class TestServerDataEventProcessor(unittest.TestCase):
    def test1(self):
        # TODO: try to mock the result r.pubsub() in order to test that we are subscribing to the correct channels
        pass
