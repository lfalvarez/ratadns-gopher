import random
import redis
import redis.client
import unittest
from unittest.mock import MagicMock, call

from gopher.eventprocessor import EventConsumer, hex_to_ip, ServerDataEventProcessor


class TestEventProcessor(unittest.TestCase):
    def test1(self):
        ec = EventConsumer()
        input_value = random.random()
        ec.consume(input_value)
        output_value = ec.get_data()
        self.assertEqual(input_value, output_value)


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
    def setUp(self):
        self.redis_mock = MagicMock(spec=redis.StrictRedis)  # mock redis connection
        # noinspection PyTypeChecker
        self.ep = ServerDataEventProcessor(self.redis_mock)

    def test_correct_channels(self):
        calls = [call('QueriesPerSecond'), call('AnswersPerSecond')]
        self.redis_mock.pubsub().subscribe.assert_has_calls(calls)

    def test_process(self):
        a = random.random()
        publish, result = self.ep.process(a)
        self.assertTrue(publish)
        self.assertEqual(result, a)
