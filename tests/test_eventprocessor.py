import random
import unittest

import fakeredis
import json

from gopher.eventprocessor import \
    EventConsumer, \
    MovingWindow, \
    hex2ip, \
    get_topk, \
    DataSortedByQTypeEventProcessor, \
    ServerDataEventProcessor, \
    TopQNamesWithIPEventProcessor


class TestEventConsumer(unittest.TestCase):
    def test_get_data_after_consume(self):
        ec = EventConsumer()
        input_value = random.random()
        ec.consume(input_value)
        output_value = ec.get_data()
        self.assertEqual(input_value, output_value)


class TestEventProcessor(unittest.TestCase):
    pass


class TestMovingWindow(unittest.TestCase):
    def setUp(self):
        self.moving_window = MovingWindow()

    def test_add_10_items_and_get(self):
        test_list = [i for i in range(0, 10)]
        for i in test_list:
            self.moving_window.add_item(i, i)

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, test_list)

        items = self.moving_window.get_items_after_limit(4)
        self.assertListEqual(items, [i for i in range(5, 10)])

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, test_list)

    def test_add_10_items_remove_and_get(self):
        for i in range(0, 10):
            self.moving_window.add_item(i, i)
        self.moving_window.remove_old_data(4)

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, [i for i in range(5, 10)])


class TestGetTopK(unittest.TestCase):
    def setUp(self):
        self.simple_list = [1, 2, 3, 4, 5]
        self.identity_fn = lambda x: x
        self.item1 = {"arg1": 1, "arg2": 1}
        self.item2 = {"arg1": 2, "arg2": 3}
        self.item3 = {"arg1": 3, "arg2": 2}
        self.item4 = {"arg1": 4, "arg2": 4}
        self.item5 = {"arg1": 1, "arg2": 5}
        self.complex_list = [self.item1, self.item2, self.item3, self.item4, self.item5]

    def test_get_0_elements_simple(self):
        k = 0
        result_list = get_topk(self.simple_list, k, self.identity_fn)
        self.assertListEqual(result_list, [])

    def test_get_more_elements_than_size_simple(self):
        k = len(self.simple_list) + 1
        result_list = get_topk(self.simple_list, k, self.identity_fn)
        self.assertListEqual(result_list, sorted(self.simple_list, reverse=True))

    def test_get_3_elements_simple(self):
        k = 3
        result_list = get_topk(self.simple_list, k, self.identity_fn)
        self.assertListEqual(result_list, [5, 4, 3])

    def test_get_2_elements_complex(self):
        k = 2
        key_fn_2 = lambda x: x["arg2"]
        result_2_list = get_topk(self.complex_list, k, key_fn_2)
        self.assertListEqual(result_2_list, [self.item5, self.item4])

        key_fn_1 = lambda x: x["arg1"]
        result_1_list = get_topk(self.complex_list, k, key_fn_1)
        self.assertListEqual(result_1_list, [self.item4, self.item3])


class TestHex2IP(unittest.TestCase):
    def test_valid_ipv6(self):
        hex_ip = "2001:cdba::3257:9652"
        ip = hex2ip(hex_ip)
        self.assertEqual(ip, hex_ip)

    def test_valid_ipv4(self):
        hex_ip = "A41E6B0B"
        ip = hex2ip(hex_ip)
        self.assertEqual(ip, "164.30.107.11")


class TestWindowedEventProcessor(unittest.TestCase):
    """
    Abstract class, no testing necessary
    """
    pass


class TestDataSortedByQTypeEventProcessor(unittest.TestCase):
    def setUp(self):
        self.r = fakeredis.FakeStrictRedis()
        self.test_config = json.load(open("test_config.json"))
        self.qtype_ep = DataSortedByQTypeEventProcessor(self.r, self.test_config)

    def tearDown(self):
        self.r.flushall()

    def test_select_item(self):
        input_data = {"qtype_data": [
            {"qtype": 1, "queries": [{"ip": 'ip1', "queries_count": 1},
                                     {"ip": 'ip2', "queries_count": 5},
                                     {"ip": 'ip3', "queries_count": 10}]},
            {"qtype": 2, "queries": [{"ip": 'ip4', "queries_count": 1},
                                     {"ip": 'ip5', "queries_count": 5},
                                     {"ip": 'ip6', "queries_count": 10}]},
            {"qtype": 3, "queries": [{"ip": 'ip7', "queries_count": 1},
                                     {"ip": 'ip8', "queries_count": 5},
                                     {"ip": 'ip9', "queries_count": 10}]}
        ], "time_span": 1}

        result = self.qtype_ep.select_item(input_data)

        expected_output_data = {"qtype_data": [
            {"qtype": 1, "queries": [{"ip": 'ip3', "queries_count": 10},
                                     {"ip": 'ip2', "queries_count": 5}]},
            {"qtype": 2, "queries": [{"ip": 'ip6', "queries_count": 10},
                                     {"ip": 'ip5', "queries_count": 5}]},
            {"qtype": 3, "queries": [{"ip": 'ip9', "queries_count": 10},
                                     {"ip": 'ip8', "queries_count": 5}]}
        ], "time_span": 1}

        self.assertDictEqual(result, expected_output_data)

    def test_process(self):
        pass


class TestServerDataEventProcessor(unittest.TestCase):
    def setUp(self):
        self.r = fakeredis.FakeStrictRedis()
        self.test_config = json.load(open("test_config.json"))
        self.server_data_ep = ServerDataEventProcessor(self.r, self.test_config)

    def tearDown(self):
        self.r.flushall()

    def test_process_single_item(self):
        input_item_1 = {"data": 100, "serverId": "server1", "timeStamp": 1, "type": "AnswersPerSecond"}

        _, result = self.server_data_ep.process(input_item_1)

        expected_output = [{"servers_data": [
            {"answers_per_second": 100, "queries_per_second": 0, "server_id": "total"},
            {"answers_per_second": 100, "queries_per_second": 0, "server_id": "server1"}],
            "time_span": 1}]

        self.assertDictEqual(result[0]["servers_data"][0], expected_output[0]["servers_data"][0])

    def test_process_two_items(self):
        input_item_1 = {"data": 100, "serverId": "server1", "timeStamp": 1, "type": "AnswersPerSecond"}
        input_item_2 = {"data": 100, "serverId": "server2", "timeStamp": 2, "type": "QueriesPerSecond"}

        self.server_data_ep.process(input_item_1)
        _, result = self.server_data_ep.process(input_item_2)

        expected_output = [{"servers_data": [
            {"answers_per_second": 100, "queries_per_second": 100, "server_id": "total"},
            {"answers_per_second": 100, "queries_per_second": 0, "server_id": "server1"},
            {"answers_per_second": 0, "queries_per_second": 100, "server_id": "server2"}],
            "time_span": 1}]

        self.assertDictEqual(result[0]["servers_data"][0], expected_output[0]["servers_data"][0])


class TestTopQNamesWithIPEventProcessor(unittest.TestCase):
    def setUp(self):
        self.r = fakeredis.FakeStrictRedis()
        self.test_config = json.load(open("test_config.json"))
        self.top_qnames_ep = TopQNamesWithIPEventProcessor(self.r, self.test_config)

    def tearDown(self):
        self.r.flushall()

    def test_select_item(self):
        input_data = {"qnames_data": [
            {"qname": 'qname1',
             "total_count": 7,
             "servers_data": [
                 {"qname_server_count": 3,
                  "server_id": 'server1',
                  "top_ips": [{"ip": 'ip1', "ip_server_count": 1},
                              {"ip": 'ip2', 'ip_server_count': 2}]},
                 {"qname_server_count": 4,
                  "server_id": 'server2',
                  "top_ips": [{"ip": 'ip3',
                               "ip_server_count": 1},
                              {"ip": 'ip4',
                               'ip_server_count': 2},
                              {"ip": 'ip5',
                               "ip_server_count": 1}]}]},
            {"qname": 'qname2',
             "total_count": 10,
             "servers_data": [
                 {"qname_server_count": 9,
                  "server_id": 'server1',
                  "top_ips": [{"ip": 'ip1', "ip_server_count": 3},
                              {"ip": 'ip2', "ip_server_count": 5},
                              {"ip": 'ip6', "ip_server_count": 2}]},
                 {"qname_server_count": 1,
                  "server_id": 'server2',
                  "top_ips": [{"ip": 'ip5', "ip_server_count": 1}]}]},
            {"qname": 'qname3',
             "total_count": 2,
             "servers_data": [
                 {"qname_server_count": 2,
                  "server_id": 'server1',
                  "top_ips": [{"ip": 'ip5',
                               "ip_server_count": 2}]}]},
            {"qname": 'qname4',
             "total_count": 12,
             "servers_data": [
                 {"qname_server_count": 6,
                  "server_id": 'server1',
                  "top_ips": [{"ip": 'ip1', "ip_server_count": 1},
                              {"ip": 'ip2', 'ip_server_count': 2},
                              {"ip": 'ip6', 'ip_server_count': 3}]},
                 {"qname_server_count": 6,
                  "server_id": 'server2',
                  "top_ips": [{"ip": 'ip3',
                               "ip_server_count": 1},
                              {"ip": 'ip4',
                               'ip_server_count': 5}]}]},
            {"qname": 'qname5',
             "total_count": 1,
             "servers_data": [
                 {"qname_server_count": 1,
                  "server_id": 'server1',
                  "top_ips": [{"ip": 'ip1', "ip_server_count": 1}]}]}],
            "time_span": 1}

        result = self.top_qnames_ep.select_item(input_data)

        expected_output_data = {"qnames_data": [
            {"qname": 'qname4',
             "total_count": 12,
             "servers_data": [
                 {"qname_server_count": 6,
                  "server_id": 'server1',
                  "top_ips": [{"ip": 'ip6', 'ip_server_count': 3},
                              {"ip": 'ip2', 'ip_server_count': 2}]},
                 {"qname_server_count": 6,
                  "server_id": 'server2',
                  "top_ips": [{"ip": 'ip4',
                               "ip_server_count": 5},
                              {"ip": 'ip3',
                               'ip_server_count': 1}]}]},
            {"qname": 'qname2',
             "total_count": 10,
             "servers_data": [
                 {"qname_server_count": 9,
                  "server_id": 'server1',
                  "top_ips": [{"ip": 'ip2', "ip_server_count": 5},
                              {"ip": 'ip1', "ip_server_count": 3}]},
                 {"qname_server_count": 1,
                  "server_id": 'server2',
                  "top_ips": [{"ip": 'ip5', "ip_server_count": 1}]}]}], "time_span": 1}

        self.assertDictEqual(result, expected_output_data)

    def test_process(self):
        pass
