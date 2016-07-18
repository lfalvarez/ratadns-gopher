import random
import unittest

from gopher.eventprocessor import \
    EventConsumer, \
    MovingWindow, \
    hex2ip, \
    get_topk


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
    pass


class TestDataSortedByQTypeEventProcessor(unittest.TestCase):
    pass


class TestServerDataEventProcessor(unittest.TestCase):
    pass


class TestTopQNamesWithIPEventProcessor(unittest.TestCase):
    pass
