import random
import unittest


from gopher.eventprocessor import \
        EventConsumer, \
        MovingWindow, \
        ServerDataEventProcessor, \
        EventProcessor, \
        TopQNamesEventProcessor, \
        QueriesSummaryEventProcessor, \
        hex2ip, \
        get_topk


class TestEventConsumer(unittest.TestCase):
    def test_get_data_after_consume(self):
        ec = EventConsumer()
        input_value = random.random()
        ec.consume(input_value)
        output_value = ec.get_data()
        self.assertEqual(input_value, output_value)


# class TestEventProcessor(unittest.TestCase):
#     def setUp(self):
#         pass
# 
#     def test_register_consumer(self):
#         pass

class TestMovingWindow(unittest.TestCase):
    def setUp(self):
        self.moving_window = MovingWindow()

    def test_add_10_items_and_get(self):
        test_list = [i for i in range(0,10)]
        for i in test_list:
            self.moving_window.add_item(i, i)

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, test_list)

        items = self.moving_window.get_items_after_limit(4)
        self.assertListEqual(items, [i for i in range(5, 10)])

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, test_list)

    def test_add_10_items_remove_and_get(self):
        for i in range(0,10):
            self.moving_window.add_item(i, i)
        self.moving_window.remove_old_data(4)

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, [i for i in range(5, 10)])


class TestHex2IP(unittest.TestCase):
    def test_valid_ipv6(self):
        hex_ip = "2001:cdba::3257:9652"
        ip = hex2ip(hex_ip)
        self.assertEqual(ip, hex_ip)

    def test_valid_ipv4(self):
        hex_ip = "A41E6B0B"
        ip = hex2ip(hex_ip)
        self.assertEqual(ip, "164.30.107.11")


class TestGetTopK(unittest.TestCase):
    def setUp(self):
        self.simple_list = [1, 2, 3, 4, 5]
        self.identity = lambda x: x

    def test_get_0_elements(self):
        k = 0
        result_list = get_topk(self.simple_list, k, self.identity)
        self.assertListEqual(result_list, [])

    def test_get_more_elements_than_size(self):
        k = len(self.simple_list) + 1
        result_list = get_topk(self.simple_list, k, self.identity)
        self.assertListEqual(result_list, sorted(self.simple_list, reverse=True))

    def test_get_3_elements(self):
        k = 3
        result_list = get_topk(self.simple_list, k, self.identity)
        self.assertListEqual(result_list, [5, 4, 3])

# class TestWindowedEventProcessor(unittest.TestCase):
#
#
# class TestQueriesSummaryEventProcessor(unittest.TestCase):
#
#
# class TestServerDataEventProcessor(unittest.TestCase):
#
#
# class TestTopQNamesEventProcessor(unittest.TestCase):
