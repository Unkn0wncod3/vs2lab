import logging
import socket
import threading
import time
import unittest

import clientserver
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

def _reserve_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

def _wait_for_server(host, port, attempts=30, delay=0.1):
    for _ in range(attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((host, port))
                return
            except OSError:
                time.sleep(delay)
    raise RuntimeError("server did not start in time")


class TestPhoneBookBackend(unittest.TestCase):
    def test_get_returns_number_for_existing_entry(self):
        phonebook = clientserver.PhoneBook({"Alice": "12345"})
        self.assertEqual(phonebook.get("Alice"), "12345")

    def test_get_returns_none_for_unknown_entry(self):
        phonebook = clientserver.PhoneBook({"Alice": "12345"})
        self.assertIsNone(phonebook.get("Bob"))

    def test_getall_returns_all_entries(self):
        entries = {"Alice": "12345", "Bob": "67890"}
        phonebook = clientserver.PhoneBook(entries)
        self.assertEqual(phonebook.getall(), entries)

    def test_with_random_entries_creates_requested_number_of_entries(self):
        phonebook = clientserver.PhoneBook.with_random_entries(10, seed=7)
        self.assertEqual(len(phonebook.getall()), 10)

    def test_with_random_entries_is_repeatable_with_seed(self):
        first = clientserver.PhoneBook.with_random_entries(5, seed=11).getall()
        second = clientserver.PhoneBook.with_random_entries(5, seed=11).getall()
        self.assertEqual(first, second)


class TestPhoneDirectoryService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = _reserve_free_port()
        cls.entries = {
            "Alice": "12345",
            "Bob": "67890",
            "Carol": "11111",
        }
        cls.server = clientserver.Server(
            clientserver.PhoneBook(cls.entries),
            port=cls.port,
        )
        cls.server_thread = threading.Thread(target=cls.server.serve)
        cls.server_thread.start()
        _wait_for_server("127.0.0.1", cls.port)

    def setUp(self):
        super().setUp()
        self.client = clientserver.Client(port=self.port)

    def test_get_returns_number(self):
        self.assertEqual(self.client.get("Alice"), "12345")

    def test_get_returns_none_for_unknown_entry(self):
        self.assertIsNone(self.client.get("Mallory"))

    def test_getall_returns_complete_phonebook(self):
        self.assertEqual(self.client.getall(), self.entries)

    def tearDown(self):
        self.client.close()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.server_thread.join()


class TestPhoneDirectoryServiceWithLargeGetAll(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = _reserve_free_port()
        cls.entries = {
            f"Person {index:03d}": f"040-{100000 + index}"
            for index in range(500)
        }
        cls.server = clientserver.Server(
            clientserver.PhoneBook(cls.entries),
            port=cls.port,
        )
        cls.server_thread = threading.Thread(target=cls.server.serve)
        cls.server_thread.start()
        _wait_for_server("127.0.0.1", cls.port)

    def test_getall_with_500_entries(self):
        client = clientserver.Client(port=self.port)
        try:
            self.assertEqual(client.getall(), self.entries)
        finally:
            client.close()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.server_thread.join()


if __name__ == "__main__":
    unittest.main()
