import json
import logging
import random
import socket

import const_cs
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)


class PhoneBook:
    def __init__(self, entries=None):
        if entries is None:
            entries = {
                "Person001": "042-123456",
                "Person002": "042-987654",
                "Person003": "042-555555",
            }
        self._entries = dict(entries)

    @classmethod
    def with_random_entries(cls, count, seed=None):
        generator = random.Random(seed)
        entries = {}
        for index in range(count):
            entries[f"Person{index + 1:03d}"] = (
                f"042-{generator.randint(100000, 999999)}"
            )
        return cls(entries)

    def get(self, name):
        return self._entries.get(name)

    def getall(self):
        return dict(self._entries)

class Server:
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")

    def __init__(self, phonebook=None, host=const_cs.HOST, port=const_cs.PORT):
        self.phonebook = phonebook or PhoneBook()
        self.host = host
        self.port = port
        self._serving = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.settimeout(3)
        self._logger.info("Server bound to socket %s", self.sock)

    def serve(self):
        self.sock.listen(5)
        self._logger.info("Server listening on %s:%s", self.host, self.port)
        while self._serving:
            try:
                connection, address = self.sock.accept()
                self._logger.info("Accepted connection from %s", address)
                with connection:
                    request_bytes = connection.recv(65535)
                    if not request_bytes:
                        self._logger.info("Client %s disconnected without request", address)
                        continue
                    request = self._decode_message(request_bytes)
                    self._logger.info(
                        "Received request %s",
                        self._summarize_message(request),
                    )
                    response = self._handle_request(request)
                    self._logger.info(
                        "Sending response %s",
                        self._summarize_message(response),
                    )
                    connection.sendall(self._encode_message(response))
                self._logger.info("Closed connection to %s", address)
            except socket.timeout:
                continue
        self.sock.close()
        self._logger.info("Server down.")

    def stop(self):
        self._serving = False

    def _handle_request(self, request):
        command = request.get("command")
        if command == "GET":
            name = request.get("name")
            if not name:
                return {"status": "ERROR", "message": "missing name"}
            number = self.phonebook.get(name)
            if number is None:
                return {"status": "NOTFOUND", "name": name}
            return {"status": "OK", "name": name, "number": number}
        if command == "GETALL":
            return {"status": "OK", "entries": self.phonebook.getall()}
        return {"status": "ERROR", "message": "unknown command"}

    @staticmethod
    def _encode_message(message):
        return json.dumps(message).encode("utf-8")

    @staticmethod
    def _decode_message(data):
        return json.loads(data.decode("utf-8"))

    @staticmethod
    def _summarize_message(message):
        if message.get("command") == "GETALL":
            return {"command": "GETALL"}
        entries = message.get("entries")
        if entries is not None:
            return {"status": message.get("status"), "entry_count": len(entries)}
        return message


class Client:
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Client")

    def __init__(self, host=const_cs.HOST, port=const_cs.PORT):
        self.host = host
        self.port = port
        self.sock = None

    def get(self, name):
        response = self._request({"command": "GET", "name": name})
        if response["status"] == "OK":
            return response["number"]
        if response["status"] == "NOTFOUND":
            return None
        raise ValueError(response["message"])

    def getall(self):
        response = self._request({"command": "GETALL"})
        if response["status"] != "OK":
            raise ValueError(response["message"])
        return response["entries"]

    def call(self, name="Alice"):
        result = self.get(name)
        print(result)
        return result

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def _request(self, request):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._logger.info("Connecting to %s:%s", self.host, self.port)
        try:
            self.sock.connect((self.host, self.port))
            self._logger.info("Client connected to socket %s", self.sock)
            self.sock.sendall(self._encode_message(request))
            self._logger.info("Sent request %s", self._summarize_message(request))
            data = self.sock.recv(65535)
            response = self._decode_message(data)
            self._logger.info("Received response %s", self._summarize_message(response))
            return response
        finally:
            self.close()
            self._logger.info("Client down.")

    @staticmethod
    def _encode_message(message):
        return json.dumps(message).encode("utf-8")

    @staticmethod
    def _decode_message(data):
        return json.loads(data.decode("utf-8"))

    @staticmethod
    def _summarize_message(message):
        if message.get("command") == "GETALL":
            return {"command": "GETALL"}
        entries = message.get("entries")
        if entries is not None:
            return {"status": message.get("status"), "entry_count": len(entries)}
        return message
