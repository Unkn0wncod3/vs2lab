import constRPC
import threading
import time

from context import lab_channel


class DBList:
    def __init__(self, start_list):
        self.value = list(start_list)

    def append(self, data):
        self.value = self.value + [data]
        return self


class Client:
    def __init__(self):
        self.channel = lab_channel.Channel()
        self.client = self.channel.join('client')
        self.server = None
        self.result_thread = None

    def run(self):
        self.channel.bind(self.client)
        self.server = self.channel.subgroup('server')

    def stop(self):
        if self.result_thread is not None:
            self.result_thread.join()
        self.channel.leave('client')

    def _wait_for_result(self, callback_function):
        while True:
            reply = self.channel.receive_from(self.server)
            if reply[1][0] == constRPC.RESULT:
                callback_function(reply[1][1])
                return

    def append(self, data, db_list, callback_function):
        assert isinstance(db_list, DBList)
        request = (constRPC.APPEND, data, db_list)
        self.channel.send_to(self.server, request)
        reply = self.channel.receive_from(self.server)

        if reply[1][0] != constRPC.ACK:
            raise RuntimeError('Expected ACK from server.')

        self.result_thread = threading.Thread(
            target=self._wait_for_result,
            args=(callback_function,),
            daemon=False
        )
        self.result_thread.start()
        return reply[1][1]


class Server:
    def __init__(self):
        self.channel = lab_channel.Channel()
        self.server = self.channel.join('server')
        self.timeout = 3

    @staticmethod
    def append(data, db_list):
        assert isinstance(db_list, DBList)
        return db_list.append(data)

    def run(self):
        self.channel.bind(self.server)
        while True:
            request = self.channel.receive_from_any(self.timeout)
            if request is not None:
                client_name = request[0]
                rpc_call = request[1]
                if constRPC.APPEND == rpc_call[0]:
                    self.channel.send_to({client_name}, (constRPC.ACK, constRPC.OK))
                    time.sleep(10)
                    result = self.append(rpc_call[1], rpc_call[2])
                    self.channel.send_to({client_name}, (constRPC.RESULT, result))
