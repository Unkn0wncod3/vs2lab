import rpc
import logging
import time

from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)


def handle_result(result_list):
    print("Result from server: {}".format(result_list.value))


client = rpc.Client()
client.run()

base_list = rpc.DBList({'Startwert'})
ack = client.append('Appendwert', base_list, handle_result)

print("Server acknowledgement: {}".format(ack))
for step in range(1, 6):
    print("Client is still working while waiting... step {}".format(step))
    time.sleep(2)

if client.result_thread is not None:
    client.result_thread.join()

client.stop()
