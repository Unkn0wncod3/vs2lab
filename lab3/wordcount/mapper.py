import re
import sys
import time
import zlib

import zmq

import constWordcount


WORD_PATTERN = re.compile(r"[A-Za-z0-9]+")


def reducer_for(word):
    return zlib.crc32(word.encode("utf-8")) % constWordcount.REDUCER_COUNT


def words_from(sentence):
    return [word.lower() for word in WORD_PATTERN.findall(sentence)]


def create_splitter_socket(context):
    pull_socket = context.socket(zmq.PULL)
    splitter_address = f"tcp://{constWordcount.HOST}:{constWordcount.SPLITTER_PORT}"
    pull_socket.connect(splitter_address)
    return pull_socket


def create_reducer_sockets(context):
    reducer_sockets = []
    for port in constWordcount.REDUCER_PORTS:
        push_socket = context.socket(zmq.PUSH)
        reducer_address = f"tcp://{constWordcount.HOST}:{port}"
        push_socket.connect(reducer_address)
        reducer_sockets.append(push_socket)
    return reducer_sockets


def forward_stop_signal(mapper_id, reducer_sockets):
    for push_socket in reducer_sockets:
        push_socket.send_string(constWordcount.END_OF_TEXT)
    print(f"Mapper {mapper_id} stopped")


def dispatch_words(mapper_id, words, reducer_sockets):
    print(f"Mapper {mapper_id} received {len(words)} words")

    for word in words:
        reducer_id = reducer_for(word)
        reducer_sockets[reducer_id].send_string(word)
        print(f"Mapper {mapper_id} sent '{word}' to reducer {reducer_id}")


def main():
    mapper_id = sys.argv[1] if len(sys.argv) > 1 else "1"

    context = zmq.Context()
    pull_socket = create_splitter_socket(context)
    reducer_sockets = create_reducer_sockets(context)

    time.sleep(1)
    print(f"Mapper {mapper_id} started")

    while True:
        sentence = pull_socket.recv_string()
        if sentence == constWordcount.END_OF_TEXT:
            forward_stop_signal(mapper_id, reducer_sockets)
            break

        dispatch_words(mapper_id, words_from(sentence), reducer_sockets)


if __name__ == "__main__":
    main()

