import sys
import time
from pathlib import Path

import zmq

import constWordcount


def read_sentences(path):
    with path.open(encoding="utf-8") as file:
        return [line.strip() for line in file if line.strip()]


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("input.txt")
    sentences = read_sentences(input_path)

    context = zmq.Context()
    push_socket = context.socket(zmq.PUSH)
    address = "tcp://" + constWordcount.HOST + ":" + constWordcount.SPLITTER_PORT
    push_socket.bind(address)

    print("Splitter bound to {}".format(address))
    print("Loaded {} sentences from {}".format(len(sentences), input_path))

    time.sleep(1)

    for sentence in sentences:
        push_socket.send_string(sentence)
        print("Splitter sent: {}".format(sentence))

    for _ in range(constWordcount.MAPPER_COUNT):
        push_socket.send_string(constWordcount.END_OF_TEXT)

    print("Splitter sent stop messages")


if __name__ == "__main__":
    main()

