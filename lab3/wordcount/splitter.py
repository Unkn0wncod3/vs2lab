import sys
import time
from pathlib import Path

import zmq

import constWordcount


def input_path_from_args():
    return Path(sys.argv[1]) if len(sys.argv) > 1 else Path("input.txt")


def read_sentences(path):
    with path.open(encoding="utf-8") as file:
        return [line.strip() for line in file if line.strip()]


def create_splitter_socket(context):
    push_socket = context.socket(zmq.PUSH)
    address = f"tcp://{constWordcount.HOST}:{constWordcount.SPLITTER_PORT}"
    push_socket.bind(address)
    print(f"Splitter bound to {address}")
    return push_socket


def send_sentences(push_socket, sentences):
    for sentence in sentences:
        push_socket.send_string(sentence)
        print(f"Splitter sent: {sentence}")


def send_stop_messages(push_socket):
    for _ in range(constWordcount.MAPPER_COUNT):
        push_socket.send_string(constWordcount.END_OF_TEXT)
    print("Splitter sent stop messages")


def main():
    input_path = input_path_from_args()
    sentences = read_sentences(input_path)

    context = zmq.Context()
    push_socket = create_splitter_socket(context)

    print(f"Loaded {len(sentences)} sentences from {input_path}")

    time.sleep(1)

    send_sentences(push_socket, sentences)
    send_stop_messages(push_socket)


if __name__ == "__main__":
    main()

