import sys

import zmq

import constWordcount


def main():
    reducer_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    if reducer_id < 0 or reducer_id >= constWordcount.REDUCER_COUNT:
        raise ValueError("Reducer id must be 0 or 1")

    context = zmq.Context()
    pull_socket = context.socket(zmq.PULL)
    address = "tcp://" + constWordcount.HOST + ":" + constWordcount.REDUCER_PORTS[reducer_id]
    pull_socket.bind(address)

    counts = {}
    stop_messages = 0

    print("Reducer {} bound to {}".format(reducer_id, address))

    while stop_messages < constWordcount.MAPPER_COUNT:
        word = pull_socket.recv_string()

        if word == constWordcount.END_OF_TEXT:
            stop_messages += 1
            print(
                "Reducer {} received stop message {}/{}".format(
                    reducer_id, stop_messages, constWordcount.MAPPER_COUNT
                )
            )
            continue

        counts[word] = counts.get(word, 0) + 1
        print("Reducer {}: {} -> {}".format(reducer_id, word, counts[word]))

    print("Reducer {} final result:".format(reducer_id))
    for word in sorted(counts):
        print("{} -> {}".format(word, counts[word]))


if __name__ == "__main__":
    main()

