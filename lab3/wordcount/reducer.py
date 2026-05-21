import sys

import zmq

import constWordcount


def reducer_id_from_args():
    reducer_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    if reducer_id not in constWordcount.REDUCER_IDS:
        raise ValueError(f"Reducer id must be one of: {constWordcount.REDUCER_IDS}")

    return reducer_id


def create_reducer_socket(context, reducer_id):
    pull_socket = context.socket(zmq.PULL)
    address = f"tcp://{constWordcount.HOST}:{constWordcount.REDUCER_PORTS[reducer_id]}"
    pull_socket.bind(address)
    print(f"Reducer {reducer_id} bound to {address}")
    return pull_socket


def handle_stop_message(reducer_id, stop_messages):
    print(f"Reducer {reducer_id} received stop message {stop_messages}/{constWordcount.MAPPER_COUNT}")


def count_word(counts, word):
    counts[word] = counts.get(word, 0) + 1
    return counts[word]


def print_final_result(reducer_id, counts):
    print(f"Reducer {reducer_id} final result:")
    for word in sorted(counts):
        print(f"{word} -> {counts[word]}")


def main():
    reducer_id = reducer_id_from_args()

    context = zmq.Context()
    pull_socket = create_reducer_socket(context, reducer_id)

    counts = {}
    stop_messages = 0

    while stop_messages < constWordcount.MAPPER_COUNT:
        word = pull_socket.recv_string()

        if word == constWordcount.END_OF_TEXT:
            stop_messages += 1
            handle_stop_message(reducer_id, stop_messages)
            continue

        new_count = count_word(counts, word)
        print(f"Reducer {reducer_id}: {word} -> {new_count}")

    print_final_result(reducer_id, counts)


if __name__ == "__main__":
    main()

