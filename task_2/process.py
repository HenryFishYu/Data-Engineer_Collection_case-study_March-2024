import argparse
from itertools import islice
import json
import queue
import threading

from task_2.review import Review

READ_BATCH_SIZE = 2
NUMBER_OF_THREADS = 4


def read_input() -> (str, str, str, str):
    parser = argparse.ArgumentParser(description='Process reviews and filter out inappropriate words')

    parser.add_argument('--input', required=True, help='Local filesystem path to the JSONL file containing the reviews')
    parser.add_argument('--inappropriate_words', required=True,
                        help='Local filesystem path to the new-line delimited text file containing the words to filter out')
    parser.add_argument('--output', required=True,
                        help='Local filesystem path to write the successfully processed reviews to')
    parser.add_argument('--aggregations', required=True,
                        help='Local file system path to write the aggregations as JSONL')

    args = parser.parse_args()
    input_file = args.input
    inappropriate_words_file = args.inappropriate_words
    output_file = args.output
    aggregations_file = args.aggregations
    return input_file, inappropriate_words_file, output_file, aggregations_file


def message_consumer(queue: queue.Queue[str], file_name: str) -> None:
    file = open(file_name, "w")
    while True:
        message = queue.get()
        if message is None:
            return
        queue.task_done()
        if not message:
            return
        file.write(message)
        file.write("\n")
        file.flush()


def message_generator(queue: queue.Queue[str], file_name: str) -> None:
    with open(file_name) as file:
        while True:
            lines = list(islice(file, READ_BATCH_SIZE))
            if not lines:
                return
            for line in lines:
                queue.put(line)
            queue.join()


def message_processor(ingestion_queue: queue.Queue[str], output_message_queue: queue.Queue[str],
                      output_poison_message_queue: queue.Queue[str], inappropriate_words_set: set[str]):
    while True:
        message = ingestion_queue.get()
        if message is None:
            return
        message = message.strip("\n")
        try:
            json_data = json.loads(message)
            review = Review.from_json(json_data)
            if review.inappropriate_words_filter(inappropriate_words_set):
                output_message_queue.put(review.to_json_str())
            else:
                output_poison_message_queue.put(message)
        except:
            output_poison_message_queue.put(message)
        finally:
            ingestion_queue.task_done()


def main() -> None:
    ingestion_queue = queue.Queue()
    output_message_queue = queue.Queue()
    output_poison_message_queue = queue.Queue()
    input, inappropriate_words, output, aggregations = read_input()
    inappropriate_words_set = set(line.rstrip('\n').upper() for line in open(inappropriate_words, "r").readlines())

    ingestion_thread = threading.Thread(target=message_generator, args=(ingestion_queue, input))
    consumer_message_thread = threading.Thread(target=message_consumer, args=(output_message_queue, output))
    consumer_poison_message_thread = threading.Thread(target=message_consumer,
                                                      args=(output_poison_message_queue, "error_message.txt"))
    ingestion_thread.start()
    consumer_message_thread.start()
    consumer_poison_message_thread.start()

    process_threads = []
    for i in range(NUMBER_OF_THREADS):
        process_thread = threading.Thread(target=message_processor, args=(
        ingestion_queue, output_message_queue, output_poison_message_queue, inappropriate_words_set))
        process_thread.start()
        process_threads.append(process_thread)

    ingestion_thread.join()

    for i in range(NUMBER_OF_THREADS):
        ingestion_queue.put(None)

    for thread in process_threads:
        thread.join()

    output_message_queue.put(None)
    output_poison_message_queue.put(None)

    consumer_message_thread.join()
    consumer_poison_message_thread.join()




if __name__ == "__main__":
    main()
