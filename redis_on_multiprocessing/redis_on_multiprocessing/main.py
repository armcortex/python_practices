from multiprocessing import Process
import time
import redis
import json
import sys

from gen_data import generate_message, Customer


def producer():
    red = redis.StrictRedis('localhost', 6379)
    cnt = 0
    while True:
        try:
            person = generate_message()
        except:
            break

        if (cnt % 10) == 0:
            print(f'{cnt}: Publish: {person}')

        red.publish('person', person.json())

        cnt += 1
        if cnt >= sys.maxsize:
            cnt = 0

        time.sleep(0.01)


def consumer():
    red = redis.StrictRedis('localhost', 6379, socket_timeout=10)
    sub = red.pubsub()
    sub.subscribe('person')

    # TODO: System will hanging
    # cnt = 0
    while True:
        for cnt, message in enumerate(sub.listen()):
            if message is not None and isinstance(message, dict):
                data = message.get('data')

                # Failed
                if data == 'KILL':
                    sub.unsubscribe()
                    break

                if isinstance(data, bytes):
                    raw = json.loads(data.decode('utf-8'))
                    person = Customer(**raw)

                    if (cnt % 10) == 0:
                        print(f'{cnt}: Subscribe: {person}')

        # cnt += 1
        # if cnt >= sys.maxsize:
        #     cnt = 0

        # time.sleep(0.01)


def main():
    p_producer = Process(target=producer)
    p_consumer = Process(target=consumer)

    p_producer.start()
    p_consumer.start()

    p_producer.join()
    p_consumer.join()


if __name__ == '__main__':
    main()
