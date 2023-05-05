import fire
from google.cloud import pubsub_v1
import uuid
from time import sleep
import threading

def main(project_id='ml-services-385715'):

    lock = threading.Lock()
    shared_data = 0
    def callback(message):
        nonlocal shared_data
        with lock:
            sleep(0.5)
            shared_data += 1
        # print(f'received {message}')
        message.ack()
        print(f'acknowledged {message.message_id}')

    def plain_callback(message):
        print(f"Received {message}.")
        message.ack()

    topic_id = 'topic-streamvis-{}'.format(str(uuid.uuid4())[:8])
    subscription_id = 'subscription-streamvis-{}'.format(str(uuid.uuid4())[:8])
    print()
    print(f'TOPIC={topic_id}')
    print(f'SUBSC={subscription_id}')

    with pubsub_v1.SubscriberClient() as client:
        topic_path = client.topic_path(project_id, topic_id)
        sub_path = client.subscription_path(project_id, subscription_id)

    with pubsub_v1.PublisherClient() as client:
        topic = client.create_topic(request = {'name': topic_path})
        # print(f'Created topic {topic}')

    sub_client = pubsub_v1.SubscriberClient()
    req = dict(name=sub_path, topic=topic_path)
    subscr = sub_client.create_subscription(request=req)
    # print(f'Created subscription {subscr}')

    # print(f'Subscribing to {sub_path}...', end='')  
    future = sub_client.subscribe(sub_path, callback=plain_callback)
    del future
    del sub_client

    # future = sub_client.subscribe(sub_path, callback)
    while True:
        sleep(1)
        if lock.acquire(blocking=False):
            # print(f'shared_data = {shared_data}')
            lock.release()
        else:
            print(f'skipping.  couldn\'t acquire lock')

fire.Fire(main)
