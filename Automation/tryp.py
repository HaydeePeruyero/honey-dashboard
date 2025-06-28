import pika
import subprocess
import sys
import os
from concurrent.futures import ThreadPoolExecutor
import queue

executor = ThreadPoolExecutor(max_workers=4)
ack_queue = queue.Queue()  # Thread-safe queue for delivery tags to ack

class consumer:
    def __init__(self, message: str):
        dirs = message.split('#')
        self.workdir = dirs[0]
        self.file = self.workdir + '/' + dirs[1]
        self.step = dirs[2]
        self.outdir = self.processOutdir()
        self.script = f"{self.workdir}/Bash-Scripts/{self.step}.sh"

    def processOutdir(self):
        outdir = self.workdir + '/output/' + self.step
        if not os.path.isdir(outdir):
            try:
                os.makedirs(outdir, exist_ok=True)
                print(f'Creating {outdir}')
            except OSError as error:
                print(error)
        else:
            print('Outdir exists')
        return outdir

    def processMessage(self):
        if self.step == '01_download':
            id = self.file.split('/')[-1]
            command = [self.script, self.workdir, id, self.outdir]
        else:
            command = [self.script, self.workdir, self.file, self.outdir]

        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print("Script ran successfully!")
            print("STDOUT:\n", result.stdout)
            if result.stderr:
                print("STDERR:\n", result.stderr)
        except subprocess.CalledProcessError as e:
            print("Script failed!")
            print("Return code:", e.returncode)
            print("STDOUT:\n", e.stdout)
            print("STDERR:\n", e.stderr)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)

    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        message = body.decode()
        print(f" [x] Received {message}")

        def process_and_signal_ack():
            worker = consumer(message)
            worker.processMessage()
            # Instead of acking here, put the delivery_tag in the ack_queue
            ack_queue.put((ch, method.delivery_tag))
            print(" [x] Done")

        executor.submit(process_and_signal_ack)

    channel.basic_consume(queue='task_queue', on_message_callback=callback)

    try:
        while True:
            # Process RabbitMQ events
            connection.process_data_events(time_limit=1)

            # Process any pending acks
            while not ack_queue.empty():
                ch, delivery_tag = ack_queue.get()
                ch.basic_ack(delivery_tag=delivery_tag)
                ack_queue.task_done()
    except KeyboardInterrupt:
        print('Interrupted')
        executor.shutdown(wait=True)
        channel.stop_consuming()
        connection.close()

if __name__ == '__main__':
    main()
