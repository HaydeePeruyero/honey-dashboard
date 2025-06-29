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
        self.file = dirs[1]
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

    import subprocess

    def processMessage(self):
        if self.step == '01_download':
            id = self.file.split('/')[-1]
            command = [self.script, self.workdir, id, self.outdir]
        else:
            command = [self.script, self.workdir, self.file, self.outdir]

        # Choose a log file name (you can customize this)
        log_path = f"{self.outdir}/log_{self.step}.txt"

        try:
            with open(log_path, "w") as logfile:
                # Start the process
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,  # line-buffered
                    universal_newlines=True
                )

                print(f"Running command: {' '.join(command)}")
                print(f"Logging output to {log_path}")

                # Read output line by line
                for line in process.stdout:
                    print(line, end='')      # print live
                    logfile.write(line)      # write to file

                # Wait for the process to finish and get exit code
                returncode = process.wait()

                if returncode == 0:
                    print("Script ran successfully!")
                else:
                    print(f"Script failed with return code {returncode}")
                    # You can raise an exception here if you prefer:
                    # raise subprocess.CalledProcessError(returncode, command)

        except Exception as e:
            print("An error occurred while running the script:")
            print(e)


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
