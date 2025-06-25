import pika
import subprocess, sys, os

class consumer:
    """
    Subscriber class for the task_publisher class, its purpose is to process individual files

    """
    def __init__(self, message: list):
        dirs = message.split('#')
        self.workdir = dirs[0]
        self.file = self.workdir + '/' + dirs[1]
        self.workdir = self.workdir
        self.step = dirs[2]
        self.outdir = self.processOutdir()
        self.script = f"{self.workdir}/Bash-Scripts/{self.step}.sh"


    def processOutdir(self):
        """ Checks for existance of output directory and creates it if not """
        outdir = self.workdir + '/output/' + self.step
        if os.path.isdir(outdir):
            print('Outdir existe')
            return outdir
        else:
            try:
                os.makedirs(outdir, exist_ok=True)
                print(f'Creando {outdir}')
            except OSError as error:
                print(error)
            return outdir

    def processMessage(self):
        """ Full function for getting the information from the queue and accessing the script """
        # For downloading we use a single txt fil with ids so we extract it
        if self.step == '01_download':
            # Splits the path created and just gets the id 
            id = (self.file.split('/')[-1:])
            id = id[0] # id is a singleton so extract just the element
            command = [self.script, 
                self.workdir, id, self.outdir]
        else:
        # For every other step, wprk with just 3 arguments, where we work, where are the files and where to put them
            command = [self.script, 
                    self.workdir, self.file, self.outdir]
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

    def assignWorker(message):
        worker = consumer(message)
        worker.processMessage()
    # Connection to rabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declaring the queue, we can do this as many times as we want so we do it everytime 
    channel.queue_declare(queue='task_queue', durable=True) # Making it durable means it will outlive a node reset
    

    # Callback function that processes the message in the queue
    def callback(ch, method, properties, body):
        message = body.decode()
        print(f" [x] Received {message}")
        assignWorker(message)
        print(" [x] Done")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    # We initialize the consuming for the queue
    channel.basic_consume(queue='task_queue', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    # Listens to the queue for new tasks until interrumpted
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
            