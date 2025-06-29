import pika 
import os, sys


class task_publisher:
    """
    Clase para publicar tareas de documentos a procesar
    
    Attributes
    ----------
    workdir: str
        Directorio principal donde se trabaja
    inputdir: str
        Path al directorio o archivo txt de documentos a procesar
    step: str
        Nombre del paso a procesar.
    """

    def __init__(self, workdir: str = None, inputdir: str = None, step: str = None):        
        # Por default 
        self.workdir = workdir
        if self.workdir == None:
            self.workdir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
            self.process_workdir()
        else:
            self.workdir = workdir
            self.process_workdir()
        self.inputdir = inputdir
        self.step = self.process_step(step)
        self.inputdir = self.process_inputdir()
        

    def process_workdir(self):
        """
        Revisa que el workdir ingresado exista.
        """
        if os.path.isdir(self.workdir):
            print('Work directory existe')
        else:
            print('Work directory no existe, intentelo de nuevo')
            print(self.workdir)
            self.workdir = input()
            self.workdir = self.process_workdir()
            return



    # TODO make sure input is prompted only if it is failed 
    def process_inputdir(self):
        """
        Revisa que el inputdir ingresado exista.
        """
        if self.step != '01_download':
            while True:
                try:
                    print("Escriba el directorio dentro de workdir donde se encuentran los archivos a procesar:")
                    filedir = os.path.join(self.workdir, input())

                    if not os.path.isdir(filedir):
                        raise FileNotFoundError(f'{filedir} no existe, intentelo de nuevo.')

                    print('File directory existe')
                    return filedir

                except FileNotFoundError as e:
                    print(e)
        else:
            while True:
                try:
                    print("Escriba la ruta el archivo donde se encuentran los ids relativa al workdir:")
                    filedir = os.path.join(self.workdir, input())

                    if not os.path.exists(filedir):
                        raise FileNotFoundError(f'{filedir} no existe, intentelo de nuevo.')

                    print('El archivo proporcionado existe')
                    return filedir

                except FileNotFoundError as e:
                    print(e)

    
    def process_step(self, step):
        """
        Con ayuda de un diccionario asigna un valor numérico al texto descriptivo usado por el sistema
        """
        conversion = {
            "01": "01_download",
            "02": "02_fastqcAnalysis",
            "03": "03_trimming",
            "04": "04_assembly",
            "05a": "05a_binning",
            "05b": "05b_qualityCheck",
            "06": "06_mags",
            "1": "01_download",
            "2": "02_fastqc_analysis",
            "3": "03_trimming",
            "4": "04_assembly",
            "5": "05_binning",
            "6": "06_mags",
        }
        try:
            return conversion[step]
        except KeyError:
            print("Ingresaste una clave incorrecta, ingresa solo el numero")
            step = input()
            self.process_step()


    def publish(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Declaring the queue, we can do this as many times as we want 
        channel.queue_declare(queue='task_queue', durable=True) # Making it durable means it will outlive a node reset

        # Crear un loop para todos los archivos en el directorio dado
        if self.step == '01_download':
            ids = []
            with open(self.inputdir, 'r') as f:
                ids = f.read().split('\n')
            for id in ids:
                # Using the '#' delimiter send the workdir, the download id and the step 
                message = f"{self.workdir}#{id}#{self.step}"
                # The exchange is an empty string by which we connect to the queue, this is the method by which we publish it 
                channel.basic_publish(exchange='',
                                    routing_key='task_queue',
                                    body=message,
                                    properties=pika.BasicProperties(
                                        delivery_mode = pika.DeliveryMode.Persistent
                                    ))
                print(f" [x] Sent {message}")
        else:    
            for file in os.listdir(self.inputdir):
                filepath = os.path.join(self.inputdir, file)
                message = f"{self.workdir}#{self.inputdir}/{file}#{self.step}"
                # The exchange is an empty string by which we connect to the queue, this is the method by which we publish it 
                channel.basic_publish(exchange='',
                                    routing_key='task_queue',
                                    body=message,
                                    properties=pika.BasicProperties(
                                        delivery_mode = pika.DeliveryMode.Persistent
                                    ))
                print(f" [x] Sent {message}")

        connection.close()
    