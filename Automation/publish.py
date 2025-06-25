from task_publisher import task_publisher
import pika, os, sys

def main():
    print("Bienvenido, escriba el working directory o nada para el workdir madre")
    workdir = input() 
    if workdir == "":
        workdir = None
    print("Escriba el directorio de donde se obtendrán los archivos o el archivo para descarga relativo al workdir")
    inputdir = input()
    print("Escriba el número del paso que se llevará a cabo de los siguientes:\n" \
            "01_Download\n" \
            "02_FastqAnalysis")
    step = input()
    publisher = task_publisher(workdir, inputdir, step)
    
    publisher.publish()

if __name__ == "__main__":
    main()
