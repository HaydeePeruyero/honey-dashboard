from task_publisher import task_publisher
import pika, os, sys

def main():
    print("Bienvenido, escriba el working directory o nada para el workdir madre")
    workdir = input() 
    if workdir == "":
        workdir = None
    print("Escriba el número del paso que se llevará a cabo de los siguientes:\n" \
            "01_Download\n" \
            "02_FastqAnalysis")
    step = input()
    publisher = task_publisher(workdir, inputdir="", step=step)
    
    publisher.publish()

if __name__ == "__main__":
    main()
