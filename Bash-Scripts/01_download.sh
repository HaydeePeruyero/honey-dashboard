#!/bin/bash
#Entrada: 1.- El directorio donde se va a trabajar, este directorio ocupa que halla una carpeta que se llame data
#         2.- El nombre del archivo que tien los ids, esta debera estar en la carpeta data
#         3.. El nombre de la carpeta donde se van a guardar las  secuencias en formato fastq,esta carpeta tambien debera e>

# Directorio de trabajo 
WORKDIR="$1"

# ID a descargar 
SRA_ID="$2"

# Directorio de salida para los archivos fastq
OUTDIR="$3"

# Número de hilos a usar
THREADS=4

# Verificar si SRA Toolkit está instalado
if ! command -v prefetch &> /dev/null || ! command -v fasterq-dump &> /dev/null; then
    echo "Error: SRA Toolkit (prefetch y fasterq-dump) no están instalados o no están en el PATH"
    exit 1
fi

#
echo "SRA_LIST is set to: $SRA_LIST"



# Descargar y convertir el ID

# Verificar si ya existen archivos comprimidos
if ls "$OUTDIR/${SRA_ID}"*.fastq.gz &> /dev/null; then
    echo "Ya existe output para $SRA_ID, se omite."
    continue
fi

echo "Procesando $SRA_ID..."

# Descargar con prefetch

prefetch "$SRA_ID" --output-directory "$OUTDIR"

# Convertir a fastq con fasterq-dump
fasterq-dump "$OUTDIR/$SRA_ID" --outdir "$OUTDIR" -e "$THREADS"

# Comprimir los archivos fastq
gzip "$OUTDIR/$SRA_ID"_*.fastq

echo "$SRA_ID listo."

echo "Descarga y conversión completadas."