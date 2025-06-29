#!/bin/bash
# Script para evaluar la calidad de las librerias de secuenciacion
# Entrada:
#   1.- El directorio de trabajo principal
#   2.- El archivo que se va a procesar
#   3.- El nombre del archivo que se va a mandar a procesar 

HONEY="$1"
FILENAME="$2"
OUTDIR="$3"
INPUTDIR="$(dirname "$FILENAME")"
ZIP="${OUTDIR}/zip"
HTML="${OUTDIR}/html"

# Crear carpetas en caso de que no existan
mkdir -p "$OUTDIR" "$ZIP" "$HTML"


if [[ $FILENAME == *.fastq.gz ]] # Checar temrinación correcta
    then
    echo "Esta corriendo la muestra ${FILENAME}..."
    name=$(basename "${FILENAME}" .fastq.gz)

    # Check if output files already exist
    if [[ -f "$HTML/${name}_fastqc.html" || -f "$ZIP/${name}_fastqc.zip" ]]; then
        echo "Error: El archivo de salida ya existe para la muestra '${name}'."
        exit 2
    fi

    # Ejecutar fastqc
    fastqc "${FILENAME}"
    # TODO Change statements so the files are not moved into inputdir 
    echo "Terminó de correr la muestra y ahora va a mover los archivos"
    mv "$INPUTDIR/${name}_fastqc.html" "$HTML/."
    mv "$INPUTDIR/${name}_fastqc.zip" "$ZIP/."

fi


