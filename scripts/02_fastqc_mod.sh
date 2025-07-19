#!/bin/bash

# Script para evaluar la calidad de las librerias de secuenciacion

# Entrada:
# 1.- El directorio donde se va a trabajar
# 2.- El nombre de la subcarpeta de data donde estan los fastq sin procesar
# 3.- El nombre de la subcarpeta de results donde se pondran los resultados

# Ejemplode uso
# ./02_fastqc_mod.sh /data2/miel_dash SRA_Mex 01_fastqc_Mex

# Directorios de entrada
HONEY="$1"
DATA="${HONEY}/data/$2"
RESULTS="${HONEY}/results"
FASTQC="${RESULTS}/$3"

# Directorio de trabajo
ZIP="${FASTQC}/zip"
HTML="${FASTQC}/html"

# Crear carpetas en caso de que no existan

mkdir -p "$FASTQC" "$ZIP" "$HTML"

# Verificar permisos de escritura
for dir in "$FASTQC" "$ZIP" "$HTML"; do
    if [ ! -w "$dir" ]; then
        echo " No tienes permisos de escritura en '$dir'."
        exit 1
    fi
done

# Script para el bucle
for filename in "${DATA}"/*.fastq.gz
do
	name=$(basename "${filename}" .fastq.gz)
        #Verifica si ya existe el output
        if [ -f "${ZIP}/${name}_fastqc.zip" ]; then
            echo "Ya existe $zip_result, se omite."
            continue
        fi

	echo "Esta corriendo la muestra ${filename}..."
	fastqc "${filename}"
	echo "Termino de correr la muestra y ahora va a move los archivos"
	mv "$DATA/${name}_fastqc.html" "$HTML/."
	mv "$DATA/${name}_fastqc.zip" "$ZIP/."
done
