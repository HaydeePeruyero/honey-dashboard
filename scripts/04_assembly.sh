#!/bin/bash

# Entrada:
# 1.- El directorio donde se va a trabajar
# 2.- El nombre de la subcarpeta de resultados donde se van a poner los outputs
# 3.- La direccion apartir de 1 donde están los IDs de las muestras

##### EJEMPLO separado por lineas para mejor visualizacion
# ./04_assembly.sh
# /data2/miel_dash
# 01_fastqc_Mex
# data/ids_SRR_txt/id_Mex.txt
# 2> Archivos_Output_Mex/archivo_error_Paso04.txt
# &> Archivos_Output_Mex/archivo_output_Paso04.txt
##### las ultimas dos lineas sirven para monitoriar resultados aunque corran en un screen

# Directorios de entrada
HONEY="$1"
FASTQC="$HONEY/results/$2"
SRA_LIST="$HONEY/$3"

# Directorios de trabajo
ASSEMBLY="$FASTQC/metagenome_assembly"
TRIM="${FASTQC}/trim"

# Crear el directorio de salida si no existe
mkdir -p "$ASSEMBLY"

# Comprobamos la existencia de la lista de IDs
if [ ! -f "$SRA_LIST" ]; then
   echo "Error: No se encontró el archivo de lista $SRA_LIST"
   exit 1
fi

echo "SRA_LIST is set to: $SRA_LIST"
ls -1 "$SRA_LIST"

# Descargar y ensamblar cada ID
while read -r SRA_ID; do
    # Verificar si ya existe el output de ensamblaje
    if [ -f "${ASSEMBLY}/assembly_${SRA_ID}/$SRA_ID.contigs.fa" ]; then
       echo "Ya existe $SRA_ID.contigs.fa, se omite."
       continue
    fi

    # Leer paths de archivos .trim
    R1="${TRIM}/${SRA_ID}_R1.trim.fastq.gz"
    R2="${TRIM}/${SRA_ID}_R2.trim.fastq.gz"

    # Verificar existencia de los archivos .trim
    if [ ! -f "${R1}" ] || [ ! -f "${R2}" ]; then
        echo "No se encontraron los archivos de lectura para $R1 en $TRIM"
        exit 1
    fi

    echo "Procesando $SRA_ID..."

    # Ensamblado con metaspades
    #metaspades.py -1 "$R1" -2 "$R2" -o "$ASSEMBLY/assembly_${SRA_ID}"

    # Ensamblado con megahit
    megahit -1 "${R1}" -2 "${R2}" -t 12 -o "${ASSEMBLY}/assembly_${SRA_ID}"

    # Renombrando el output
    mv "${ASSEMBLY}/assembly_${SRA_ID}/final.contigs.fa" "${ASSEMBLY}/assembly_${SRA_ID}/${SRA_ID}.contigs.fa"

    echo "Muestra $SRA_ID procesada y guardada en $ASSEMBLY"

done < "$SRA_LIST"

echo "Procesamiento completado."
