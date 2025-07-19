#!/bin/bash

# Script para evaluar la calidad de las librerías de secuenciación con Trimmomatic

# Entrada:
# 1.- El directorio donde se va a trabajar
# 2.- El nombre de la subcarpeta de data donde se hicieron las descargas
# 3.- El nombre de la subcarpeta de results donde se guardan los resultados
# 4.- La direccion apartir de 1 donde están los IDs de las muestras

# Ejemplo de uso:
# ./03_trimmomatic.sh /data2/miel_dash SRA_Mex 01_fastqc_Mex data/ids_SRR_txt/id_Mex.txt

#directorios
HONEY="$1"
DATA="${HONEY}/data/$2"
FASTQC="$HONEY/$3"
TRIM="${FASTQC}/trim"
#lista de ids de SRA
SRA_LIST="$HONEY/$4"


# Crear carpetas si no existen
mkdir -p "$TRIM"



# Loop para procesar todos los archivos _1.fastq.gz (pares)
#for R1 in "${DATA}"/*_1.fastq.gz
#do
while read -r base; do

    # Derivar nombre base (ej: SRR27931732)
    R1="${DATA}/${base}_1.fastq.gz"
    R2="${DATA}/${base}_2.fastq.gz"

    if [ -f "${TRIM}/${base}_R1.trim.fastq.gz" ]; then
        echo "Ya esta procesada la muestra, se omite."
        continue
    fi

    echo "Procesando muestra: $base"

    # Archivos de salida
    OUT_R1_PAIRED="${TRIM}/${base}_R1.trim.fastq.gz"
    OUT_R1_UNPAIRED="${TRIM}/${base}_R1un.trim.fastq.gz"
    OUT_R2_PAIRED="${TRIM}/${base}_R2.trim.fastq.gz"
    OUT_R2_UNPAIRED="${TRIM}/${base}_R2un.trim.fastq.gz"

    # Ejecutar Trimmomatic
    trimmomatic PE -phred33 \
        "$R1" "$R2" \
        "$OUT_R1_PAIRED" "$OUT_R1_UNPAIRED" \
        "$OUT_R2_PAIRED" "$OUT_R2_UNPAIRED" \
        SLIDINGWINDOW:25:28 MINLEN:35 ILLUMINACLIP:TruSeq3-PE.fa:2:40:15

    echo "Muestra $base procesada y guardada en $TRIM"
done < "$SRA_LIST"
