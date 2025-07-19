#!/bin/bash

# Script para kraken

# Trimmomatic Entrada:
#         1.- El directorio donde se va a trabajar, se asume que todos los demas imputs parte de este directorio
#         2.- El directorio donde esta lo del paso los fastq, debe tener una subcarpeta que se llametrim
#         3.. El directorio de la carpeta donde esta la lista de los SRR que se van a correr
#         4._ El directorio de la base de datos en este caso /data2/haydee/kraken_PFP

# Ejemplo de uso
#./06a_taxonomic_asignment_PFP.sh \
# /data2/miel_dash 01_fastqc_Mex \
# data/ids_SRR_txt/id_Mex.txt \
# /data2/haydee/kraken_PFP \
# 2> Archivos_Output_Mex/archivo_error_Paso06a.txt \
# &> Archivos_Output_Mex/archivo_output_Paso06a.txt

#Directorios de entrada
HONEY="$1"
FASTQC="$HONEY/results/$2"
TRIM="${FASTQC}/trim"

#lista de ids de SRA
SRA_LIST="$HONEY/$3"

#Directorio de salida
TAXONOMY_READS="${FASTQC}/taxonomic_reads"
KRAKEN="$TAXONOMY_READS/kraken"
REPORT="$TAXONOMY_READS/report"

# Crear carpetas si no existen
mkdir -p "$TAXONOMY_READS" "$KRAKEN" "$REPORT"

# Verificar permisos de escritura
for dir in "$FASTQC" "$TAXONOMY_READS" "$KRAKEN" "$REPORT"; do
    if [ ! -w "$dir" ]; then
        echo " No tienes permisos de escritura en '$dir'."
        exit 1
    fi
done

# Asignacion de base de datos
kdat="$4"
if [ -z "$kdat" ]; then
    echo "Error: No se proporcionó la base de datos de Kraken." >&2
    exit 1
fi


# Loop para procesar todos los archivos _1.fastq.gz (pares)
while read -r base; do

    #verificar que no existan ya los outputs
    if [ -f "$KRAKEN/$base.kraken" ]; then
         echo "Ya existe $zip_result, se omite."
         continue
    fi

    # leer aids de archivo SRA_LIST (ej: SRR27931732)
    R1="${TRIM}/${base}_R1.trim.fastq.gz"
    R2="${TRIM}/${base}_R2.trim.fastq.gz"

    # Verica que exista el .trim
    if [ ! -f "$R1" ]; then
        echo "No se encontró el archivo ${base}_1.trim.fastq.gz en $TRIM"
        exit 1
    fi

    echo "Procesando muestra: $base"

    # crea el archivo .kraken y .report
    kraken2 --db "$kdat" --threads 10 --paired "$R1" "$R2" --output "$KRAKEN/$base.kraken" --report "$REPORT/$base".report


    echo "Muestra $base procesada y guardada en $TAXONOMY_READS"

done < "$SRA_LIST"
