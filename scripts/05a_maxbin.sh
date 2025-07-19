#!/bin/bash

# Entrada:
# 1.- El directorio donde se va a trabajar
# 2.- El nombre de la subcarpeta de results donde se van a poner los outputs
# 3.- La direccion apartir de 1 donde están los IDs de las muestras

#Salida
# Las salidas se guardaran en una carpeta de metagenome_assembly llamada maxbin

#Ejemplo de uso:
#  /data2/miel_dash 01_fastqc_Es data/ids_SRR_txt/id_Es_Muestra.txt


# Directorios de entrada
HONEY="$1"
FASTQC="$HONEY/results/$2"
SRA_LIST="$HONEY/$3"

#Diretorios de trabajo
TRIM="$FASTQC/trim"
ASSEMBLY="$FASTQC/metagenome_assembly"

echo "SRA_LIST is set to: $SRA_LIST"
ls -1 "$SRA_LIST"

# Descargar y ensamblar cada ID
while read -r SRA_ID; do
    # Verificar si ya existe el directorio de ensamblaje
    if [ -f "${ASSEMBLY}/assembly_${SRA_ID}/maxbin/${SRA_ID}.log" ]; then
        echo "Ya existe output para ${SRA_ID}, se omite."
        continue
    fi

    # Leer paths de archivos .trim
    R1="${TRIM}/${SRA_ID}_R1.trim.fastq.gz"
    R2="${TRIM}/${SRA_ID}_R2.trim.fastq.gz"

    #El directorio de salida
    MAXBIN="${ASSEMBLY}/assembly_${SRA_ID}/maxbin"
    # Crear el directorio de salida
    mkdir -p "${MAXBIN}"

    # Verificar existencia de los archivos .contigs.fa
    if [ ! -d "${ASSEMBLY}/assembly_${SRA_ID}" ]; then
        echo "No se encontraron los archivos de lectura para ${SRA_ID} en ${ASSEMBLY}/assembly_${SRA_ID}"
        exit 1
    fi

    #se va a ocupar el cauntings que esta en ASSEMBLY
    COUTING="${ASSEMBLY}/assembly_${SRA_ID}/${SRA_ID}.contigs.fa"

    #verifica que si exista el archivo
    if [ ! -f "$COUTING" ]; then
    echo "No se encontró el archivo $COUTING para $SRA_ID"
    continue
    fi


    echo "Procesando $SRA_ID..."

    #es importante que en el -out agreges el prefijo que quieres que le agregue a los archivos
    run_MaxBin.pl -thread 12 -contig "${COUTING}" -reads "${R1}" -reads2 "$R2" -out "${MAXBIN}/${SRA_ID}"

    echo "Muestra $SRA_ID procesada y guardada en ${MAXBIN}"

done < "$SRA_LIST"

echo "Procesamiento completado."
