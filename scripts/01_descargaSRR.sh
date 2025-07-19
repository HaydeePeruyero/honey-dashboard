#!/bin/bash

#Este script toma una lista de ids y descarga los archivos .fasta

# Entrada: 1.- El directorio donde se va a trabajar,
#         2.- El nombre del archivo que tien los ids, esta debera estar en la carpeta data
#         3.. El nombre de la carpeta donde se van a guardar las  secuencias en formato fastq,esta carpeta tambien debera estar en data

# Ejemplo de uso
#./01_descargarSRR.sh /data2/miel_dash data/ids_SRR_txt/id_Es_1.txt SRA_Es

# Directorios
HONEY="$1"
DATA="${HONEY}/data"

# Lista de IDs de SRA
SRA_LIST="${HONEY}/$2"

# Directorio de salida para los archivos fastq
OUTDIR="$DATA/$3"

# Crear el directorio de salida si no existe
 mkdir -p "$OUTDIR"

# Verificar permisos de escritura
for dir in "$DATA" "$OUTDIR"; do
    if [ ! -w "$dir" ]; then
        echo " No tienes permisos de escritura en '$dir'."
        exit 1
    fi
done

# Número de hilos a usar
THREADS=4

# Verificar si SRA Toolkit está instalado
if ! command -v prefetch &> /dev/null || ! command -v fasterq-dump &> /dev/null; then
    echo "Error: SRA Toolkit (prefetch y fasterq-dump) no están instalados o no están en el PATH"
    exit 1
fi

#
echo "SRA_LIST is set to: $SRA_LIST"
ls -1 "SRA_LIST"

# Descargar y convertir cada ID
while read -r SRA_ID; do
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
done < "$SRA_LIST"

echo "Descarga y conversión completadas."
