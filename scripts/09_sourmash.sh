#!/bin/bash

# Script para sourmash en los ASSEMBLY filtrados

# Entradas:
#   1.- Directorio base de trabajo
#   2.- Subdirectorio de los resultados (ej 01_fastqc_Es)
#   3.- Subdirectorio de los mags        (ej Mags)
#   4.- Archivo con lista de IDs filtrados de los mags    (ej data/IDs_MAGS_filtados.txt
#   5.- Directorio con base de datos para sourmash /data2/anton/camda25/preprocessing/data/gtdb.sbt.zip

# Variables de entrada
HONEY="$1"
FASTQC="$HONEY/results/$2"
OUTDIR="$HONEY/results/$3"
FILTERED_LIST="$HONEY/$4"
SOURMASH_DB="$5"

# Directorios de trabajo
ASSEMBLY="$FASTQC/metagenome_assembly"
SKETCHES="$OUTDIR/sketches"
SEARCH="$OUTDIR/search"


# ===============================
# Validaciones
# ===============================

# 1. Validar base de datos sourmash
if [ -z "$SOURMASH_DB" ]; then
   echo "Error: No se proporcionó la base de datos de sourmash." >&2
   exit 1
fi

# 2. Validar existencia del archivo de lista
if [ ! -f "$FILTERED_LIST" ]; then
   echo "Error: No se encontró el archivo de lista $FILTERED_LIST"
   exit 1
fi
# 3. Verificar permisos de escritura en outdir
if [ ! -w "$OUTDIR" ]; then
   echo "Error: No se tiene permisos de escritura en $OUTDIR"
   exit 1
fi

# 4. Verificar permisos de lectura en metagenome_assembly
if [ ! -r "$ASSEMBLY" ] || [ ! -x "$ASSEMBLY" ]; then
   echo "Error: No se puede acceder (leer/entrar) al directorio $ASSEMBLY"
   exit 1
fi

# 5. Verificar permisos de escritura en ASSEMBLY
#    (crear si no existe y verificar)
mkdir -p "$SKETCHES" "$SEARCH"

if [ ! -w "$SKETCHES" ]; then
   echo "Error: No se tienen permisos de escritura en $SKETCHES"
   exit 1
fi
if [ ! -w "$SEARCH" ]; then
   echo "Error: No se tienen permisos de escritura en $SEARCH"
   exit 1
fi

# ===============================
# Procesamiento
# ===============================

echo "Empezando primer paso se corre sourmash sketch sobre los Ids de $FILTERED_LIST. "

while read -r FILTERED_ID; do

   echo "Procesando muestra: ${FILTERED_ID}"
   SAMPLE_ID=$( echo "$FILTERED_ID" | cut -d '.' -f1 )

   MAXBIN="${ASSEMBLY}/assembly_${SAMPLE_ID}/maxbin"
   if [ ! -d $MAXBIN ]; then
      echo "Directorio de $MAXBIN no encontrado para ${FILTERED_ID}, se omite."
      continue
   fi

   #busca si ya existe el outdir
   if [ -f "$SKETCHES/${FILTERED_ID}.sig" ]; then
      echo "Ya se proceso esta muestra ($FILTERED_ID), se omite."
      continue
   fi
   #busca si exista el .fasta
   if [ -f "$MAXBIN/$FILTERED_ID.fasta" ]; then
      echo "No se encontro $FILTERED_ID.fasta en $MAXBIN, se omite."
      continue
   fi
   
   sourmash sketch dna -o "$SKETCHES/${FILTERED_ID}.sig" \
                       -p 'k=31,scaled=1000,abund' "$MAXBIN/$FILTERED_ID.fasta"

   echo " ${FILTERED_ID} procesada y guardada en $SKETCHES"
   
done < "$FILTERED_LIST"

echo "Empezando el segundo paso se corre sourmash search sobre los Ids de $FILTERED_LIST. "

while read -r FILTERED_ID; do

   echo "Procesando muestra: ${FILTERED_ID}"

   #busca si ya existe el outdir
   if [ -f "$SEARCH/${FILTERED_ID}.csv" ]; then
      echo "Ya se procesó esta muestra ($FILTERED_ID), se omite."
      continue
   fi
   
   sourmash search "$SKETCHES/${FILTERED_ID}.sig" $SOURMASH_DB \
                   --threshold 0.1 -o "$SEARCH/${FILTERED_ID}.csv"


   echo " ${FILTERED_ID} procesada y guardada en $SEARCH"
   
done < "$FILTERED_LIST"

echo "Se concluyeron ambos pasos."
