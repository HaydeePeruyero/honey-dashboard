#!/bin/bash

# Script para Kraken sobre múltiples MAGs por muestra

# Entradas:
#   1.- Directorio base de trabajo
#   2.- Subdirectorio de los resultados (ej 01_fastqc_Es)
#   3.- Archivo con lista de IDs SRA    (ej data/ids_SRR_txt/id_Es_1.txt)
#   4.- Directorio con base de datos de Kraken (ej. /data2/haydee/kraken_PFP)

# Variables de entrada
HONEY="$1"
FASTQC="$HONEY/results/$2"
SRA_LIST="$HONEY/$3"
KRAKEN_DB="$4"

# Directorios de trabajo
TAXONOMY_READS="$FASTQC/taxonomy_reads"
ASSEMBLY="$FASTQC/metagenome_assembly"
TAXONOMY_MAGS="$FASTQC/taxonomy_mags"
KRAKEN="$TAXONOMY_MAGS/kraken"
REPORT="$TAXONOMY_MAGS/report"

# ===============================
# Validaciones
# ===============================

# 1. Validar base de datos Kraken
if [ -z "$KRAKEN_DB" ]; then
   echo "Error: No se proporcionó la base de datos de Kraken." >&2
   exit 1
fi

# 2. Validar existencia del archivo de lista
if [ ! -f "$SRA_LIST" ]; then
   echo "Error: No se encontró el archivo de lista $SRA_LIST"
   exit 1
fi

# 3. Verificar permisos de lectura en metagenome_assembly
if [ ! -r "$ASSEMBLY" ] || [ ! -x "$ASSEMBLY" ]; then
   echo "Error: No se puede acceder (leer/entrar) al directorio $ASSEMBLY"
   exit 1
fi

# 4. Verificar permisos de escritura en taxonomy_mags
#    (crear si no existe y verificar)
mkdir -p "$TAXONOMY_MAGS" "$KRAKEN" "$REPORT"

if [ ! -w "$TAXONOMY_MAGS" ]; then
   echo "Error: No se tienen permisos de escritura en $TAXONOMY_MAGS"
   exit 1
fi

# ===============================
# Procesamiento
# ===============================

while read -r SRA_ID; do
   MAXBIN="${ASSEMBLY}/assembly_${SRA_ID}/maxbin"
   if [ ! -d "$MAXBIN" ]; then
      echo "Directorio de $MAXBIN no encontrado para ${SRA_ID}, se omite."
      continue
   fi

   echo "Procesando muestra: ${SRA_ID}"

   # Iterar sobre todos los .fasta de MaxBin
   for fasta_file in "$MAXBIN"/*.fasta; do
      [ ! -f "$fasta_file" ] && continue

      base_name=$(basename "$fasta_file" .fasta)
      echo "Procesando bin: $base_name"

     #busca si ya existe el outdir
      if [ -f "$KRAKEN/${base_name}.kraken" ]; then
         echo "Ya se procesó esta muestra ($base_name), se omite."
         continue
      fi

      #corriendo kraken2
      kraken2 --db "$KRAKEN_DB" \
        --threads 12 \
        --input "$fasta_file" \
        --output "$KRAKEN/${base_name}.kraken" \
        --report "$REPORT/${base_name}.report"
   done

   echo "Muestra ${SRA_ID} procesada y guardada en $TAXONOMY_MAGS"
done < "$SRA_LIST"
