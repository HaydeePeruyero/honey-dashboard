#!/bin/bash

# Script para concatenar archivos quality*_con_id.tsv en un solo archivo final

# Entrada:
# 1.- El directorio donde se va a trabajar
# 2.- El nombre de la subcarpeta de resultados donde se van a poner los outputs
# 3.- El SUFIJO que se le va a poner al archivo

# Ejemplo:
# ./helper_checkmConcat.sh /data2/miel_dash 01_fastqc_Mex Mex

#Definir el directorio de trabajo
HONEY="$1"
FASTQC="${HONEY}/results/$2"
ASSEMBLY="${FASTQC}/metagenome_assembly"
SUFIJO="$3"

# Entrar al directorio
cd "${ASSEMBLY}" || { echo "No se pudo acceder al directorio ${ASSEMBLY}"; exit 1; }

#archivo final concatenado
OUTPUT="${HONEY}/results/docs/final_quality_${SUFIJO}.tsv"

#Corrobora que no exista el output para evitar sobre escribir cosas
if [ -f "${OUTPUT}" ]; then
  echo "final_quality_${SUFIJO}.tsv ya es un archivo, se para el proceso."
fi

#Buscar archivos a concatenar
FILES=(assembly_*/checkm/quality_*.tsv)

# Verificar si hay archivos que coincidan
if [ "${#FILES[@]}" -eq 0 ]; then
    echo "No se encontraron archivos quality*.tsv en subdirectorios."
fi

# Obtener el encabezado desde el primer archivo y guardarlo
head -n 1 "${FILES[0]}" > "$OUTPUT"

# Concatenar todos los archivos excluyendo su primera línea
tail -n +2 -q "${FILES[@]}" >> "$OUTPUT"

echo "Concatenación completada. Archivo generado: $OUTPUT"
