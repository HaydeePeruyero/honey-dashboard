#!/bin/bash

# Script para correr kroana sobre múltiples MAGs por muestra
# Entradas:
#   1.- Directorio base de trabajo
#   2.- Subdirectorio con los fastq
#   3.- Archivo con lista de IDs SRA

# Ejemplo:
# ./08_krona.sh
# /data2/miel_dash
# 01_fastqc_Mex
# data/ids_SRR_txt/id_Mex.txt
# 2> Archivos_Output_Mex/archivo_error_Paso08.txt
# &> Archivos_Output_Mex/archivo_output_Paso08.txt

# Variables de entrada
HONEY="$1"
FASTQC="$HONEY/$2"
SRA_LIST="$HONEY/$3"


# Directorios de trabajo
TAXONOMY_MAGS="$FASTQC/taxonomy_mags"
KRAKEN="$TAXONOMY_MAGS/kraken"
VISUALISATION="$TAXONOMY_MAGS/kroana_visualisation"
IMPUT="$VISUALISATION/imput"
HTML="$VISUALISATION/html"

# Verificar permisos de escritura
if [ ! -w "$TAXONOMY_MAGS" ]; then
   echo " No tienes permisos de escritura en '$TAXONOMY_MAGS'."
   exit 1
fi

# Crear directorios si no existen
mkdir -p "${VISUALISATION}" "${IMPUT}"

echo "SRA_LIST is set to: $SRA_LIST"
ls -1 "$SRA_LIST"

# Procesar cada ID de muestra
while read -r SRA_ID; do

    #checa si existe almenos un resultado 
    if [ ! -f "$KRAKEN/SRA_ID.001.kraken" ]; then 
        echo "No se encontraton archivos .kraken para ${SRA_ID}, se omite."
        continue
    fi

    echo "Procesando muestra: ${SRA_ID}"

    # Iterar sobre todos los elementos que corresponden a ese id
    for file in "$KRAKEN"/SRA_ID.*.kraken; do ##############

        # Extraer nombre base del archivo (ej: SRRXXXXXX.003)
        base_name=$(basename "$file".kraken)

        echo "  Procesando: $base_name"

        #preparando el input
        cut -f2,3 "${KRAKEN}/$file" > "${IMPUT}/${base_name}.krona.input"

        # Ejecutar Kroana
        ktImportTaxonomy "${IMPUT}/${base_name}.krona.input" \
                         -o "${HTML}/${base_name}.krona.out.html"
        
    done

    echo "Muestra ${SRA_ID} procesada y guardada en $TAXONOMY_MAGS"

done < "$SRA_LIST"
