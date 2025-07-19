#!/bin/bash

# Entrada:
# 1.- El directorio donde se va a trabajar
# 2.- El subdirectorio donde están las carpetas de assemmbly_SRR
# 3.- El archivo donde están los IDs de los objetos SRA

#Salida
#  Los resultados serán guardados en una subcarpeta de $4 que se va a llamar checkm

# Ejemplo de uso
# ./05b_checkm.sh
# /data2/miel_dash
# 01_fastqc_Mex
# data/ids_SRR_txt/id_Mex.txt
# 2> Archivos_Output_Mex/archivo_error_Paso05b.txt
# &> Archivos_Output_Mex/archivo_output_Paso05b.txt

# Directorios
HONEY="$1"
ASSEMBLY="$HONEY/results/$2/metagenome_assembly"
SRA_LIST="$HONEY/$3"

# Verificar permisos de escritura
if [ ! -w "$ASSEMBLY" ]; then
   echo " No tienes permisos de escritura en '$ASSEMBLY'."
   exit 1
fi

echo "SRA_LIST is set to: $SRA_LIST"
ls -1 "$SRA_LIST"

# Procesar cada muestra en la lista de ids
while read -r SRA_ID; do
    # Verificar si ya existe el directorio de ensamblaje
    if [ -d "${ASSEMBLY}/assembly_${SRA_ID}/checkm/quality_${SRA_ID}.tsv" ]; then
        echo "Ya existe output para ${SRA_ID}, se omite."
        continue
    fi
    #Directorio donde estan los mags
    MAXBIN="${ASSEMBLY}/assembly_${SRA_ID}/maxbin"

    # Verificar existencia de los directorios assembly
    if [ ! -d "${MAXBIN}" ]; then
        echo "No se encontraron los archivos de lectura para ${SRA_ID} en ${MAXBIN}"
        exit 1
    fi
    #El directorio de salida
    CHECKM="${ASSEMBLY}/assembly_${SRA_ID}/checkm"
    # Crear el directorio de salida
    mkdir -p "${CHECKM}"

    echo "Procesando ${SRA_ID}..."

    checkm taxonomy_wf -t 12 domain Bacteria -x fasta "${MAXBIN}" "${CHECKM}"
    checkm qa "${CHECKM}"/Bacteria.ms "${CHECKM}" --file "${CHECKM}"/quality_"${SRA_ID}".tsv --tab_table -o 2

    echo "Muestra $SRA_ID procesada y guardada en $ASSEMBLY"

done < "$SRA_LIST"

echo "Procesamiento completado."
