#!/bin/bash
# Crear los bioms

#Entradas
#         1.- Escribir el directorio principal
#         2.- Ingresar ruta a partir de la carpeta de resultados, seguida de la carpeta donde se encuentran la
#	      subcarpeta report, que es donde se encuentran los reportes que se van a utilizar para este script.
#         3._Ingresa el nombre de tu biom

# Ejemplo de uso:
# ./07_kraken_bioms.sh /data2/miel_dash 01_fastqc_Mex muestras_Mex_4_20

#Directorios
HONEY="$1"
TAXONOMY_READS="${HONEY}/results/$2/taxonomic_reads"
REPORT="$TAXONOMY_READS/report"
BIOMS="${TAXONOMY_READS}/bioms"
NOMBRE_OUTDIR="$3"

#crea el directorio de salida si no existe
mkdir -p "${BIOMS}"

#por facilidad se mueve a la carpeta donde estan los reports
cd "${REPORT}"

#Carpentries dice que lo corras con json, y para las libretas de cavana se corren con hdf5
kraken-biom *.report --fmt hdf5 -o "${BIOMS}/$3_hdf5.biom"
kraken-biom *.report --fmt json -o "${BIOMS}/$3_json.biom"

echo "Proceso terminado..."
