
#!/bin/bash
# Title: Summary Concat
# Script para evaluar la calidad de las librerías de secuenciación con Trimmomatic Entrada:
#         1.- El directorio donde se va a trabajar
#         2.- El nombre de la carpeta donde se van a guardar los zips y los htmls
#         3.- El profijo que quieras que tenga el resultado

# Ejemplo de uso
# ./helper_summaryConcat.sh /data2/miel_dash 01_fastqc_Mex Mex

#Entradas
HONEY="$1"
RESULTS="${HONEY}/results"
FASTQC="${RESULTS}/$2"
PREFIJO="$3"

#Directorio de trabajo
ZIP="${FASTQC}/zip"
DOCS="${RESULTS}/docs"
SUMMARY="${RESULTS}/docs/summary"

# Crear carpetas si no existen
mkdir -p "$DOCS" "$SUMMARY" || echo "Failed to create directories"

#para cada .zip creamos se descomprime el archivo
for filename in "$ZIP"/*.zip
do
  unzip $filename
  # Derivar nombre base (ej: SRR27931732)
  base=$(basename "$filename" .zip)
  #mv -R "$ZIP/$base" "$SUMMARY/$base"
done
cat "$ZIP"/*/summary.txt > "$SUMMARY"/"$PREFIJO"_fastqc_summaries.txt
