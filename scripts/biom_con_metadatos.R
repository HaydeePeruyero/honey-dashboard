---
#  titulo:Exploracion taxonomica de los datos
---

library("phyloseq")
library("ggplot2")
library("RColorBrewer")
library("patchwork")
library("readr")
library("tidyverse")
library("grid")
library("gridExtra")
library("paletteer")
library("forcats")
library(plyr)
library(reshape2)
library(vegan)
library(car) # for levenetest
library(multcompView)
library(FSA) # duntest

# read the biom file
raw_metagenomes <- import_biom("/home/alumno59/estancia/honey-dashboard/data/honey_4_20_Es_Shotgun_Mexico_json.biom")

# short the names, quit the first 4 characteres
raw_metagenomes@tax_table@.Data <- substring(raw_metagenomes@tax_table@.Data, 4)
#View(raw_metagenomes@tax_table@.Data)

# rename the columns
colnames(raw_metagenomes@tax_table@.Data)<- c("Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species")
#View(raw_metagenomes@tax_table@.Data)

# upload the metadata
df_meta <- read_tsv("/home/alumno59/estancia/honey-dashboard/data/metadata_Es_Shotgun_Mex.tsv")

# rename the names for the Mex samples
# change n_specie to specie_n
df_meta <- df_meta %>%
  mutate(SampleID = case_when(
    grepl("_Scapto", ID) ~ paste0("Scapto_", sub("_Scapto.*", "", ID)),
    grepl("_Melli", ID) ~ paste0("Meli_", sub("_Melli.*", "", ID)),
    TRUE ~ as.character(ID) 
  ))

# change the metadata name of samples to construct a new biom file
df_metadata <- sample_data(df_meta)
rownames(df_metadata) <- df_meta$ID

# construc the new biom file with the new names of the samples
all_data <- merge_phyloseq(raw_metagenomes, sample_data(df_metadata))

# Obtener los nombres de las muestras
sample_names <- sample_names(all_data)

# Cambiar nombres con "_Melli" a "M_número"
sample_names <- gsub("(\\d+)_Melli", "Meli_\\1", sample_names)

# Cambiar nombres con "_Scapto" a "S_número"
sample_names <- gsub("(\\d+)_Scapto", "Scapto_\\1", sample_names)

# Asignar los nuevos nombres de las muestras al objeto phyloseq
sample_names(all_data) <- sample_names

# Verificar los nuevos nombres de las muestras
#sample_names(all_data)