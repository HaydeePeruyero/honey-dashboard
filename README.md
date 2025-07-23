# 🐝 Honey Microbiome Preprocessing Pipeline

This repository contains the preprocessing scripts, metadata files, and intermediate results used to analyze metagenomic (and future 16S rRNA) data from stingless bee honeys, as part of the **Honey Microbiome Explorer** project.

The data include both public and private sources and focus on characterizing microbial communities, biosynthetic clusters, and antibiotic resistance genes associated with native stingless bee honeys from the Yucatán Peninsula and beyond.

---

## 📁 Repository structure

```bash
├── data/                # Processed metadata and intermediate files
├── scripts/             # Preprocessing scripts (quality control, annotation, filtering, etc.)
├── results/             # Aggregated outputs used for visualization
└── README.md
```


---

## 🚀 Project goals

- 🧹 Clean, annotate, and harmonize metagenomic data from multiple studies. 
- 🔬 Integrate both shotgun metagenomes and 16S datasets.
- 📊 Enable interactive exploration through the **Honey Microbiome Explorer** dashboard.  
- 🧪 Highlight relevant findings in microbial diversity, metabolic potential, and host specificity.  

---

## 🧬 Data sources

- Public metagenomic datasets (e.g. NCBI BioProjects such as `PRJNA662672`, others to be added)
- Private datasets (included with permission)
- Associated metadata on:
  - Host species  
  - Geographical origin  
  - Floral sources  
  - Sampling and sequencing methods  

---

## 👩‍🔬 Project lead

**Haydeé Peruyero**  
Centro de Ciencias Matemáticas, UNAM – SECIHTI  
🔗 [Personal page](https://haydeeperuyero.github.io/)

---

## 🤝 Collaborators


- Gabriela Itavii Ramírez Ferrin, Universidad Tecnológica de la Mixteca  (Preprocessing)
- Juan Manuel, UMSNH  (Automatization)
- Nelly Sélem Mojica, CCM, UNAM
- Aurora Xolalpa Aroche, Universidad

---

## 🛠️ Requirements & tools

Most scripts are written in **R** and **Bash**. Dependencies are listed in each script or in a future `requirements.txt` file.

Key tools used include:

- `fastc`, `trimmomatic`, `megahit`, `MAXBIN`, `CHECKM`, `kraken2`, `krona`   
- `phyloseq`, `vegan` (R packages)  
- Custom scripts for data cleaning, filtering, and formatting

---

## 📊 Dashboard (coming soon)

The interactive **Honey Microbiome Explorer** dashboard is currently under development and will be available here:  
🌐 

Planned features:

- Interactive alpha and beta diversity visualizations.  
- Taxonomic and functional composition plots.  
- Filtering by host, region, or other metadata.  
- Access to summary tables and downloadable datasets.  

---

## 📄 License

This project is licensed under the [MIT License](https://opensource.org/license/mit) license.

---

## 📌 Citation

If you use this repository or data in your research, please cite:

> Haydeé Peruyero, et al. (2025). *Honey Microbiome Explorer Project – Preprocessing Scripts and Data*. GitHub repository: [https://github.com/HaydeePeruyero/honey-dashboard](https://github.com/HaydeePeruyero/honey-dashboard)

---

## 💬 Contact

For questions, suggestions, or collaboration inquiries, please open an issue or contact:  
🔗 [https://github.com/HaydeePeruyero]

