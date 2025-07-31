#----
library(readxl)
library(tidygeocoder)
library(tidyverse)
library(readr)
library("ggplot2")
library("ggspatial")
library("sf")
library("rnaturalearth")
library("rnaturalearthdata")
library(maps)
library(here)

metadata <- read_csv(here::here("data/tables","metadata_Es_Shotgun_Mex_ordenado.csv"))

unique_cities <- metadata %>%
  distinct(City, Longitud, Latitud, Specie)


# Cargar mapa mundial
world_map <- map_data("world")

# Mapa con puntos coloreados por especie y leyenda a la derecha
mapa <- ggplot() +
  geom_polygon(data = world_map, aes(x = long, y = lat, group = group),
               fill = "lightgrey", color = "white") +
  geom_point(data = unique_cities,
             aes(x = Longitud, y = Latitud, color = Specie),
             size = 1.5) +
  scale_color_manual(values = c(
    "Melipona beecheii" = "#b8860b",  # Golden
    "Scaptotrigona mexicana" = "#FFBF00",  # Amber
    "Apis mellifera" = "#CC79A7",     # naranja
    "Spanish PDO" = "#56B4E9",        # azul claro
    "Not collected" = "#009E73"
  ))+
  coord_fixed(xlim = c(-150, 150), ylim = c(-50, 80)) + 
  theme_void() +
  labs(title = "Sample Locations by Bee Species", color = "Bee Species") +
  theme(
    plot.title = element_text(hjust = 0.5, size = 16, face = "bold"),
    legend.title = element_text(size = 14, face = "bold"),
    legend.text = element_text(size = 12),
    legend.position = "right",  # Coloca la leyenda a la derecha del mapa
    text = element_text(size = 16)
  )

mapa

# Guardar como imagen
ggsave("results/world_map_by_species.png", plot = mapa, width = 12, height = 8, dpi = 300)

# También como SVG si es necesario
ggsave("results/world_map_by_species.svg", plot = mapa, device = "svg", width = 12, height = 8)

