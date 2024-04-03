# # Lager .png filer av opptaksområdene

aargang <- 2022

# +
utenhav <- FALSE

if (utenhav == TRUE) {
filsti_med_uten_hav <- "utenhav"
    } else if (utenhav == FALSE) {
  filsti_med_uten_hav <- "flate"
}

filsti_med_uten_hav

# +
sf::sf_use_s2(FALSE)
CRS <- 4326

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
library(sfarrow)    
        })

source("/home/jovyan/fellesr/R/DAPLA_funcs.R")

  rename_geometry <- function(g, name){
    current = attr(g, "sf_column")
    names(g)[names(g)==current] = name
    sf::st_geometry(g)=name
    g
  }

# +
arbeidsmappe_kart <- paste0("ssb-prod-dapla-felles-data-delt/GIS/Kart/", aargang, "/")

# grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_utenhav_", aargang, "/")

opptaksomrader_SOM_RHF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, "/")
opptaksomrader_SOM_RHF_filsti

opptaksomrader_SOM_HF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, "/")
opptaksomrader_SOM_HF_filsti

opptaksomrader_SOM_lokasjon_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", aargang, "/")
opptaksomrader_SOM_lokasjon_filsti

# +
opptaksomrader_SOM_RHF <- open_dataset(opptaksomrader_SOM_RHF_filsti) %>%
    sfarrow::read_sf_dataset()

opptaksomrader_SOM_HF <- open_dataset(opptaksomrader_SOM_HF_filsti) %>%
    sfarrow::read_sf_dataset()

opptaksomrader_SOM_lokasjon <- open_dataset(opptaksomrader_SOM_lokasjon_filsti) %>%
    sfarrow::read_sf_dataset()
# -

ggplot() + 
geom_sf(data = opptaksomrader_SOM_RHF)

ggplot() + 
geom_sf(data = opptaksomrader_SOM_HF)

ggplot() + 
geom_sf(data = opptaksomrader_SOM_lokasjon)
