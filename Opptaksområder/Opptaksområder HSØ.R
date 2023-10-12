# -*- coding: utf-8 -*-
aargang <- 2023

# +
utenhav <- TRUE

if (utenhav == TRUE) {
filsti_med_uten_hav <- "utenhav"
    } else if (utenhav == FALSE) {
  filsti_med_uten_hav <- "flate"
}   

filsti_med_uten_hav

sf::sf_use_s2(FALSE)
CRS <- 4326

# +
# renv::load()
renv::autoload()

suppressPackageStartupMessages({ 
library(arrow)
library(sf)
library(sfarrow)
library(tidyverse)
library(readxl)
library(klassR)
library(leaflet)
library(sfarrow)  
library(cowplot)
library(fellesr)
        })

# OBS: legg til open_dataset under NAMESPACE + export i fellesr
# source("/home/jovyan/fellesr/R/DAPLA_funcs.R")

  rename_geometry <- function(g, name){
    current = attr(g, "sf_column")
    names(g)[names(g)==current] = name
    sf::st_geometry(g)=name
    g
  }

# +
arbeidsmappe_kart <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/")

opptaksomrader_SOM_HF_filsti <- paste0(arbeidsmappe_kart, "opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, ".parquet")

# # grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_utenhav_", aargang, "/")

# opptaksomrader_SOM_RHF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet")
# opptaksomrader_SOM_RHF_filsti

# opptaksomrader_SOM_HF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, ".parquet")
# opptaksomrader_SOM_HF_filsti

# opptaksomrader_SOM_lokasjon_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", aargang, ".parquet")
# opptaksomrader_SOM_lokasjon_filsti

# +
# opptaksomrader_SOM_RHF <- fellesr::read_SSB(opptaksomrader_SOM_RHF_filsti, sf = TRUE) 
# sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

opptaksomrader_SOM_HF <- fellesr::read_SSB(opptaksomrader_SOM_HF_filsti, sf = TRUE) 
# sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

# opptaksomrader_SOM_lokasjon <- fellesr::read_SSB(opptaksomrader_SOM_lokasjon_filsti, sf = TRUE) 
# sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)
# -

opptaksomrader_SOM_HF_H12 <- opptaksomrader_SOM_HF %>%
filter(NAVN_RHF == "HELSE SØR-ØST RHF")

# +
ssb_farger <- klassR::GetKlass(614, output_style = "wide") %>%
  dplyr::rename(farge_nummer = code3, 
                HEX = name3, 
                farge = name2, 
                type = name1) %>%
  dplyr::select(-code1, -code2) %>%
  dplyr::filter(farge != "Hvit") %>%
add_row(farge_nummer = "Lilla 1.1", HEX = "#d9d1e5", farge = "Lilla", type = "egen") %>% #  
add_row(farge_nummer = "Lilla 2.1", HEX = "#7b68ad", farge = "Lilla", type = "egen") %>% #  
add_row(farge_nummer = "Lilla 3.1", HEX = "#9f8ec1", farge = "Lilla", type = "egen")
 
# ssb_farger

ssb_farger_blaa <- ssb_farger %>%
dplyr::filter(farge_nummer %in% c("SSB Blå 2"))

# ssb_farger_blaa_lys <- ssb_farger %>%
# dplyr::filter(farge_nummer %in% c("SSB Blå 2"))
# -

# ## HSØ

# +
HSØ <- ggplot2::ggplot() + 
ggplot2::geom_sf(data = opptaksomrader_SOM_HF_H12, fill = ssb_farger_blaa$HEX, color = "white", lwd = 1) +
ggplot2::theme_void()

HSØ
# -

unique(opptaksomrader_SOM_HF_H12$NAVN_HF)

# ## Kart per HF

# +
# for (i in unique(opptaksomrader_SOM_HF_H12$NAVN_HF)){

# # i <- "VESTRE VIKEN HF"

# valgt_HF <- opptaksomrader_SOM_HF_H12 %>%
# filter(NAVN_HF == i)

# VV_resten <- opptaksomrader_SOM_HF_H12 %>%
# filter(NAVN_HF != i)

# kart <- ggplot2::ggplot() + 
# ggplot2::geom_sf(data = VV_resten, fill = "grey85", color = "white", lwd = 0.3) +
# ggplot2::geom_sf(data = valgt_HF, fill = ssb_farger_blaa$HEX, color = "white", lwd = 0.3) +
# ggplot2::theme_void()
  
# ggplot2::ggsave(kart, file=paste0("/ssb/bruker/rdn/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_HSØ_", aargang, "_", i, ".png"), 
#        width = 10, height = 10, units = "cm")

# # kart
# # png(filename = paste0("/ssb/bruker/rdn/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_HSØ_", aargang, "_", i, ".png"), width = 2000, height = 2000)
# # kart
# # dev.off()
    
# }
# -

# ## OUS, LDS, DS og Ahus 

# +
valgt_HF <- opptaksomrader_SOM_HF_H12 %>%
filter(NAVN_HF %in% c("OSLO UNIVERSITETSSYKEHUS HF", "LOVISENBERG DIAKONALE SYKEHUS AS", "DIAKONHJEMMET SYKEHUS AS", "AKERSHUS UNIVERSITETSSYKEHUS HF"))

VV_resten <- opptaksomrader_SOM_HF_H12 %>%
filter(!NAVN_HF %in% c("OSLO UNIVERSITETSSYKEHUS HF", "LOVISENBERG DIAKONALE SYKEHUS AS", "DIAKONHJEMMET SYKEHUS AS", "AKERSHUS UNIVERSITETSSYKEHUS HF"))


kart <- ggplot2::ggplot() + 
ggplot2::geom_sf(data = VV_resten, fill = "grey85", color = "white", lwd = 0.3) +
ggplot2::geom_sf(data = valgt_HF, fill = ssb_farger_blaa$HEX, color = "white", lwd = 0.3) +
ggplot2::theme_void()

ggplot2::ggsave(kart, file=paste0("/ssb/bruker/rdn/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_HSØ_", aargang, "_OUS_LDS_DS_Ahus", ".png"), 
       width = 10, height = 10, units = "cm")
# -

kart
