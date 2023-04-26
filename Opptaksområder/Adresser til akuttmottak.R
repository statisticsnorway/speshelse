# -*- coding: utf-8 -*-
# # Adresser til akuttmottak
#
# OBS: fungerer kun på DAPLA (trenger nettilgang)

# +
library(tidyverse)

source("/home/jovyan/fellesr/R/DAPLA_funcs.R")
source("/home/jovyan/GISSB/R/address_to_coords.R")

# +
akuttmottak <- readxl::read_excel("/home/jovyan/Akuttmottak.xlsx") %>%
dplyr::mutate(F_POSTNR = stringr::str_pad(F_POSTNR, width = 4, "left", pad = "0"), 
             F_ADRESSE1 = case_when(
             F_ADRESSE1 == "Vefsnvegen 25-27" ~ "Vefsnvegen 25", 
                  F_ADRESSE1 == "Bugårdsgata 98" ~ "Bugårdsgata 9B", 
                 F_ADRESSE1 == "Kirkegata 2" ~ "Kirkegata 2C",
                 TRUE ~ F_ADRESSE1
             ), 
              
             F_POSTNR = case_when(
             F_ADRESSE1 == "Kirkegata 2C" ~ "7600", 
                 TRUE ~ F_POSTNR
             ))
akuttmottak

adresser <- akuttmottak$F_ADRESSE1
postnummer <- akuttmottak$F_POSTNR

akuttmottak_coords <- address_to_coords(address = adresser,
                         zip_code = postnummer, 
                                        crs_out = 4326,
                                       format = "data.frame")

akuttmottak_coords <- cbind(akuttmottak_coords, akuttmottak)

# +
# ggplot() + 
# geom_sf(data = akuttmottak_coords)
# -

leaflet::leaflet(akuttmottak_coords) %>% 
leaflet::addTiles() %>% 
leaflet::addMarkers(popup = ~ NAVN_BEDRIFT)

write_SSB(akuttmottak_coords, "ssb-prod-spesh-personell-data-kilde/akuttmottak.csv")

akuttmottak_coords
