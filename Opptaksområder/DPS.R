# -*- coding: utf-8 -*-
# # DPS

# +
sf::sf_use_s2(FALSE)
CRS <- 25833

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
        })
# -

opptaksomrader_DPS <- sfarrow::st_read_parquet("/home/jovyan/speshelse/Opptaksområder/opptaksomrader_DPS_DPS_2022.parquet")

colnames(opptaksomrader_DPS)
unique(opptaksomrader_DPS$OPPTAK)

# ### SSB fargepalett

ssb_farger <- klassR::GetKlass(614, output_style = "wide") %>%
  dplyr::rename(farge_nummer = code3, 
                HEX = name3, 
                farge = name2, 
                type = name1) %>%
  dplyr::select(-code1, -code2) %>%
  dplyr::filter(farge != "Hvit")

# +
pal_RHF <- leaflet::colorFactor(ssb_farger$HEX, domain = as.factor(opptaksomrader_DPS$OPPTAK))

opptaksomrader_DPS <- opptaksomrader_DPS %>%
sf::st_transform(crs = 4326)

opptaksomrader_DPS_leaflet <- leaflet::leaflet(options = leaflet::leafletOptions(zoomControl = FALSE)) %>% 
   leaflet::addTiles() %>%
   leaflet::addPolygons(stroke = F, data = opptaksomrader_DPS,
                       # color = "green",
                       weight = 1,
                       fillColor = pal_RHF(opptaksomrader_DPS$OPPTAK),
                       fillOpacity = 0.5, smoothFactor = 0.5,
                       popup = paste0("Opptaksområde: ", opptaksomrader_DPS$OPPTAK, " / Befolkning: ", prettyNum(opptaksomrader_DPS$PERSONER, big.mark = " ", scientific = FALSE))) %>%
  leaflet::addLegend("bottomright", pal = pal_RHF, values = as.factor(opptaksomrader_DPS$OPPTAK), opacity = 1)

# Lagrer filen
# htmlwidgets::saveWidget(opptaksomrader_RHF_leaflet, file = paste0(arbeidsmappe_opptak, "opptaksomrader_TSB_RHF_", aargang, ".html"), selfcontained=T)

opptaksomrader_DPS_leaflet
