# -*- coding: utf-8 -*-
# # Karttest

# Sjekker om kartene blir like tvers av årganger.

# +
sf::sf_use_s2(FALSE)

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
        })

source("/home/rdn/fellesr/R/DAPLA_funcs.R")

tjeneste <- "TSB"

aargang <- 2022
aargang_t1 <- aargang-1

arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"

arbeidsmappe_kart <- paste0(arbeidsmappe, "kart/", aargang, "/")
arbeidsmappe_kart_t1 <- paste0(arbeidsmappe, "kart/", aargang_t1, "/")

arbeidsmappe_opptak <- paste0(arbeidsmappe, "opptaksomrader/", aargang, "/")
arbeidsmappe_opptak_t1 <- paste0(arbeidsmappe, "opptaksomrader/", aargang_t1, "/")
# -

# # RHF

# +
opptaksomrade_RHF_filsti <- paste0(arbeidsmappe_opptak, "opptaksomrader_", tjeneste, "_RHF_", aargang, ".parquet")
opptaksomrade_RHF_t1_filsti <- paste0(arbeidsmappe_opptak_t1, "opptaksomrader_", tjeneste, "_RHF_", aargang_t1, ".parquet")

# # Lese inn filen som parquet med sfarrow
opptaksomrade_RHF <- sfarrow::st_read_parquet(opptaksomrade_RHF_filsti)
opptaksomrade_RHF_t1 <- sfarrow::st_read_parquet(opptaksomrade_RHF_t1_filsti)

colnames(opptaksomrade_RHF)

unique(opptaksomrade_RHF_t1$NAVN_RHF)
# -

# ## Helse Vest

# +
sf::st_crs(opptaksomrade_RHF)==sf::st_crs(opptaksomrade_RHF_t1)

opptaksomrade_RHF_1 <- opptaksomrade_RHF %>%
dplyr::filter(NAVN_RHF == "HELSE VEST RHF")

opptaksomrade_RHF_t1_1 <- opptaksomrade_RHF_t1 %>%
dplyr::filter(NAVN_RHF == "HELSE VEST RHF")

HELSE_VEST <- sf::st_difference(opptaksomrade_RHF_1, opptaksomrade_RHF_t1_1)

ggplot() + 
geom_sf(data = HELSE_VEST)

sf::st_area(HELSE_VEST)
# -

# ## Helse Nord RHF

# +
opptaksomrade_RHF_1 <- opptaksomrade_RHF %>%
dplyr::filter(NAVN_RHF == "HELSE NORD RHF")

opptaksomrade_RHF_t1_1 <- opptaksomrade_RHF_t1 %>%
dplyr::filter(NAVN_RHF == "HELSE NORD RHF")

HELSE_NORD <- sf::st_difference(opptaksomrade_RHF_1, opptaksomrade_RHF_t1_1)

ggplot() + 
geom_sf(data = HELSE_NORD)

sf::st_area(HELSE_NORD)
# -

# ## Helse Sør-Øst

# +
opptaksomrade_RHF_1 <- opptaksomrade_RHF %>%
dplyr::filter(NAVN_RHF == "HELSE SØR-ØST RHF")

opptaksomrade_RHF_t1_1 <- opptaksomrade_RHF_t1 %>%
dplyr::filter(NAVN_RHF == "HELSE SØR-ØST RHF")

HELSE_SOR_OST <- sf::st_difference(opptaksomrade_RHF_1, opptaksomrade_RHF_t1_1)

ggplot() + 
geom_sf(data = HELSE_SOR_OST)

nrow(HELSE_SOR_OST)
sf::st_area(HELSE_SOR_OST)
# -

# ## Helse Midt-Norge

# +
opptaksomrade_RHF_1 <- opptaksomrade_RHF %>%
dplyr::filter(NAVN_RHF == "HELSE MIDT-NORGE RHF")

opptaksomrade_RHF_t1_1 <- opptaksomrade_RHF_t1 %>%
dplyr::filter(NAVN_RHF == "HELSE MIDT-NORGE RHF")

HELSE_MIDT <- sf::st_difference(opptaksomrade_RHF_1, opptaksomrade_RHF_t1_1)

ggplot() + 
geom_sf(data = HELSE_MIDT)

nrow(HELSE_MIDT)
sf::st_area(HELSE_MIDT)
# -

# # HF

# +
opptaksomrade_HF_filsti <- paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_HF_", aargang, ".parquet")
opptaksomrade_HF_t1_filsti <- paste0(arbeidsmappe_opptak_t1, "opptaksomrader_SOM_HF_", aargang_t1, ".parquet")

# # Lese inn filen som parquet med sfarrow
opptaksomrade_HF <- sfarrow::st_read_parquet(opptaksomrade_HF_filsti)
opptaksomrade_HF_t1 <- sfarrow::st_read_parquet(opptaksomrade_HF_t1_filsti)

unique(opptaksomrade_HF_t1$NAVN_HF)

HF <- "UNIVERSITETSSYKEHUSET NORD-NORGE HF"

opptaksomrade_HF_1 <- opptaksomrade_HF %>%
dplyr::filter(NAVN_HF == HF)

opptaksomrade_HF_t1_1 <- opptaksomrade_HF_t1 %>%
dplyr::filter(NAVN_HF == HF)

HF <- sf::st_difference(opptaksomrade_HF_1, opptaksomrade_HF_t1_1)

ggplot() + 
geom_sf(data = HF)

HF$area <- sf::st_area(HF)

HF %>%
data.frame()

nrow(HF)
# -

# # Lokasjon

# +
if (tjeneste == "SOM") {
opptaksomrade_lokasjon_filsti <- paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_lokasjon_", aargang, ".parquet")
opptaksomrade_lokasjon_t1_filsti <- paste0(arbeidsmappe_opptak_t1, "opptaksomrader_SOM_lokasjon_", aargang_t1, ".parquet")

# # Lese inn filen som parquet med sfarrow
opptaksomrade_lokasjon <- sfarrow::st_read_parquet(opptaksomrade_lokasjon_filsti)
opptaksomrade_lokasjon_t1 <- sfarrow::st_read_parquet(opptaksomrade_lokasjon_t1_filsti)

unique(opptaksomrade_lokasjon$OPPTAK)
    }

if (tjeneste == "SOM") {

lokasjon <- "Narvik"

opptaksomrade_lokasjon_1 <- opptaksomrade_lokasjon %>%
dplyr::filter(OPPTAK == lokasjon)

opptaksomrade_lokasjon_t1_1 <- opptaksomrade_lokasjon_t1 %>%
dplyr::filter(OPPTAK == lokasjon)

lokasjon <- sf::st_difference(opptaksomrade_lokasjon_1, opptaksomrade_lokasjon_t1_1)

ggplot() + 
geom_sf(data = lokasjon)

sf::st_area(lokasjon)
    }

if (tjeneste == "SOM") {

lokasjon_2 <- "Bodø"

opptaksomrade_lokasjon_1 <- opptaksomrade_lokasjon %>%
dplyr::filter(OPPTAK == lokasjon_2)

opptaksomrade_lokasjon_t1_1 <- opptaksomrade_lokasjon_t1 %>%
dplyr::filter(OPPTAK == lokasjon_2)

lokasjon_2 <- sf::st_difference(opptaksomrade_lokasjon_1, opptaksomrade_lokasjon_t1_1)

ggplot() + 
geom_sf(data = lokasjon_2)

sf::st_area(lokasjon_2)
    }
# -

# # Laster inn bereg

# +
# bereg_filsti <- paste0("/ssb/stamme04/bereg/person/wk24/bosatte_koorfil_g", aargang, "m01d01eslep.sas7bdat")

# bosatte_koorfil <- haven::read_sas(bereg_filsti)

# bosatte_koorfil <- bosatte_koorfil %>%
#   dplyr::mutate(GRUNNKRETSNUMMER = paste0(KOMMNR, gkrets),
#                 XY = paste0(X_KOORDINAT, ", ", Y_KOORDINAT)) %>%
#   dplyr::mutate(ID = paste0(KOMMNR, "-",  # Lager ID
#                             GATENR_GAARDSNR, "-",
#                             HUSNR_BRUKSNR, "-",
#                             BOKSTAV_FESTENR, "-",
#                             XY)) %>%
#   dplyr::filter(!is.na(X_KOORDINAT), # Sletter personer uten adressekoordinater
#                 !is.na(Y_KOORDINAT))

# opptaksomrader_KLASS <- klassR::GetKlass(629, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
# dplyr::rename(GRUNNKRETSNUMMER = code4, 
#              GRUNNKRETS_NAVN = name4, 
#              OPPTAK_NUMMER = code3, 
#              OPPTAK = name3, 
#              ORGNR_HF = code2, 
#              NAVN_HF = name2, 
#              ORGNR_RHF = code1, 
#              NAVN_RHF = name1)

# nrow(opptaksomrader_KLASS)

# opptaksomrader_KLASS_2 <- opptaksomrader_KLASS %>%
# dplyr::filter(OPPTAK %in% c("Bodø", "Narvik"))

# bosatte_koorfil_2 <- bosatte_koorfil %>%
# dplyr::filter(GRUNNKRETSNUMMER %in% unique(opptaksomrader_KLASS_2$GRUNNKRETSNUMMER))

# class(bosatte_koorfil_2)
# nrow(bosatte_koorfil_2)

# bosatte_koorfil_2 <- bosatte_koorfil_2 %>%
#   dplyr::rename(X = X_KOORDINAT, 
#                 Y = Y_KOORDINAT) %>%
#   dplyr::mutate(x = as.numeric(as.character(X)),
#                 Y = as.numeric(as.character(Y))) %>%
#   sf::st_as_sf(coords = c("Y", "X"), crs = 25833) %>%
# dplyr::select(-x)

# class(bosatte_koorfil_2)
# class(lokasjon_2)

# colnames(bosatte_koorfil_2)

# lokasjon_3 <- c("Narvik", "Bodø")

# opptaksomrade_lokasjon_3 <- opptaksomrade_lokasjon %>%
# dplyr::filter(OPPTAK %in% lokasjon_3)

# test <- sf::st_filter(bosatte_koorfil_2, lokasjon_2) # opptaksomrade_lokasjon_3
# nrow(test)

# ggplot() + 
# geom_sf(data = opptaksomrade_lokasjon_3) + # lokasjon_2
# geom_sf(data = bosatte_koorfil_2) # test

