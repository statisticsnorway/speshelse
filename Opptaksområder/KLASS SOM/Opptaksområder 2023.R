# # Opptaksområder 2023

aargang <- 2023

# +
options(repr.matrix.max.rows=600, repr.matrix.max.cols=2000)

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
        })

source("/home/jovyan/fellesr/R/DAPLA_funcs.R")
# -

#  ## Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk) t-1

# +
opptaksomrader_KLASS <- klassR::GetKlass(629, output_style = "wide", date = c(paste0(aargang-1, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

nrow(opptaksomrader_KLASS)

# +
# opptaksomrader_KLASS %>%
# dplyr::filter(GRUNNKRETSNUMMER == "30039999")
# -

# ### Korrespondanse mellom t og t-1 fra KLASS

# +
grunnkrets_KLASS <- klassR::GetKlass(1, date = c(paste0(aargang-1, "-01-01"), paste0(aargang, "-01-01")), correspond = TRUE) %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = sourceCode, 
             GRUNNKRETSNUMMER = targetCode, 
             targetName = targetName) %>%
dplyr::left_join(opptaksomrader_KLASS, by = c("GRUNNKRETSNUMMER_T1" = "GRUNNKRETSNUMMER"))

nrow(grunnkrets_KLASS)

# +
nytt_navn <- grunnkrets_KLASS %>%
dplyr::filter(GRUNNKRETSNUMMER_T1 == GRUNNKRETSNUMMER) %>%
dplyr::rename(GAMMELT_NAVN= sourceName, 
             NYTT_NAVN = targetName)

nrow(nytt_navn)
nytt_navn

# +
endringer <- grunnkrets_KLASS %>%
dplyr::filter(GRUNNKRETSNUMMER_T1 !=GRUNNKRETSNUMMER )
nrow(endringer)

# SLETT
unique(endringer$GRUNNKRETSNUMMER_T1)

# LEGG TIL
unique(endringer$GRUNNKRETSNUMMER)

endringer
# -

# ### Last inn kart over opptaksområde på laveste nivå + grunnkretsene i listen for T og T1!
#
# st_intersection? Sjekk at ingen av opptaksområdene bytter opptaksområde!

opptaksomrader_SOM_lokasjon_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang-1, "/Opptaksområder/opptaksomrader_SOM_lokasjon_flate", "_", aargang-1, "/")

opptaksomrader_SOM_lokasjon <- open_dataset(opptaksomrader_SOM_lokasjon_filsti) %>%
    sfarrow::read_sf_dataset() %>%
sf::st_transform(crs = 4326)

# +
arbeidsmappe_kart <- paste0("ssb-prod-dapla-felles-data-delt/GIS/Kart/", aargang, "/")
grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_flate_", aargang, "/")

grunnkrets_kart <- open_dataset(grunnkrets_kart_filsti) %>%
    sfarrow::read_sf_dataset() %>%
sf::st_zm(drop = T) %>%
sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = 4326) %>%
  dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS) %>%
dplyr::filter(GRUNNKRETSNUMMER %in% unique(endringer$GRUNNKRETSNUMMER))

arbeidsmappe_kart_t1 <- paste0("ssb-prod-dapla-felles-data-delt/GIS/Kart/", aargang-1, "/")
grunnkrets_kart_t1_filsti <- paste0(arbeidsmappe_kart_t1, "ABAS_grunnkrets_flate_", aargang-1, "/")

grunnkrets_kart_t1 <- open_dataset(grunnkrets_kart_t1_filsti) %>%
    sfarrow::read_sf_dataset() %>%
sf::st_zm(drop = T) %>%
sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = 4326) %>%
  dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS) %>%
dplyr::filter(GRUNNKRETSNUMMER %in% unique(endringer$GRUNNKRETSNUMMER_T1))
# -

test_t1 <- sf::st_intersection(grunnkrets_kart_t1, opptaksomrader_SOM_lokasjon) %>%
data.frame() %>%
dplyr::select(GRUNNKRETSNUMMER, OPPTAK) %>%
dplyr::rename(OPPTAK_T1 = OPPTAK)

test <- sf::st_intersection(grunnkrets_kart, opptaksomrader_SOM_lokasjon) %>%
data.frame() %>%
dplyr::select(GRUNNKRETSNUMMER, OPPTAK) %>%
dplyr::rename(OPPTAK_NY = OPPTAK)

# +
endringer_2 <- endringer %>%
dplyr::select(GRUNNKRETSNUMMER_T1, sourceName, GRUNNKRETSNUMMER, targetName) %>%
dplyr::left_join(test_t1, by = c("GRUNNKRETSNUMMER_T1" = "GRUNNKRETSNUMMER")) %>%
dplyr::left_join(test, by = c("GRUNNKRETSNUMMER" = "GRUNNKRETSNUMMER"))

endringer_2 %>%
dplyr::filter(OPPTAK_T1 != OPPTAK_NY)
# -

# ### Slett

# +
slett <- endringer_2 %>%
dplyr::distinct(GRUNNKRETSNUMMER_T1, sourceName, OPPTAK_T1)

slett
# -

# ### Legg til

# +
legg_til <- endringer_2 %>%
dplyr::distinct(GRUNNKRETSNUMMER, targetName, OPPTAK_NY)

legg_til
