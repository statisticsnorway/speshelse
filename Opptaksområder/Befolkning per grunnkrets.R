# # Befolkning per grunnkrets

aargang <- 2023

suppressPackageStartupMessages({ 
library(tidyverse)
library(PxWebApiData)
library(lubridate)
        })

# ### Statistikkbanktabell

# +
T04317 <- PxWebApiData::ApiData(04317, ContentsCode = "Personer", 
                                Grunnkretser = TRUE, 
                                Tid = as.character(aargang)) [[2]] %>%
  dplyr::filter(!is.na(value)) %>%
  dplyr::rename(GRUNNKRETSNUMMER = Grunnkretser,
                PERSONER = value) %>%
  dplyr::select(GRUNNKRETSNUMMER, PERSONER)

# Retter opp feil i statistikkbanktabellen
if (aargang == 2018){
T04317 <- T04317 %>%
    dplyr::mutate(GRUNNKRETSNUMMER = case_when(
    GRUNNKRETSNUMMER == "03014201" ~ "03014211", 
        TRUE ~ GRUNNKRETSNUMMER
    ))
}
# -

# ### Laster inn bereg fra Linux

# +
# bereg_filsti <- paste0("/ssb/stamme04/bereg/person/wk24/bosatte_koorfil_g", aargang, "m01d01eslep.sas7bdat")
# bereg_koorfil <- haven::read_sas(bereg_filsti)
# nrow(bereg_koorfil) # 2023: 5483128
# -

bosatt_filsti <- paste0("/ssb/stamme03/bestat/folkem/wk14/bosatt/g", aargang, "m01d01.sas7bdat")
bosatte_koorfil <- haven::read_sas(bosatt_filsti)
nrow(bosatte_koorfil)

# +
# bereg_koorfil <- bereg_koorfil %>%
#   dplyr::mutate(GRUNNKRETS = paste0(KOMMNR, gkrets), 
#                 XY = paste0(X_KOORDINAT, ", ", Y_KOORDINAT)) %>%
#   dplyr::mutate(ID = paste0(KOMMNR, "-",  # Lager ID
#                        GATENR_GAARDSNR, "-", 
#                        HUSNR_BRUKSNR, "-", 
#                        BOKSTAV_FESTENR, "-",
#                        XY)) %>%
#   dplyr::filter(!is.na(X_KOORDINAT), # Sletter personer uten adressekoordinater
#                 !is.na(Y_KOORDINAT))

# colnames(bereg_koorfil) 

# # Lager aldersvariabel per 1. januar
# x_date   <- as.Date(paste0(aargang, "-01-01"))

# bereg_koorfil <- bereg_koorfil %>%
#  dplyr::mutate(FOEDSELSDATO = as.Date(FOEDSELSDATO, "%Y%m%d"),
#                ALDER = trunc((FOEDSELSDATO %--% x_date) / lubridate::years(1))) %>%
# dplyr::select(FOEDSELSDATO, ALDER, KJOENN, ID, KOMMNR, GATENR_GAARDSNR, HUSNR_BRUKSNR, BOKSTAV_FESTENR, GRUNNKRETS, XY, X_KOORDINAT, Y_KOORDINAT)

# head(bereg_koorfil)

# +
# sum(T04317$PERSONER)
# -

nrow(bereg_koorfil) # 2023: 5483128

# +
T04317_egen <- bereg_koorfil %>%
dplyr::group_by(ALDER, GRUNNKRETS) %>%
dplyr::tally(name = "PERSONER")

T04317_egen %>%
head()
# -

sum(T04317_egen$PERSONER)


