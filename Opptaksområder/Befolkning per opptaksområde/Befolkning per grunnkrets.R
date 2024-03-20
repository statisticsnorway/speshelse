# # Befolkning per grunnkrets

# +
# renv::restore()
# renv::restore("/ssb/bruker/rdn/speshelse")

suppressPackageStartupMessages({
  library(tidyverse)
  library(PxWebApiData)
  library(lubridate)
  library(fellesr)
})
# -

# ### Statistikkbanktabell

# +
# Sys.setenv(no_proxy= "nexus.ssb.no,git-adm.ssb.no,i.test.ssb.no,i.ssb.no,data.ssb.no,github.com,api.github.com,codeload.github.com")
# -

# aargang <- 2016
if (exists("aargang_master")==TRUE){
aargang <- aargang_master    
}

# +
T04317 <- PxWebApiData::ApiData(04317, ContentsCode = "Personer",
                                Grunnkretser = TRUE,
                                Tid = as.character(aargang)) [[2]] %>%
  dplyr::filter(!is.na(value)) %>%
  dplyr::rename(GRUNNKRETSNUMMER = Grunnkretser,
                PERSONER_STATBANK = value) %>%
  dplyr::select(GRUNNKRETSNUMMER, PERSONER_STATBANK)

# Retter opp feil i statistikkbanktabellen
if (aargang == 2018){
  T04317 <- T04317 %>%
    dplyr::mutate(GRUNNKRETSNUMMER = case_when(
      GRUNNKRETSNUMMER == "03014201" ~ "03014211",
      TRUE ~ GRUNNKRETSNUMMER
    ))
}
# -

sum(T04317$PERSONER)

# ### Laster inn bereg fra Linux

# +
# bereg_filsti <- paste0("/ssb/stamme04/bereg/person/wk24/bosatte_koorfil_g", aargang, "m01d01eslep.sas7bdat")
# bereg_koorfil <- haven::read_sas(bereg_filsti)
# nrow(bereg_koorfil) # 2023: 5483128

# +
bosatte_koorfil <- haven::read_sas(bosatt_filsti) %>%
  dplyr::rename_all(toupper)

nrow(bosatte_koorfil)

# +
# bosatte_koorfil <- bosatte_koorfil %>%
# dplyr::rename_all(toupper)

# +
# Lager aldersvariabel per 1. januar
x_date   <- as.Date(paste0(aargang, "-01-01"))

bosatte_koorfil_fix <- bosatte_koorfil %>%
  dplyr::mutate(GRUNNKRETSNUMMER = paste0(KOMMNR, GKRETS)) %>%
  select(GRUNNKRETSNUMMER, KJOENN, FOEDSELSDATO) %>%
  dplyr::mutate(FOEDSELSDATO = as.Date(FOEDSELSDATO, "%Y%m%d"),
                ALDER = trunc((FOEDSELSDATO %--% x_date) / lubridate::years(1)))

# +
# # OBS: Frydenlund ble splittet i 2021
# if (aargang == 2022) {
# bosatte_koorfil_fix <- bosatte_koorfil_fix %>%
# mutate(GRUNNKRETSNUMMER = case_when(GRUNNKRETSNUMMER == "30490109" ~ "30490113", TRUE ~ GRUNNKRETSNUMMER))
#     }

# +
# arbeidsmappe <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/")

# filsti <- paste0(arbeidsmappe, "befolkning_per_grunnkrets_", aargang, ".parquet")
# filsti

arbeidsmappe <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/befolkning_per_opptaksomrade/")

filsti <- paste0(arbeidsmappe, "inndata/befolkning_per_grunnkrets_", aargang, ".parquet")

# +

T04317_egen <- bosatte_koorfil_fix %>%
  dplyr::group_by(KJOENN, ALDER, GRUNNKRETSNUMMER) %>%
  dplyr::tally(name = "PERSONER")

# Lagrer filen
arrow::write_parquet(T04317_egen, filsti)

T04317_egen %>%
  head()
# -

nrow(bosatte_koorfil_fix)
sum(T04317_egen$PERSONER)

# +
T04317_egen_agg <- T04317_egen %>%
  dplyr::group_by(GRUNNKRETSNUMMER) %>%
  dplyr::summarise(PERSONER = sum(PERSONER))

sum(T04317_egen_agg$PERSONER)
# -

T04317_egen_agg <- T04317_egen_agg %>%
  dplyr::left_join(T04317, by = "GRUNNKRETSNUMMER")

T04317_egen_agg %>%
  dplyr::mutate(diff = PERSONER-PERSONER_STATBANK) %>%
  dplyr::filter(diff > 0)

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
# nrow(bereg_koorfil) # 2023: 5483128

# +
# T04317_egen <- bereg_koorfil %>%
# dplyr::group_by(ALDER, GRUNNKRETS) %>%
# dplyr::tally(name = "PERSONER")

# T04317_egen %>%
# head()

# +
# sum(T04317_egen$PERSONER)
# -


