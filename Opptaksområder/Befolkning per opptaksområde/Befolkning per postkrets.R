# # Befolkning per postkrets

valgt_crs <- 4326

# +
# renv::restore()
# renv::restore("/ssb/bruker/rdn/speshelse")

suppressPackageStartupMessages({
  library(tidyverse)
  library(PxWebApiData)
  library(lubridate)
  library(haven)
  library(arrow)
  library(sf)
  library(sfarrow)
})

# +
### Statistikkbanktabell
# -

aargang <- 2019

# +
T07459 <- PxWebApiData::ApiData(07459, ContentsCode = "Personer",
                                Kjonn = T,
                                Alder = T,
                                Region = c("5001", "4204"),
                               Tid = as.character(aargang)) [[2]]

T07459 %>%
dplyr::group_by(Region) %>%
dplyr::summarise(PERSONER = sum(value))
# -

# ### Laster inn bereg fra Linux

bereg_filsti <- paste0("/ssb/stamme04/bereg/person/wk24/bosatte_koorfil_g", aargang, "m01d01eslep.sas7bdat")

# +
bosatte_koorfil <- haven::read_sas(bereg_filsti)

bosatte_koorfil <- bosatte_koorfil %>%
  dplyr::mutate(grunnkrets = paste0(KOMMNR, gkrets),
                XY = paste0(X_KOORDINAT, ", ", Y_KOORDINAT)) %>%
  dplyr::mutate(ID = paste0(KOMMNR, "-",  # Lager ID
                       GATENR_GAARDSNR, "-",
                       HUSNR_BRUKSNR, "-",
                       BOKSTAV_FESTENR, "-",
                       XY)) %>%
  dplyr::filter(!is.na(X_KOORDINAT), # Sletter personer uten adressekoordinater
                !is.na(Y_KOORDINAT))

# +
colnames(bosatte_koorfil)

# Lager aldersvariabel per 1. januar
x_date   <- as.Date(paste0(aargang, "-01-01"))

bosatte_koorfil_fix <- bosatte_koorfil %>%
 dplyr::mutate(FOEDSELSDATO = as.Date(FOEDSELSDATO, "%Y%m%d"),
                # alder = aargang-as.numeric(substr(FOEDSELSDATO, 1, 4))) %>%
               ALDER = trunc((FOEDSELSDATO %--% x_date) / lubridate::years(1))) %>%
dplyr::select(FOEDSELSDATO, ALDER, KJOENN, ID, KOMMNR, GATENR_GAARDSNR, HUSNR_BRUKSNR, BOKSTAV_FESTENR, grunnkrets, XY, X_KOORDINAT, Y_KOORDINAT)

head(bosatte_koorfil_fix)
# -

bosatte_koorfil_fix_subset <- bosatte_koorfil_fix %>%
dplyr::filter(KOMMNR %in% c("5001", "4204")) %>%
 dplyr::rename(X = X_KOORDINAT,
                Y = Y_KOORDINAT) %>%
  dplyr::mutate(x = as.numeric(as.character(X)),
                Y = as.numeric(as.character(Y))) %>%
  sf::st_as_sf(coords = c("Y", "X"), crs = 25833) %>%
  sf::st_transform(crs = valgt_crs)

# +
arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"
arbeidsmappe_kart <- paste0(arbeidsmappe, "kart/", aargang, "/")

postkretser_kart_filsti <- paste0(arbeidsmappe_kart, "POST_postkretser_flate_", aargang, ".parquet")
# -

rename_geometry <- function(g, name){
  current = attr(g, "sf_column")
  names(g)[names(g)==current] = name
  sf::st_geometry(g)=name
  g
}

postkretser_kart <- sfarrow::st_read_parquet(postkretser_kart_filsti)

postkretser_kart <- postkretser_kart %>%
  dplyr::filter(KOMMUNENR %in% c("4204", "5001")) %>%
  sf::st_zm(drop = T) %>%
  sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = valgt_crs) # %>%
  # dplyr::rename(GRUNNKRETSNUMMER = POSTNR)

postkretser_kart <- rename_geometry(postkretser_kart, "geometry")
sf::st_geometry(postkretser_kart) <- "geometry"

# +
# colnames(postkretser_kart)

# unique(postkretser_kart$POSTNR)

# +
# ggplot() +
# geom_sf(data = postkretser_kart)

# +
opptaksomrade_postkrets_bereg <- sf::st_intersection(bosatte_koorfil_fix_subset, postkretser_kart) %>%
  data.frame() %>%
  dplyr::group_by(KOMMUNENR, POSTNR, KJOENN, ALDER) %>%
  dplyr::tally(name = "PERSONER")

# opptaksomrader_DPS_trondheim_poly <- dplyr::left_join(opptaksomrader_DPS_trondheim_poly, opptaksomrader_DPS_trondheim_bereg, by = "OPPTAK") %>%
#   sf::st_transform(crs = valgt_crs)
# -

colnames(opptaksomrade_postkrets_bereg)

nrow(opptaksomrade_postkrets_bereg)
nrow(bosatte_koorfil_fix_subset)

# ### Lagrer filen
arbeidsmappe_opptak <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/befolkning_per_opptaksomrade/")
filsti <- paste0(arbeidsmappe_opptak, "inndata/befolkning_per_postkrets_", aargang, ".parquet")

arrow::write_parquet(opptaksomrade_postkrets_bereg, filsti)

T07459_diff <- T07459 %>%
  dplyr::group_by(Region) %>%
  dplyr::summarise(PERSONER = sum(value))

sum(T07459_diff$PERSONER)

opptaksomrade_postkrets_bereg_diff <- opptaksomrade_postkrets_bereg %>%
  dplyr::group_by(KOMMUNENR) %>%
  dplyr::summarise(PERSONER_EGEN = sum(PERSONER))

sum(opptaksomrade_postkrets_bereg_diff$PERSONER_EGEN)

diff <- dplyr::left_join(T07459_diff, opptaksomrade_postkrets_bereg_diff, by = c("Region" = "KOMMUNENR")) %>%
  dplyr::mutate(diff = PERSONER-PERSONER_EGEN)

