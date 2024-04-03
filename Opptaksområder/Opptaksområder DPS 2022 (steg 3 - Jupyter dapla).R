# # Opptaksområder DPS (utenhav)
#
# OBS: DPS-områdene for flate må lages i prodsonen og legges på GCS før dette programmet kan kjøres (f.eks. ssb-prod-helse-speshelse-data-kilde/felles/Kart/2022/Opptaksområder/opptaksomrader_DPS_DPS_flate_2022)

# ### Velger årgang

aargang <- 2021

# ### Flate eller utenhav

# +
utenhav <- TRUE

if (utenhav == TRUE) {
filsti_med_uten_hav <- "utenhav"
    } else if (utenhav == FALSE) {
  filsti_med_uten_hav <- "flate"
}

filsti_med_uten_hav
# -

# ### Laster inn pakker 

# OBS: cartography må installeres med renv!

# +
sf::sf_use_s2(FALSE)
CRS <- 25833

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
library(sfarrow)    
library(htmlwidgets)
        })

# # install.packages("cartography")
# renv::install("cartography")

# library(cartography)
# -

aargang <- 2022

# ### Filstier

# +
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC")) | Sys.getenv("JUPYTER_IMAGE_SPEC") == "") {
    
    arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"
    arbeidsmappe_kart <- paste0(arbeidsmappe, "kart/", aargang, "/")
    arbeidsmappe_opptak <- paste0(arbeidsmappe, "opptaksomrader/", aargang, "/")
    
    if (utenhav == FALSE) {
    grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_flate_", aargang, ".parquet")
        }
    if (utenhav == TRUE) {
    grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_utenhav_", aargang, ".parquet")

        }
    
    grunnkrets_kart <- sfarrow::st_read_parquet(grunnkrets_kart_filsti)
    
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
    
    source("/home/jovyan/fellesr/R/DAPLA_funcs.R")
    
    arbeidsmappe_kart <- paste0("ssb-prod-dapla-felles-data-delt/GIS/Kart/", aargang, "/")
    # arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"
    
    if (utenhav == FALSE) {
    grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_flate_", aargang, "/")
        }
    if (utenhav == TRUE) {
    grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_utenhav_", aargang, "/")
        }
    grunnkrets_kart <- open_dataset(grunnkrets_kart_filsti) %>%
    sfarrow::read_sf_dataset()
}
# -

# ## Laster inn kart (grunnkrets)

# +
start.time <- Sys.time()

  rename_geometry <- function(g, name){
    current = attr(g, "sf_column")
    names(g)[names(g)==current] = name
    sf::st_geometry(g)=name
    g
  }

# Lese inn filen som parquet med sfarrow
grunnkrets_kart <- grunnkrets_kart %>%
sf::st_zm(drop = T) %>%
sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = CRS) %>%
  dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS)

grunnkrets_kart <- rename_geometry(grunnkrets_kart, "geometry")
sf::st_geometry(grunnkrets_kart) <- "geometry"

grunnkrets_kart_uten_trondheim_kristiansand <- grunnkrets_kart %>%
dplyr::filter(!KOMMUNENR %in% c("4204", "5001"))

grunnkrets_kart_trondheim <- grunnkrets_kart %>%
dplyr::filter(KOMMUNENR %in% c("5001"))

grunnkrets_kart_kristiansand <- grunnkrets_kart %>%
dplyr::filter(KOMMUNENR %in% c("4204"))

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
# -

if (aargang == 2017) {
grunnkrets_kart <- grunnkrets_kart %>%
dplyr::mutate(GRUNNKRETSNUMMER = case_when(
    GRUNNKRETSNUMMER == "00101609" ~ "07101609", # Storevahr har feil grunnkretsnummer i filen!
    TRUE ~ GRUNNKRETSNUMMER))
    }

# ### SSB fargepalett

ssb_farger <- klassR::GetKlass(614, output_style = "wide") %>%
  dplyr::rename(farge_nummer = code3, 
                HEX = name3, 
                farge = name2, 
                type = name1) %>%
  dplyr::select(-code1, -code2) %>%
  dplyr::filter(farge != "Hvit")

# ## Laster inn kart (postnummer)

# +
# start.time <- Sys.time()

# postkretser_kart_filsti <- paste0(arbeidsmappe_kart, "POST_postkretser_flate_", aargang, ".parquet")

# # Lese inn filen som parquet med sfarrow
# postkretser_kart <- sfarrow::st_read_parquet(postkretser_kart_filsti) %>%
# sf::st_zm(drop = T) %>%
# sf::st_cast("MULTIPOLYGON") %>%
#   sf::st_transform(crs = CRS) %>%
#   dplyr::rename(GRUNNKRETSNUMMER = POSTNR) %>%
# dplyr::filter(KOMMUNENR %in% c("4204", "5001"))

# postkretser_kart <- rename_geometry(postkretser_kart, "geometry")
# sf::st_geometry(postkretser_kart) <- "geometry"

# end.time <- Sys.time()
# time.taken <- end.time - start.time
# time.taken
# -

# ## Kodeliste for opptaksområder i spesialisthelsetjenesten (DPS)

# +
opptaksomrader_KLASS <- klassR::GetKlass(632, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

nrow(opptaksomrader_KLASS)

# Grunnkrets
opptaksomrader_KLASS_grunnkrets <- opptaksomrader_KLASS %>%
dplyr::filter(nchar(GRUNNKRETSNUMMER) > 4)

nrow(opptaksomrader_KLASS_grunnkrets)
# -

# ### Sjekker antall grunnkretser mot KLASS

# +
# grunnkrets_KLASS <- klassR::GetKlass(1, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
# dplyr::filter(!substr(code2, 1, 4) %in% c("4204", "5001"))

# nrow(grunnkrets_KLASS)

# mangler_fra_KLASS <- grunnkrets_KLASS %>%
# dplyr::filter(!code2 %in% unique(opptaksomrader_KLASS$GRUNNKRETSNUMMER))

# unique(mangler_fra_KLASS$name2)
# nrow(mangler_fra_KLASS)
# head(mangler_fra_KLASS)
# -

# ### Sjekker om noen grunnkretser mangler fra kartet

# +
# test <- dplyr::left_join(opptaksomrader_KLASS_grunnkrets, grunnkrets_kart, by = "GRUNNKRETSNUMMER") %>%
# data.frame() %>%
# dplyr::filter(is.na(KOMMUNENR))

# nrow(test)
# unique(test$GRUNNKRETS_NAVN)
# -

# ### Merger opptaksområder med grunnkretskart

# +
# opptaksomrader_KLASS_2_kart <- grunnkrets_kart %>%
# dplyr::left_join(opptaksomrader_KLASS_2, by = "GRUNNKRETSNUMMER")

grunnkrets_uten_trondheim_kristiansand_kart <- dplyr::left_join(grunnkrets_kart_uten_trondheim_kristiansand, opptaksomrader_KLASS_grunnkrets, by = "GRUNNKRETSNUMMER")

# +
# ggplot() + 
# geom_sf(data = opptaksomrader_KLASS_grunnkrets_kart)
# -

# ## Trondheim

# +
grunnkrets_kart_trondheim <- grunnkrets_kart_trondheim %>%
dplyr::group_by(KOMMUNENR) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup()

ggplot() + 
geom_sf(data = grunnkrets_kart_trondheim)
# -

opptaksomrader_DPS_trondheim <- open_dataset(paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_DPS_DPS_flate_", aargang, "/"))
opptaksomrader_DPS_trondheim

# +
opptaksomrader_DPS_trondheim <- open_dataset(paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_DPS_DPS_flate_", aargang, "/")) %>%
  dplyr::filter(OPPTAK %in% c("Nidelv", "Nidaros")) %>%
  sfarrow::read_sf_dataset()

ggplot() + 
geom_sf(data = opptaksomrader_DPS_trondheim) +
geom_sf(data = grunnkrets_kart_trondheim, color = "red")

# +
opptaksomrader_DPS_trondheim_2 <- sf::st_intersection(opptaksomrader_DPS_trondheim, grunnkrets_kart_trondheim)

nidaros <- opptaksomrader_DPS_trondheim_2 %>%
dplyr::filter(OPPTAK == "Nidaros")

nidelv <- opptaksomrader_DPS_trondheim_2 %>%
dplyr::filter(OPPTAK == "Nidelv")

ggplot() + 
geom_sf(data = nidaros, fill = "blue") +
geom_sf(data = nidelv, fill = "red")
# -

# ## Kristiansand

# +
grunnkrets_kart_kristiansand <- grunnkrets_kart_kristiansand %>%
dplyr::group_by(KOMMUNENR) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup()

ggplot() + 
geom_sf(data = grunnkrets_kart_kristiansand)

# +
opptaksomrader_DPS_kristiansand <- open_dataset(paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_DPS_DPS_flate_", aargang, "/")) %>%
  dplyr::filter(OPPTAK %in% c("Solvang", "Strømme")) %>%
  sfarrow::read_sf_dataset()

ggplot() + 
geom_sf(data = opptaksomrader_DPS_kristiansand) +
geom_sf(data = grunnkrets_kart_kristiansand, color = "red")

# +
opptaksomrader_DPS_kristiansand_2 <- sf::st_intersection(opptaksomrader_DPS_kristiansand, grunnkrets_kart_kristiansand)

solvang <- opptaksomrader_DPS_kristiansand_2 %>%
dplyr::filter(OPPTAK == "Solvang")

stromme <- opptaksomrader_DPS_kristiansand_2 %>%
dplyr::filter(OPPTAK == "Strømme")

ggplot() + 
geom_sf(data = solvang, fill = "blue") +
geom_sf(data = stromme, fill = "red")

# +
opptaksomrader_DPS_trondheim_2 <- opptaksomrader_DPS_trondheim_2 %>%
dplyr::select(OPPTAK)

opptaksomrader_DPS_kristiansand_2 <- opptaksomrader_DPS_kristiansand_2 %>%
dplyr::select(OPPTAK)

grunnkrets_uten_trondheim_kristiansand_kart_2 <- grunnkrets_uten_trondheim_kristiansand_kart %>%
dplyr::select(OPPTAK)
# -

grunnkrets_med_trondheim_kristiansand_kart <- rbind(grunnkrets_uten_trondheim_kristiansand_kart_2, opptaksomrader_DPS_trondheim_2, opptaksomrader_DPS_kristiansand_2)

# ## Lager opptaksområder for DPS-områder

# +
opptaksomrader_DPS_befolkning <- open_dataset(paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_DPS_DPS_flate_", aargang, "/"))) %>%
sfarrow::read_sf_dataset() %>%
data.frame() %>%
dplyr::select(OPPTAK, PERSONER)

opptaksomrader_KLASS_info <- opptaksomrader_KLASS %>%
dplyr::distinct(OPPTAK, NAVN_HF, NAVN_RHF) %>%
dplyr::left_join(opptaksomrader_DPS_befolkning, by = "OPPTAK")

opptaksomrader_lokasjon <- grunnkrets_med_trondheim_kristiansand_kart %>%
  dplyr::group_by(OPPTAK) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup() %>%
dplyr::left_join(opptaksomrader_KLASS_info, by = "OPPTAK")

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  sfarrow::st_write_parquet(obj=opptaksomrader_lokasjon, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_DPS_DPS_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  opptaksomrader_DPS_DPS_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_DPS_DPS_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_DPS_DPS_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_lokasjon, file = opptaksomrader_DPS_DPS_filsti, sf = TRUE)
}
# -

ggplot() + 
geom_sf(data = opptaksomrader_lokasjon)

# ## Lager opptaksområder for DPS-områder (HF)

# +
opptaksomrader_DPS_HF <- opptaksomrader_lokasjon %>%
dplyr::group_by(NAVN_RHF, NAVN_HF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)),
                   PERSONER = sum(PERSONER)) %>%
  dplyr::ungroup()

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  sfarrow::st_write_parquet(obj=opptaksomrader_DPS_HF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_DPS_HF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  opptaksomrader_DPS_HF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_DPS_HF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_DPS_HF_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_DPS_HF, file = opptaksomrader_DPS_HF_filsti, sf = TRUE)
}
# -

ggplot() + 
geom_sf(data = opptaksomrader_DPS_HF)

# ## Lager opptaksområder for DPS-områder (RHF)

# +
opptaksomrader_DPS_RHF <- opptaksomrader_DPS_HF %>%
dplyr::group_by(NAVN_RHF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)),
                   PERSONER = sum(PERSONER)) %>%
  dplyr::ungroup()

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  sfarrow::st_write_parquet(obj=opptaksomrader_DPS_RHF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_DPS_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  opptaksomrader_DPS_RHF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_DPS_RHF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_DPS_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_DPS_RHF, file = opptaksomrader_DPS_RHF_filsti, sf = TRUE)
}
# -

ggplot() + 
geom_sf(data = opptaksomrader_DPS_RHF)
