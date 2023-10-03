# -*- coding: utf-8 -*-
# # Opptaksområder

# ## Velger årgang og tjenesteområde

# Mangler grunnkretskart før 2015

aargang <- 2023

# ### Velger tjenesteområde
#
# + SOM
# + PHV
# + TSB
# + DPS

tjeneste <- "DPS"

# ## Flate eller utenhav

utenhav <- FALSE

if (utenhav == TRUE) {
filsti_med_uten_hav <- "utenhav"
    } else if (utenhav == FALSE) {
  filsti_med_uten_hav <- "flate"
}

filsti_med_uten_hav

# ## Kommuner med postkretser (DPS)
#
# Kristiansand og Trondheim

kommuner_med_postkretser <- c("4204", "5001")

# ## Laster inn pakker 

# +
sf::sf_use_s2(FALSE)
CRS <- 25833

renv::autoload()

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
    library(fellesr)
    library(cartography)
        })
# -

# ## Laster inn kart (grunnkrets)

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
    
    postkretser_kart_filsti <- paste0(arbeidsmappe_kart, "POST_postkretser_flate_", aargang, ".parquet")
    
    grunnkrets_kart <- sfarrow::st_read_parquet(grunnkrets_kart_filsti)
    postkretser_kart <- sfarrow::st_read_parquet(postkretser_kart_filsti)

} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
    
    # source("/home/jovyan/fellesr/R/DAPLA_funcs.R")
    
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


# Oppretter `arbeidsmappe_opptak` dersom den ikke finnes.

arbeidsmappe
arbeidsmappe_kart
arbeidsmappe_opptak

if (file.exists(arbeidsmappe_opptak)==FALSE) {
  dir.create(arbeidsmappe_opptak)
}

# +
start.time <- Sys.time()

# Endrer navn på geometrikolonnen til "geometry"
  rename_geometry <- function(g, name){
    current = attr(g, "sf_column")
    names(g)[names(g)==current] = name
    sf::st_geometry(g)=name
    g
  }

grunnkrets_kart <- rename_geometry(grunnkrets_kart, "geometry")
sf::st_geometry(grunnkrets_kart) <- "geometry"
# -

# ### Fix for MULTISURFACE (gjelder kun i 2023?)

# +
MULTISURFACE_test <- as.data.frame(table(sf::st_geometry_type(grunnkrets_kart))) %>%
dplyr::filter(Var1 == "MULTISURFACE" & Freq > 0)

if (nrow(MULTISURFACE_test) != 0) {
# Kun MULTISURFACE
grunnkrets_kart_MULTISURFACE <- grunnkrets_kart %>%
  dplyr::filter(sf::st_geometry_type(geometry) == "MULTISURFACE")
    
# Uten MULTISURFACE    
grunnkrets_kart_uten_MULTISURFACE <- grunnkrets_kart %>%
  dplyr::filter(sf::st_geometry_type(geometry) != "MULTISURFACE")    
    
grunnkrets_kart_MULTISURFACE_fix <- data.frame()
    
    for (i in 1:nrow(grunnkrets_kart_MULTISURFACE)){

grunnkrets_kart_MULTISURFACE_i <- grunnkrets_kart_MULTISURFACE[i,]

wkt_geometry <- unique(grunnkrets_kart_MULTISURFACE_i$geometry)
wkt_geometry <- sf::st_as_sfc(wkt_geometry, crs = CRS)

polygons <- lapply(wkt_geometry, function(geometry) {
  polygon <- sf::st_cast(sf::st_cast(geometry, "MULTIPOLYGON"), "MULTIPOLYGON")
  return(polygon)
})

test <- polygons %>%
  sf::st_as_sfc(crs = CRS) %>%
  data.frame()
    
grunnkrets_kart_MULTISURFACE_i <- grunnkrets_kart_MULTISURFACE_i %>%
  data.frame() %>%
  dplyr::select(-geometry) %>%
    dplyr::bind_cols(test) %>%
    sf::st_sf() 
    
sf::st_geometry(grunnkrets_kart_MULTISURFACE_i) <- "geometry"

grunnkrets_kart_MULTISURFACE_fix <- rbind(grunnkrets_kart_MULTISURFACE_fix, grunnkrets_kart_MULTISURFACE_i)    
    
    }
    
grunnkrets_kart <- rbind(grunnkrets_kart_uten_MULTISURFACE, grunnkrets_kart_MULTISURFACE_fix)    
}

# +
# Lese inn filen som parquet med sfarrow
grunnkrets_kart <- grunnkrets_kart %>%
# filter(sf::st_geometry_type(geometry) != "MULTISURFACE") %>%
sf::st_zm(drop = T) %>%
sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = CRS) %>%
  dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS)

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
# -

# ## Fikser dubletter

# +
nrow(grunnkrets_kart)

grunnkrets_kart_dubletter <- grunnkrets_kart %>%
  group_by(KOMMUNENR, GRUNNKRETSNUMMER) %>%
  filter(n()>1)

nrow(grunnkrets_kart_dubletter)

if (nrow(grunnkrets_kart_dubletter) > 0) {
  grunnkrets_kart_uten_dubletter <- grunnkrets_kart %>%
    dplyr::filter(!GRUNNKRETSNUMMER %in% unique(grunnkrets_kart_dubletter$GRUNNKRETSNUMMER)) %>%
    dplyr::select(KOMMUNENR, GRUNNKRETSNUMMER)
  
  # OBS: det finnes dubletter i grunnkretsfilen! Slår disse samme slik at det kun finnes ett multipolygon per grunnkrets
  grunnkrets_kart_dubletter_2 <- grunnkrets_kart %>%
    dplyr::filter(GRUNNKRETSNUMMER %in% unique(grunnkrets_kart_dubletter$GRUNNKRETSNUMMER)) %>%
    group_by(KOMMUNENR, GRUNNKRETSNUMMER) %>%
    dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)))
  
  grunnkrets_kart <- rbind(grunnkrets_kart_uten_dubletter, grunnkrets_kart_dubletter_2)
}

nrow(grunnkrets_kart)
# -

if (aargang == 2017) {
grunnkrets_kart <- grunnkrets_kart %>%
dplyr::mutate(GRUNNKRETSNUMMER = case_when(
    GRUNNKRETSNUMMER == "00101609" ~ "07101609", # Storevahr har feil grunnkretsnummer i filen!
    TRUE ~ GRUNNKRETSNUMMER))
    }

# ## Laster inn befolkning per opptaksområde

# +
if (tjeneste == "PHV"){
    tjeneste_SB <- c("VOP", "BUP")
} else {
   tjeneste_SB <- tjeneste 
}

befolkning_per_opptak <- PxWebApiData::ApiData(13982, 
                                               HelseReg = T,
                                               HelseTjenomr = tjeneste_SB,
                                               Kjonn = "0",
                                               Alder = T,
                                               Tid = as.character(aargang),
                                               ContentsCode = TRUE) [[2]] %>%
group_by(HelseReg) %>%
summarise(PERSONER = sum(value))
# -

# ## Laster inn kodelister fra KLASS

# ### Opptaksområder

helsregion_KLASS <- klassR::GetKlass(603, output_level = 2) %>%
dplyr::rename(ORGNR_RHF = code, 
              REGION = parentCode) %>%
dplyr::select(ORGNR_RHF, REGION) %>%
dplyr::mutate(REGION = paste0("H", REGION))

if (tjeneste == "SOM"){
    opptaksomrader_KLASS <- klassR::GetKlass(629, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1) %>%
    dplyr::left_join(helsregion_KLASS, by = "ORGNR_RHF")
}

if (tjeneste == "PHV"){
opptaksomrader_KLASS <- klassR::GetKlass(630, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1) %>%
    dplyr::left_join(helsregion_KLASS, by = "ORGNR_RHF")
}

if (tjeneste == "TSB"){
opptaksomrader_KLASS <- klassR::GetKlass(631, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1) %>%
        dplyr::left_join(helsregion_KLASS, by = "ORGNR_RHF")
    }

# +
if (tjeneste == "DPS"){
opptaksomrader_KLASS <- klassR::GetKlass(632, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
  dplyr::rename(GRUNNKRETSNUMMER = code4, 
                GRUNNKRETS_NAVN = name4, 
                OPPTAK_NUMMER = code3, 
                OPPTAK = name3, 
                ORGNR_HF = code2, 
                NAVN_HF = name2, 
                ORGNR_RHF = code1, 
                NAVN_RHF = name1) %>%
            dplyr::left_join(helsregion_KLASS, by = "ORGNR_RHF")
    
# Postnummer
opptaksomrader_KLASS_postnummer <- opptaksomrader_KLASS %>%
  dplyr::filter(nchar(GRUNNKRETSNUMMER) == 4)
nrow(opptaksomrader_KLASS_postnummer)
}

# nrow(opptaksomrader_KLASS)

# # Grunnkrets
# opptaksomrader_KLASS_grunnkrets <- opptaksomrader_KLASS %>%
#   dplyr::filter(nchar(GRUNNKRETSNUMMER) > 4)


# nrow(opptaksomrader_KLASS_grunnkrets)

# -

nrow(opptaksomrader_KLASS)

# +
if (tjeneste == "DPS"){
    postkretser_kart <- postkretser_kart %>%
  sf::st_zm(drop = T) %>%
  sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = CRS) %>%
  dplyr::rename(GRUNNKRETSNUMMER = POSTNR) %>%
  dplyr::filter(KOMMUNENR %in% c("4204", "5001"))

postkretser_kart <- rename_geometry(postkretser_kart, "geometry")
sf::st_geometry(postkretser_kart) <- "geometry"

opptaksomrader_KLASS_postnummer_2 <- postkretser_kart %>%
  dplyr::left_join(opptaksomrader_KLASS_postnummer, by = "GRUNNKRETSNUMMER")

tester <- opptaksomrader_KLASS_postnummer_2 %>%
  filter(is.na(OPPTAK))

nrow(opptaksomrader_KLASS_postnummer_2)

sort(unique(opptaksomrader_KLASS_postnummer_2$GRUNNKRETSNUMMER))
}
# -

# ## Trondheim

# +
if (tjeneste == "DPS"){
# Lager kart med kun grensen til Trondheum
grunnkrets_kart_trondheim <- grunnkrets_kart %>%
  dplyr::filter(KOMMUNENR == "5001") %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  sf::st_transform(crs = CRS)

ggplot() + 
  geom_sf(data = grunnkrets_kart_trondheim)

# Slår sammen geometriene for DPS-områdene i Trondheim (basert på postnummer) 
opptaksomrader_DPS_trondheim <- opptaksomrader_KLASS_postnummer_2 %>%
  dplyr::filter(KOMMUNENR == "5001", 
                !is.na(OPPTAK)) %>%
  dplyr::group_by(OPPTAK) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup()

ggplot() + 
  geom_sf(data = opptaksomrader_DPS_trondheim)

nrow(opptaksomrader_DPS_trondheim)

# Beholder kun grensene mellom DPS-områdene innad i Trondheim
opptaksomrader_DPS_trondheim_borders <- cartography::getBorders(x = opptaksomrader_DPS_trondheim) %>%
  sf::st_set_crs(CRS) %>%
  sf::st_cast("LINESTRING") %>%
  sf::st_snap(grunnkrets_kart_trondheim, tolerance = 100) # Forlenger linjene slik at den treffer polygonen

# Nesten:
ggplot() +
  geom_sf(data = grunnkrets_kart_trondheim) +
  geom_sf(data = opptaksomrader_DPS_trondheim_borders)
# Må få slått sammen disse to

opptaksomrader_DPS_trondheim_poly <- sf::st_geometry(grunnkrets_kart_trondheim) %>%
  lwgeom::st_split(st_geometry(opptaksomrader_DPS_trondheim_borders)) %>% 
  sf::st_collection_extract("POLYGON") %>% 
  st_as_sf() %>%
  mutate(id = 1:n()) %>%
  dplyr::rename(geometry = x) # %>%

opptaksomrader_DPS_trondheim_poly <- opptaksomrader_DPS_trondheim_poly[1:2,]
opptaksomrader_DPS_trondheim_poly$OPPTAK <- c("Nidelv", "Nidaros")
opptaksomrader_DPS_trondheim_poly["id"] <- NULL

ggplot() +
  geom_sf(data = opptaksomrader_DPS_trondheim_poly[1,])
    }
# -

# ## Kristiansand

# +
if (tjeneste == "DPS"){
# Lager kart med kun grensen til Kristiansand
grunnkrets_kart_kristiansand <- grunnkrets_kart %>%
  dplyr::filter(KOMMUNENR == "4204") %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  sf::st_transform(crs = CRS)

# Slår sammen geometriene for DPS-områdene i Kristiansand (basert på postnummer) 
opptaksomrader_DPS_kristiansand <- opptaksomrader_KLASS_postnummer_2 %>%
  dplyr::filter(KOMMUNENR == "4204", 
                !is.na(OPPTAK)) %>%
  dplyr::group_by(OPPTAK) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup()

nrow(opptaksomrader_DPS_kristiansand)

ggplot() + 
  geom_sf(data = grunnkrets_kart_kristiansand, color = "red") +
  geom_sf(data = opptaksomrader_DPS_kristiansand)

nrow(opptaksomrader_DPS_kristiansand)

# Beholder kun grensene mellom DPS-områdene innad i Kristiansand
opptaksomrader_DPS_kristiansand_borders <- cartography::getBorders(x = opptaksomrader_DPS_kristiansand) %>%
  sf::st_set_crs(CRS) %>%
  sf::st_line_merge() %>%
  sf::st_cast("LINESTRING") %>% 
  sf::st_snap(grunnkrets_kart_kristiansand, tolerance = 100) # Forlenger linjene slik at den treffer polygonen

# sf::st_bbox(grunnkrets_kart_kristiansand)

#?sf::st_snap
# Nesten:
ggplot() +
  geom_sf(data = grunnkrets_kart_kristiansand) +
  geom_sf(data = opptaksomrader_DPS_kristiansand_borders) # 3, 4, 8, 9
# Må få slått sammen disse to


punkt_1a <- opptaksomrader_DPS_kristiansand_borders[3,] %>%
  sf::st_line_sample(sample = 1) %>% # 0= starpoint, 1 = endpoint
  sf::st_cast("POINT") %>%
  sf::st_coordinates() %>%
  data.frame() %>%
  sf::st_as_sf(coords = c('X', 'Y')) %>%
  sf::st_set_crs(CRS)

ggplot2::ggplot() +
  ggplot2::geom_sf(data = grunnkrets_kart_kristiansand) +
  ggplot2::geom_sf(data = opptaksomrader_DPS_kristiansand_borders) +
  ggplot2::geom_sf(data = punkt_1a, color = "red") 

punkt_2a <- opptaksomrader_DPS_kristiansand_borders[4,] %>%
  sf::st_line_sample(sample = 0) %>% # 1 = endpoint
  sf::st_cast("POINT") %>%
  sf::st_coordinates() %>%
  data.frame() %>%
  sf::st_as_sf(coords = c('X', 'Y')) %>%
  sf::st_set_crs(CRS)

ggplot2::ggplot() +
  ggplot2::geom_sf(data = grunnkrets_kart_kristiansand) +
  ggplot2::geom_sf(data = opptaksomrader_DPS_kristiansand_borders) +
  ggplot2::geom_sf(data = punkt_1a, color = "red") +
  ggplot2::geom_sf(data = punkt_2a, color = "blue") 

punkt_1b <- data.frame(sf::st_coordinates(grunnkrets_kart_kristiansand))[c(4923),]
punkt_1b <- punkt_1b %>%
  sf::st_as_sf(coords = c('X', 'Y')) %>%
  sf::st_set_crs(CRS) %>%
  dplyr::select(-L1, -L2) %>%
  sf::st_cast("POINT")

linje_1 <- rbind(punkt_1a, punkt_1b) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  sf::st_cast("LINESTRING")

punkt_2b <- data.frame(sf::st_coordinates(grunnkrets_kart_kristiansand))[c(4916),]
punkt_2b <- punkt_2b %>%
  sf::st_as_sf(coords = c('X', 'Y')) %>%
  sf::st_set_crs(CRS) %>%
  select(-L1, -L2) %>%
  sf::st_cast("POINT")

linje_2 <- rbind(punkt_2a, punkt_2b) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  sf::st_cast("LINESTRING")

ggplot2::ggplot() + 
  ggplot2::geom_sf(data = grunnkrets_kart_kristiansand) +
  ggplot2::geom_sf(data = opptaksomrader_DPS_kristiansand_borders) +
  ggplot2::geom_sf(data = linje_1, color = "red") +
  ggplot2::geom_sf(data = linje_2, color = "blue") +
  ggplot2::geom_sf(data = punkt_1a, color = "red") +
  ggplot2::geom_sf(data = punkt_2a, color = "blue") +
  ggplot2::geom_sf(data = punkt_2b, color = "blue") 

opptaksomrader_DPS_kristiansand_borders_2 <- opptaksomrader_DPS_kristiansand_borders %>%
  dplyr::select(geometry)
linjer <- rbind(opptaksomrader_DPS_kristiansand_borders_2, linje_1, linje_2)


opptaksomrader_DPS_kristiansand_poly <- sf::st_geometry(grunnkrets_kart_kristiansand) %>%
  lwgeom::st_split(sf::st_geometry(linjer)) %>% 
  # lwgeom::st_split(sf::st_geometry(alle_linjer)) %>% 
  sf::st_collection_extract("POLYGON") %>% 
  sf::st_as_sf() %>%
  dplyr::mutate(id = 1:n()) %>%
  dplyr::rename(geometry = x) # %>%

ggplot() +
  geom_sf(data = opptaksomrader_DPS_kristiansand_poly) 
# 1 = helt til venstre (Strømme), 2 = på toppen (Solvang), 3 = ?, 4 = midten (Solvang), 5 = helt til høyre (Strømme), 6 = ?

opptaksomrader_DPS_kristiansand_poly <- opptaksomrader_DPS_kristiansand_poly[c(1, 2, 4, 5),]
opptaksomrader_DPS_kristiansand_poly$OPPTAK <- c("Strømme", "Solvang", "Solvang", "Strømme") # OBS!!!! DOBBELTSJEKK DETTE
opptaksomrader_DPS_kristiansand_poly["id"] <- NULL

opptaksomrader_DPS_kristiansand_poly_2 <- opptaksomrader_DPS_kristiansand_poly %>%
  dplyr::group_by(OPPTAK) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) # %>%
      # sf::st_set_crs(CRS) 

ggplot2::ggplot() +
  ggplot2::geom_sf(data = opptaksomrader_DPS_kristiansand_poly_2) 

ggplot2::ggplot() +
  ggplot2::geom_sf(data = opptaksomrader_DPS_kristiansand_poly_2[1,])
    }
# -

# ### SSB fargepalett

ssb_farger <- klassR::GetKlass(614, output_style = "wide") %>%
  dplyr::rename(farge_nummer = code3, 
                HEX = name3, 
                farge = name2, 
                type = name1) %>%
  dplyr::select(-code1, -code2) %>%
  dplyr::filter(farge != "Hvit")

# ## Sjekker antall grunnkretser mot KLASS

grunnkrets_KLASS <- klassR::GetKlass(1, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code2, 
              GRUNNKRETS = name2)

nrow(grunnkrets_KLASS)

# +
if (tjeneste != "DPS"){
mangler_fra_KLASS <- grunnkrets_KLASS %>%
dplyr::filter(!GRUNNKRETSNUMMER %in% unique(opptaksomrader_KLASS$GRUNNKRETSNUMMER))

unique(mangler_fra_KLASS$name2)
nrow(mangler_fra_KLASS)
    }

if (tjeneste == "DPS"){
mangler_fra_KLASS <- grunnkrets_KLASS %>%
    dplyr::filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% kommuner_med_postkretser) %>%
dplyr::filter(!GRUNNKRETSNUMMER %in% unique(opptaksomrader_KLASS$GRUNNKRETSNUMMER))

unique(mangler_fra_KLASS$name2)
nrow(mangler_fra_KLASS)
    }

mangler_fra_KLASS %>%
arrange(GRUNNKRETSNUMMER) %>%
head()
# -

# ## Sjekker om noen grunnkretser mangler fra kartet

# OBS: dubletter i grunnkrets_kart?

nrow(grunnkrets_kart)
length(unique(grunnkrets_kart$GRUNNKRETSNUMMER))

# +
if (tjeneste != "DPS"){
test <- dplyr::left_join(opptaksomrader_KLASS, grunnkrets_kart, by = "GRUNNKRETSNUMMER") %>%
data.frame() %>%
dplyr::filter(is.na(KOMMUNENR))
    }

if (tjeneste == "DPS"){
test <- opptaksomrader_KLASS %>%
filter(!OPPTAK %in% c("Solvang", "Strømme", "Nidelv", "Nidaros")) %>%
dplyr::left_join(grunnkrets_kart, by = "GRUNNKRETSNUMMER") %>%
# test <- dplyr::left_join(opptaksomrader_KLASS, grunnkrets_kart, by = "GRUNNKRETSNUMMER") %>%
data.frame() %>%
# dplyr::filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% kommuner_med_postkretser) %>%
dplyr::filter(is.na(KOMMUNENR))
    }

# +
nrow(test)
unique(test$GRUNNKRETS_NAVN)

# test %>%
# head()

# +
# test %>%
# filter(GRUNNKRETS_NAVN == "Frydenlund")
# -

# ## Merger opptaksområder med grunnkretskart

# +
# opptaksomrader_KLASS %>%
# filter(OPPTAK == "Solvang") %>%
# distinct(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF)

# +
if (tjeneste != "DPS"){
opptaksomrader_KLASS_2_kart <- grunnkrets_kart %>%
dplyr::left_join(opptaksomrader_KLASS, by = "GRUNNKRETSNUMMER")
    }

if (tjeneste == "DPS"){
opptaksomrader_KLASS_2_kart <- grunnkrets_kart %>%
dplyr::filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% kommuner_med_postkretser) %>%
dplyr::left_join(opptaksomrader_KLASS, by = "GRUNNKRETSNUMMER") %>%
select(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF, REGION)
    
opptaksomrader_KLASS_DPS <- opptaksomrader_KLASS %>%
distinct(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF, REGION)
    
opptaksomrader_KLASS_2_kart_kristiansand <- opptaksomrader_DPS_kristiansand_poly_2 %>%
dplyr::left_join(opptaksomrader_KLASS_DPS, by = "OPPTAK") %>%
select(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF, REGION)    
    
opptaksomrader_KLASS_2_kart_trondheim <- opptaksomrader_DPS_trondheim_poly %>%
dplyr::left_join(opptaksomrader_KLASS_DPS, by = "OPPTAK") %>%
select(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF, REGION)    
    
opptaksomrader_KLASS_2_kart <- rbind(opptaksomrader_KLASS_2_kart, opptaksomrader_KLASS_2_kart_trondheim, opptaksomrader_KLASS_2_kart_kristiansand)    
    }

nrow(opptaksomrader_KLASS_2_kart)
# -

# # Lager opptaksområder for RHF

# +
start.time <- Sys.time()

opptaksomrader_RHF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(REGION, NAVN_RHF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup()
# -

# ### Legger til befolkning per opptaksområde

opptaksomrader_RHF <- opptaksomrader_RHF %>%
dplyr::left_join(befolkning_per_opptak,by = c("REGION" = "HelseReg"))

# ### Lagrer filen

if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  sfarrow::st_write_parquet(obj=opptaksomrader_RHF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_", tjeneste, "_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  opptaksomrader_SOM_RHF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, 
                                          "/Opptaksområder/opptaksomrader_", tjeneste, "_RHF_", filsti_med_uten_hav, "_", aargang, 
                                          "/opptaksomrader_", tjeneste, "_RHF_", filsti_med_uten_hav, "_", aargang, # OBS: trenger ikke å lage mappe inne i mappe?
                                          ".parquet")
  write_SSB(opptaksomrader_RHF, file = opptaksomrader_SOM_RHF_filsti, sf = TRUE)
}

# ## Visualiserer kartet

# +
if (utenhav == TRUE) {

ggplot() + 
geom_sf(data = opptaksomrader_RHF)

} else {

pal_RHF <- leaflet::colorFactor(ssb_farger$HEX, domain = as.factor(opptaksomrader_KLASS_2_kart$NAVN_RHF))

opptaksomrader_RHF <- opptaksomrader_RHF %>%
sf::st_transform(crs = 4326)

opptaksomrader_RHF_leaflet <- leaflet::leaflet(options = leaflet::leafletOptions(zoomControl = FALSE)) %>% 
   leaflet::addTiles() %>%
   leaflet::addPolygons(stroke = F, data = opptaksomrader_RHF,
                       # color = "green",
                       weight = 1,
                       fillColor = pal_RHF(opptaksomrader_RHF$NAVN_RHF),
                       fillOpacity = 0.5, smoothFactor = 0.5,
                       popup = paste0("Opptaksområde: ", opptaksomrader_RHF$NAVN_RHF, " / Befolkning: ", prettyNum(opptaksomrader_RHF$PERSONER, big.mark = " ", scientific = FALSE))) %>%
  leaflet::addLegend("bottomright", pal = pal_RHF, values = as.factor(opptaksomrader_RHF$NAVN_RHF), opacity = 1)

# Lagrer filen
# htmlwidgets::saveWidget(opptaksomrader_RHF_leaflet, file = paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_RHF_", aargang, ".html"), selfcontained=T)

opptaksomrader_RHF_leaflet
    }
# -

# # Lager opptaksområder for HF

opptaksomrader_HF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(ORGNR_HF, NAVN_HF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup()

# ### Legger til befolkning per opptaksområde

opptaksomrader_HF <- opptaksomrader_HF %>%
dplyr::left_join(befolkning_per_opptak,by = c("ORGNR_HF" = "HelseReg"))

# ## Lagrer filen

if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
sfarrow::st_write_parquet(obj=opptaksomrader_HF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_", tjeneste, "_HF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
opptaksomrader_HF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, 
                                       "/Opptaksområder/opptaksomrader_", tjeneste, "_HF_", filsti_med_uten_hav, "_", aargang, 
                                       "/opptaksomrader_", tjeneste, "_HF_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_HF, file = opptaksomrader_HF_filsti, sf = TRUE)
}

# ## Visualiserer kartet

# +
if (utenhav == TRUE) {

ggplot() + 
geom_sf(data = opptaksomrader_HF)

} else {
pal_HF <- leaflet::colorFactor(ssb_farger$HEX, domain = as.factor(opptaksomrader_KLASS_2_kart$NAVN_HF))

opptaksomrader_HF <- opptaksomrader_HF %>%
sf::st_transform(crs = 4326)

opptaksomrader_HF_leaflet <- leaflet::leaflet(options = leaflet::leafletOptions(zoomControl = FALSE)) %>% 
   leaflet::addTiles() %>%
   leaflet::addPolygons(stroke = F, data = opptaksomrader_HF,
                       # color = "green",
                       weight = 1,
                       fillColor = pal_HF(opptaksomrader_HF$NAVN_HF),
                       fillOpacity = 0.5, smoothFactor = 0.5,
                       popup = paste0("Opptaksområde: ", opptaksomrader_HF$NAVN_HF, " / Befolkning: ", prettyNum(opptaksomrader_HF$PERSONER, big.mark = " ", scientific = FALSE))) %>%
  leaflet::addLegend("bottomright", pal = pal_HF, values = as.factor(opptaksomrader_HF$NAVN_HF), opacity = 1)

# Lagrer filen
# htmlwidgets::saveWidget(opptaksomrader_HF_leaflet, file = paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_HF_", aargang, ".html"), selfcontained=T)

opptaksomrader_HF_leaflet
    }
# -

# # Lager opptaksområder for lokasjonsområder

opptaksomrader_lokasjon %>%
data.frame()

# +
if (tjeneste %in% c("SOM", "DPS")){
opptaksomrader_lokasjon <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(OPPTAK_NUMMER, OPPTAK) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup()

# Legger til befolkning per opptaksområde

opptaksomrader_lokasjon <- opptaksomrader_lokasjon %>%
dplyr::left_join(befolkning_per_opptak,by = c("OPPTAK_NUMMER" = "HelseReg"))

# Lagrer filen

paste0(arbeidsmappe_opptak, "opptaksomrader_", tjeneste, "_lokasjon_", filsti_med_uten_hav, "_", filsti_med_uten_hav, "_", aargang, ".parquet")

if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
sfarrow::st_write_parquet(obj=opptaksomrader_lokasjon, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_", tjeneste, "_lokasjon_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
opptaksomrader_lokasjon_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, 
                                         "/Opptaksområder/opptaksomrader_", tjeneste, "_lokasjon_", filsti_med_uten_hav, "_", aargang, 
                                         "/opptaksomrader_", tjeneste, "_lokasjon_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_lokasjon, file = opptaksomrader_lokasjon_filsti, sf = TRUE)
}

# Visualiserer kartet

if (utenhav == TRUE) {

ggplot() + 
geom_sf(data = opptaksomrader_lokasjon)

} else {
pal_lokasjon <- leaflet::colorFactor(ssb_farger$HEX, domain = as.factor(opptaksomrader_KLASS_2_kart$OPPTAK))

opptaksomrader_lokasjon <- opptaksomrader_lokasjon %>%
sf::st_transform(crs = 4326)

opptaksomrader_lokasjon_leaflet <- leaflet::leaflet(options = leaflet::leafletOptions(zoomControl = FALSE)) %>% 
   leaflet::addTiles() %>%
   leaflet::addPolygons(stroke = F, data = opptaksomrader_lokasjon,
                       # color = "green",
                       weight = 1,
                       fillColor = pal_lokasjon(opptaksomrader_lokasjon$OPPTAK),
                       fillOpacity = 0.5, smoothFactor = 0.5,
                       popup = paste0("Opptaksområde: ", opptaksomrader_lokasjon$OPPTAK, " / Befolkning: ", prettyNum(opptaksomrader_lokasjon$PERSONER, big.mark = " ", scientific = FALSE))) %>%
  leaflet::addLegend("bottomright", pal = pal_lokasjon, values = as.factor(opptaksomrader_lokasjon$OPPTAK), opacity = 1)

# Lagrer filen
# htmlwidgets::saveWidget(opptaksomrader_lokasjon_leaflet, file = paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_lokasjon_", aargang, ".html"), selfcontained=T)

opptaksomrader_lokasjon_leaflet
    }
    }
# -

# ## Sjekker om befolkning stemmer med offisielle befolkninstall

# +
# sum(T04317$PERSONER)

# # Sjekker om antall personer stemmer med tabell 04317
# sum(T04317$PERSONER)-sum(opptaksomrader_RHF$PERSONER)

# # Sjekker om antall personer stemmer med tabell 04317
# sum(T04317$PERSONER)-sum(opptaksomrader_HF$PERSONER)

# # Sjekker om antall personer stemmer med tabell 04317
# sum(T04317$PERSONER)-sum(opptaksomrader_lokasjon$PERSONER)
# -

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
