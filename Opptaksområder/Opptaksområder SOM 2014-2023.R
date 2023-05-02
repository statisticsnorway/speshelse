# -*- coding: utf-8 -*-
# # Opptaksområder SOM

# Kjøringen uten hav tar ca. 45 minutter (med den største kernelen)
#
# + OBS: har ikke kjørt for 2023 utenhav
# + OBS: ikke riktig totaltall 2014-2019
# + OBS: hvordan lagre htmlwidgets på dapla?

# ## Velger årgang

aargang <- 2023

# ## Velger tjenesteområde

tjeneste <- "SOM"

# ## Flate eller utenhav

# +
utenhav <- TRUE

if (utenhav == TRUE) {
filsti_med_uten_hav <- "utenhav"
    } else if (utenhav == FALSE) {
  filsti_med_uten_hav <- "flate"
}

filsti_med_uten_hav
# -

# ## Laster inn pakker 

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

# # Laster inn kart (grunnkrets)

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

# if (file.exists(arbeidsmappe_opptak)==FALSE) {
#   dir.create(arbeidsmappe_opptak)
# }

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

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken

if (aargang == 2017) {
grunnkrets_kart <- grunnkrets_kart %>%
dplyr::mutate(GRUNNKRETSNUMMER = case_when(
    GRUNNKRETSNUMMER == "00101609" ~ "07101609", # Storevahr har feil grunnkretsnummer i filen!
    TRUE ~ GRUNNKRETSNUMMER))
    }
# -

# ## SSB fargepalett

# +
Sys.setenv(no_proxy = "nexus.ssb.no, git-adm.ssb.no, i.test.ssb.no, i.ssb.no, data.ssb.no, api.github.com, github.com") # OBS: denne burde ikke være nødvendig?

ssb_farger <- klassR::GetKlass(614, output_style = "wide") %>%
  dplyr::rename(farge_nummer = code3, 
                HEX = name3, 
                farge = name2, 
                type = name1) %>%
  dplyr::select(-code1, -code2) %>%
  dplyr::filter(farge != "Hvit")
# -

# # Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk)
#
# + OBS: endre alle navn til CAPS LOCK?

opptaksomrader_KLASS <- klassR::GetKlass(629, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

nrow(opptaksomrader_KLASS)

# # Akuttmottak

# +
# opptaksomrader_KLASS %>%
# head()

# orgnr_foretak <- c(unique(opptaksomrader_KLASS$ORGNR_HF), "984027737")

# source("/ssb/bruker/rdn/fellesr/R/dynarev_uttrekk.R")
# con <- dynarev_uttrekk(con_ask = "con")

# vof_for <- dplyr::tbl(con, dbplyr::in_schema("DSBBASE", "SSB_FORETAK")) %>%
#   dplyr::filter(ORGNR %in% orgnr_foretak) %>%
#   dplyr::select(FORETAKS_NR, ORGNR, NAVN) %>%
#   dplyr::rename(ORGNR_FORETAK = ORGNR, 
#                 NAVN_FORETAK = NAVN)

# vof <- dplyr::tbl(con, dbplyr::in_schema("DSBBASE", "SSB_BEDRIFT")) %>%
#   dplyr::mutate(NAVN_BEDRIFT = paste0(NAVN, " ", KARAKTERISTIKK), 
#                 BEDRIFTS_NR_GDATO_year = year(BEDRIFTS_NR_GDATO), 
#                 RECORD_ED_year = year(RECORD_ED)) %>%
#   dplyr::filter(BEDRIFTS_NR_GDATO_year <= aargang,
#                 RECORD_ED_year >= aargang) %>%
#   dplyr::select(FORETAKS_NR, ORGNR, NAVN_BEDRIFT, SN07_1, SB_TYPE, F_ADRESSE1, F_POSTNR) %>%
#   dplyr::rename(ORGNR_BEDRIFT = ORGNR) %>%
#   dplyr::inner_join(vof_for, by = "FORETAKS_NR") %>%
#   dplyr::collect()

# opptak_test <- data.frame(unique(opptaksomrader_KLASS$OPPTAK))
# colnames(opptak_test)[1] <- "OPPTAK"

# opptak_test <- opptak_test %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
# OPPTAK == "Akershus" ~ "974631776", # c("974631776", "974706490", "974705192"),
# OPPTAK == "Stavanger" ~ "974703300",
# OPPTAK == "Haugesund" ~ "974724774",
# OPPTAK == "Kristiansund" ~ "974746948", 
# OPPTAK == "Molde" ~ "974745569",
# OPPTAK == "Ålesund" ~ "974747138",
# OPPTAK == "Volda" ~ "974747545",
# OPPTAK == "Orkdal" ~ "974329506",
# OPPTAK == "Bodø" ~ "974795361",
# OPPTAK == "Narvik" ~ "974795396",
# OPPTAK == "Sandnessjøen" ~ "974795477",
# OPPTAK == "Mosjøen" ~ "974795485",
# OPPTAK == "Mo i Rana" ~ "974795515",
# OPPTAK == "Harstad" ~ "974795639",
# OPPTAK == "Lofoten" ~ "974795558",
# OPPTAK == "Vesterålen" ~ "974795574",
# OPPTAK == "Østfold" ~ "974633752",
# OPPTAK == "Drammen" ~ "974631326",
# OPPTAK == "Kongsberg" ~ "974631385",
# OPPTAK == "Bærum" ~ "974705788",
# OPPTAK == "Kongsvinger" ~ "974631776",
# OPPTAK == "Gjøvik" ~ "974632535",
# OPPTAK == "Elverum-Hamar" ~ "974631768",
# OPPTAK == "Lillehammer" ~ "874632562",
# OPPTAK == "Tynset" ~ "974725215",
# OPPTAK == "Vestfold" ~ "823247672",
# OPPTAK == "Skien" ~ "974633191",
# OPPTAK == "Notodden" ~ "974633159",
# OPPTAK == "Arendal" ~ "974631091",
# OPPTAK == "Kristiansand" ~ "974733013",
# OPPTAK == "Flekkefjord" ~ "974595214",
# OPPTAK == "Haraldsplass" ~ "974316285", # 924913061?
# OPPTAK == "Førde" ~ "974744570",
# OPPTAK == "Stord" ~ "974742985",
# OPPTAK == "Odda" ~ "974743086",
# OPPTAK == "Voss" ~ "974743272",
# OPPTAK == "Lærdal" ~ "974745089",
# OPPTAK == "Nordfjord" ~ "974745364",
# OPPTAK == "St. Olavs hospital" ~ "974749025",
# OPPTAK == "Levanger" ~ "974754118",
# OPPTAK == "Namsos" ~ "974753898",
# OPPTAK == "Tromsø" ~ "974795787",
# OPPTAK == "Ringerike" ~ "974631407",
# OPPTAK == "Haukeland" ~ "974557746",
# OPPTAK == "Hammerfest" ~ "974795833",
# OPPTAK == "Kirkenes" ~ "974795930",
# OPPTAK == "Oslo universitetssykehus" ~ "974588951",
# OPPTAK == "Diakonhjemmet" ~ "974116804", # ???
# OPPTAK == "Lovisenberg" ~ "974207532", # ???

# TRUE ~ ""
# ))

# opptak_test_1 <- opptak_test %>%
# dplyr::filter(OPPTAK %in% c("Oslo universitetssykehus", 
#                            "Akershus", 
#                            "Østfold", 
#                            "Elverum-Hamar")) %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
#     OPPTAK == "Elverum-Hamar" ~ "974724960", 
#     OPPTAK == "Østfold" ~ "974633655", # 974633698
#     OPPTAK == "Akershus" ~ "974706490", # 974705192
#     OPPTAK == "Oslo universitetssykehus" ~ "974589095", # 874716782 / 998152291

# TRUE ~ OPPTAK_ORGNR
# ))

# opptak_test_2 <- opptak_test %>%
# dplyr::filter(OPPTAK %in% c("Oslo universitetssykehus", 
#                            "Akershus", 
#                            "Østfold")) %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
#     OPPTAK == "Østfold" ~ "974633698",
#     OPPTAK == "Akershus" ~ "974705192",
#     OPPTAK == "Oslo universitetssykehus" ~ "874716782", # 998152291

# TRUE ~ OPPTAK_ORGNR
# ))

# opptak_test_3 <- opptak_test %>%
# dplyr::filter(OPPTAK %in% c("Oslo universitetssykehus")) %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
#     OPPTAK == "Oslo universitetssykehus" ~ "998152291",
# TRUE ~ OPPTAK_ORGNR
# ))

# opptak_test_alle <- rbind(opptak_test, opptak_test_1, opptak_test_2, opptak_test_3)

# # vof %>%
# # dplyr::filter(SN07_1 == "86.101", 
# #              grepl("SOMATIKK", NAVN_BEDRIFT), 
# #              ORGNR_FORETAK == "997005562")

# # head(opptak_test_alle)
# # head(vof)

# nrow(opptak_test_alle)

# opptak_test_alle_2 <- opptak_test_alle %>%
# dplyr::left_join(vof, by = c("OPPTAK_ORGNR" = "ORGNR_BEDRIFT")) %>%
# dplyr::select(-FORETAKS_NR, -SB_TYPE)

# opptak_test_alle_2
# -

# ## Sjekker antall grunnkretser mot KLASS

# +
grunnkrets_KLASS <- klassR::GetKlass(1, output_style = "wide", date = c(paste0(aargang, "-01-01")))

nrow(grunnkrets_KLASS)

mangler_fra_KLASS <- grunnkrets_KLASS %>%
dplyr::filter(!code2 %in% unique(opptaksomrader_KLASS$GRUNNKRETSNUMMER))

unique(mangler_fra_KLASS$name2)
nrow(mangler_fra_KLASS)
head(mangler_fra_KLASS)
# -

# ## Sjekker om noen grunnkretser mangler fra kartet

# OBS: dubletter i grunnkrets_kart?

# +
nrow(grunnkrets_kart)
length(unique(grunnkrets_kart$GRUNNKRETSNUMMER))

test <- dplyr::left_join(opptaksomrader_KLASS, grunnkrets_kart, by = "GRUNNKRETSNUMMER") %>%
data.frame() %>%
dplyr::filter(is.na(KOMMUNENR))

nrow(test)
unique(test$GRUNNKRETS_NAVN)

# colnames(grunnkrets_kart)

# grunnkrets_kart %>%
# dplyr::filter(GRUNNKRETSNUMMER == "18041014")
# -

# # Henter befolkningstall fra tabell 04317

# OBS: erstatt med egen fil laget fra befolkningsregisteret? (for å få med under og over 18 år?)

befolkning_per_opptaksomrade <- read_SSB(paste0("ssb-prod-helse-speshelse-data-kilde/felles/befolkning_per_opptaksomrade/", aargang, "/befolk_hf_", aargang, ".parquet"))

T04317 <- PxWebApiData::ApiData(04317, ContentsCode = "Personer", 
                                Grunnkretser = TRUE, 
                                Tid = as.character(aargang)) [[2]] %>%
  dplyr::filter(!is.na(value)) %>%
  dplyr::rename(GRUNNKRETSNUMMER = Grunnkretser,
                PERSONER = value) %>%
  dplyr::select(GRUNNKRETSNUMMER, PERSONER)

# +
# head(T04317)

# opptaksomrader_KLASS_2 <- opptaksomrader_KLASS %>%
# dplyr::left_join(T04317, by = "GRUNNKRETSNUMMER") %>%
# dplyr::mutate(PERSONER = tidyr::replace_na(PERSONER, 0))

# opptaksomrader_KLASS_2 %>%
# dplyr::filter(is.na(OPPTAK), 
#              substr(GRUNNKRETSNUMMER, 5, 8) != "9999")

# +
# # Sjekker om antall personer stemmer med tabell 04317
# sum(T04317$PERSONER)-sum(opptaksomrader_KLASS_2$PERSONER)

# opptaksomrader_KLASS_3 <- opptaksomrader_KLASS %>%
# dplyr::full_join(T04317, by = "GRUNNKRETSNUMMER") %>%
# dplyr::mutate(PERSONER = tidyr::replace_na(PERSONER, 0))

# opptaksomrader_KLASS_3 %>%
# dplyr::filter(is.na(OPPTAK)) %>%
# # dplyr::arrange(GRUNNKRETSNUMMER)
# dplyr::arrange(desc(PERSONER))
# -

# # Uoppgitt grunnkrets

# +
# opptaksomrader_KLASS_3 %>%
# filter(GRUNNKRETSNUMMER == "01019999")

# T04317 %>%
# dplyr::filter(!GRUNNKRETSNUMMER %in% unique(opptaksomrader_KLASS_2$GRUNNKRETSNUMMER), 
#              substr(GRUNNKRETSNUMMER, 5, 8) != "9999")

# T04317 %>%
# dplyr::filter(!GRUNNKRETSNUMMER %in% unique(opptaksomrader_KLASS_2$GRUNNKRETSNUMMER))
# -

# ## Merger opptaksområder med grunnkretskart

# +
opptaksomrader_KLASS_2_kart <- grunnkrets_kart %>%
dplyr::left_join(opptaksomrader_KLASS, by = "GRUNNKRETSNUMMER")

# nrow(opptaksomrader_KLASS_2_kart)

# sum(opptaksomrader_KLASS_2_kart$PERSONER)
# -

# # Lager opptaksområder for RHF

# +
start.time <- Sys.time()

befolkning_per_opptaksomrade_RHF <- befolkning_per_opptaksomrade %>%
filter(TJENESTE == "SOM", LEVEL == "RHF", ALDER_KODE != "999", KJOENN != "0") %>%
group_by(ORGNR_RHF, NAVN_RHF) %>%
summarise(PERSONER = sum(PERSONER))

# # Beregner befolkning #
# opptaksomrader_KLASS_2_RHF <- opptaksomrader_KLASS_2 %>%
# dplyr::group_by(NAVN_RHF) %>%
# dplyr::summarise(PERSONER = sum(PERSONER))

# unique(sf::st_geometry_type(sf::st_geometry(opptaksomrader_KLASS_2_kart)))

opptaksomrader_RHF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(NAVN_RHF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup() %>%
dplyr::left_join(befolkning_per_opptaksomrade_RHF, by = "NAVN_RHF")

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  sfarrow::st_write_parquet(obj=opptaksomrader_RHF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  opptaksomrader_SOM_RHF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_RHF, file = opptaksomrader_SOM_RHF_filsti, sf = TRUE)
}
# -

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

# +
befolkning_per_opptaksomrade_HF <- befolkning_per_opptaksomrade %>%
filter(TJENESTE == "SOM", LEVEL == "HF", ALDER_KODE != "999", KJOENN != "0") %>%
group_by(ORGNR_RHF, NAVN_RHF, ORGNR_HF, NAVN_HF) %>%
summarise(PERSONER = sum(PERSONER))

# # Beregner befolkning #
# opptaksomrader_KLASS_2_HF <- opptaksomrader_KLASS_2 %>%
# dplyr::group_by(NAVN_HF) %>%
# dplyr::summarise(PERSONER = sum(PERSONER))

opptaksomrader_HF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(NAVN_HF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup() %>%
  dplyr::left_join(befolkning_per_opptaksomrade_HF, by = "NAVN_HF")

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
sfarrow::st_write_parquet(obj=opptaksomrader_HF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
opptaksomrader_SOM_HF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_HF, file = opptaksomrader_SOM_HF_filsti, sf = TRUE)
}
# -

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

# +
head(befolkning_per_opptaksomrade)

befolkning_per_opptaksomrade_lokasjon <- befolkning_per_opptaksomrade %>%
filter(TJENESTE == "SOM", LEVEL == "Lokasjon", ALDER_KODE != "999", KJOENN != "0") %>%
group_by(OPPTAK_NUMMER, OPPTAK) %>%
summarise(PERSONER = sum(PERSONER))

# # Beregner befolkning #
# opptaksomrader_KLASS_2_OPPTAK <- opptaksomrader_KLASS_2 %>%
# dplyr::group_by(OPPTAK) %>%
# dplyr::summarise(PERSONER = sum(PERSONER))

opptaksomrader_lokasjon <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(OPPTAK) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup() %>%
  dplyr::left_join(befolkning_per_opptaksomrade_lokasjon, by = "OPPTAK")

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
sfarrow::st_write_parquet(obj=opptaksomrader_lokasjon, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
opptaksomrader_SOM_lokasjon_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_lokasjon, file = opptaksomrader_SOM_lokasjon_filsti, sf = TRUE)
}
# -

# ## Visualiserer kartet

# +
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
# -

sum(T04317$PERSONER)

# Sjekker om antall personer stemmer med tabell 04317
sum(T04317$PERSONER)-sum(opptaksomrader_RHF$PERSONER)

# Sjekker om antall personer stemmer med tabell 04317
sum(T04317$PERSONER)-sum(opptaksomrader_HF$PERSONER)

# Sjekker om antall personer stemmer med tabell 04317
sum(T04317$PERSONER)-sum(opptaksomrader_lokasjon$PERSONER)

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
