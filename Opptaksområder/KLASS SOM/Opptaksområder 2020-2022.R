# # Opptaksområder

# ### Laster inn pakker 

# +
sf::sf_use_s2(FALSE)

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
        })
# -

# ### Velger årgang

aargang <- 2018

# ### Filstier

# +
arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"
arbeidsmappe_kart <- paste0(arbeidsmappe, "kart/", aargang, "/")

arbeidsmappe_opptak <- paste0(arbeidsmappe, "opptaksomrader/", aargang, "/")

if (file.exists(arbeidsmappe_opptak)==FALSE) {
  dir.create(arbeidsmappe_opptak)
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

# ## Laster inn kart (grunnkrets)

# +
start.time <- Sys.time()

  rename_geometry <- function(g, name){
    current = attr(g, "sf_column")
    names(g)[names(g)==current] = name
    sf::st_geometry(g)=name
    g
  }

# grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_utenhav_", aargang, ".parquet")
grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_flate_", aargang, ".parquet")

# Lese inn filen som parquet med sfarrow
grunnkrets_kart <- sfarrow::st_read_parquet(grunnkrets_kart_filsti) %>%
sf::st_zm(drop = T) %>%
sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = 4326) %>%
  dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS)

grunnkrets_kart <- rename_geometry(grunnkrets_kart, "geometry")
sf::st_geometry(grunnkrets_kart) <- "geometry"

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
# -

# ## Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk)
#
# + OBS: endre alle navn til CAPS LOCK?

# +
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

# +
# opptaksomrader_KLASS %>%
# head()

# orgnr_foretak <- c(unique(opptaksomrader_KLASS$ORGNR_HF), "984027737")

# +
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

# # colnames(vof)

# # colnames(vof)

# # rapporteringsenheter_vof <- dplyr::left_join(vof, rapporteringsenheter, by = c("ORGNR_FORETAK")) %>%
# #   dplyr::select(-FORETAKS_NR, -NAVN)

# # # Antall underenheter (virksomheter) til rapporteringsenhetene (foretak)
# # nrow(rapporteringsenheter_vof)

# +
# opptak_test <- data.frame(unique(opptaksomrader_KLASS$OPPTAK))
# colnames(opptak_test)[1] <- "OPPTAK"

# opptak_test <- opptak_test %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
# OPPTAK == "Akershus" ~ "974631776", # c("974631776", "974706490", "974705192"),
# OPPTAK == "Stavanger" ~ "974703300",
# OPPTAK == "Haugesund" ~ "974724774",
# OPPTAK == "Kristiansund" ~ "974724774",
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
# OPPTAK == "OSLO UNIVERSITETSSYKEHUS HF" ~ "974588951",
# OPPTAK == "DIAKONHJEMMET SYKEHUS AS" ~ "974116804", # ???
# OPPTAK == "LOVISENBERG DIAKONALE SYKEHUS AS" ~ "974207532", # ???
    
# TRUE ~ ""
# ))

# opptak_test_1 <- opptak_test %>%
# dplyr::filter(OPPTAK %in% c("OSLO UNIVERSITETSSYKEHUS HF", 
#                            "Akershus", 
#                            "Østfold", 
#                            "St. Olavs hospital", 
#                            "Elverum-Hamar")) %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
# OPPTAK == "St. Olavs hospital" ~ "974749505", 
#     OPPTAK == "Elverum-Hamar" ~ "974724960", 
#     OPPTAK == "Østfold" ~ "974633655", # 974633698
#     OPPTAK == "Akershus" ~ "974706490", # 974705192
#     OPPTAK == "OSLO UNIVERSITETSSYKEHUS HF" ~ "974589095", # 874716782 / 998152291
    
# TRUE ~ OPPTAK_ORGNR
# ))

# opptak_test_2 <- opptak_test %>%
# dplyr::filter(OPPTAK %in% c("OSLO UNIVERSITETSSYKEHUS HF", 
#                            "Akershus", 
#                            "Østfold")) %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
#     OPPTAK == "Østfold" ~ "974633698",
#     OPPTAK == "Akershus" ~ "974705192",
#     OPPTAK == "OSLO UNIVERSITETSSYKEHUS HF" ~ "874716782", # 998152291
    
# TRUE ~ OPPTAK_ORGNR
# ))

# opptak_test_3 <- opptak_test %>%
# dplyr::filter(OPPTAK %in% c("OSLO UNIVERSITETSSYKEHUS HF")) %>%
# dplyr::mutate(OPPTAK_ORGNR = case_when(
#     OPPTAK == "OSLO UNIVERSITETSSYKEHUS HF" ~ "998152291",
# TRUE ~ OPPTAK_ORGNR
# ))

# opptak_test_alle <- rbind(opptak_test, opptak_test_1, opptak_test_2, opptak_test_3)

# +
# vof %>%
# dplyr::filter(SN07_1 == "86.101", 
#              grepl("SOMATIKK", NAVN_BEDRIFT),  
#              grepl("AHUS", NAVN_BEDRIFT))

# +
# # head(opptak_test_alle)
# # head(vof)

# nrow(opptak_test_alle)

# opptak_test_alle_2 <- opptak_test_alle %>%
# dplyr::left_join(vof, by = c("OPPTAK_ORGNR" = "ORGNR_BEDRIFT")) %>%
# dplyr::select(-FORETAKS_NR, -SB_TYPE)

# # opptak_test_alle_2
# -

# ### Sjekker antall grunnkretser mot KLASS

# +
grunnkrets_KLASS <- klassR::GetKlass(1, output_style = "wide", date = c(paste0(aargang, "-01-01")))

nrow(grunnkrets_KLASS)
head(grunnkrets_KLASS)

mangler_fra_KLASS <- grunnkrets_KLASS %>%
dplyr::filter(!code2 %in% unique(opptaksomrader_KLASS$GRUNNKRETSNUMMER))

unique(mangler_fra_KLASS$name2)
nrow(mangler_fra_KLASS)
# -

# ### Sjekker om noen grunnkretser mangler fra kartet

# +
test <- dplyr::left_join(opptaksomrader_KLASS, grunnkrets_kart, by = "GRUNNKRETSNUMMER") %>%
data.frame() %>%
dplyr::filter(is.na(KOMMUNENR))

unique(test$GRUNNKRETS_NAVN)

# +
# colnames(grunnkrets_kart)

# grunnkrets_kart %>%
# dplyr::filter(GRUNNKRETSNUMMER == "18041014")
# -

# ## Henter befolkningstall fra tabell 04317
#
# OBS: erstatt med egen fil laget fra befolkningsregisteret? (for å få med under og over 18 år?)

T04317 <- PxWebApiData::ApiData(04317, ContentsCode = "Personer", 
                                Grunnkretser = TRUE, 
                                Tid = as.character(aargang)) [[2]] %>%
  dplyr::filter(!is.na(value)) %>%
  dplyr::rename(GRUNNKRETSNUMMER = Grunnkretser,
                PERSONER = value) %>%
  dplyr::select(GRUNNKRETSNUMMER, PERSONER)

# +
opptaksomrader_KLASS_2 <- opptaksomrader_KLASS %>%
dplyr::full_join(T04317, by = "GRUNNKRETSNUMMER") %>%
dplyr::mutate(PERSONER = tidyr::replace_na(PERSONER, 0))

# Sjekker om antall personer stemmer med tabell 04317
sum(T04317$PERSONER)-sum(opptaksomrader_KLASS_2$PERSONER)
# -

# ### Merger opptaksområder med grunnkretskart

opptaksomrader_KLASS_2_kart <- grunnkrets_kart %>%
dplyr::left_join(opptaksomrader_KLASS_2, by = "GRUNNKRETSNUMMER")

# ## Lager opptaksområder for RHF

# +
# unique(sf::st_geometry_type(sf::st_geometry(opptaksomrader_KLASS_2_kart)))

opptaksomrader_RHF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(NAVN_RHF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)),
                   PERSONER = sum(PERSONER)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(PERSONER = prettyNum(PERSONER, big.mark = " ", scientific = FALSE))

# +
pal_RHF <- leaflet::colorFactor(ssb_farger$HEX, domain = as.factor(opptaksomrader_KLASS_2_kart$NAVN_RHF))

opptaksomrader_RHF_leaflet <- leaflet::leaflet(options = leaflet::leafletOptions(zoomControl = FALSE)) %>% 
   leaflet::addTiles() %>%
   leaflet::addPolygons(stroke = F, data = opptaksomrader_RHF,
                       # color = "green",
                       weight = 1,
                       fillColor = pal_RHF(opptaksomrader_RHF$NAVN_RHF),
                       fillOpacity = 0.5, smoothFactor = 0.5,
                       popup = paste0("Opptaksområde: ", opptaksomrader_RHF$NAVN_RHF, " / Befolkning: ", prettyNum(opptaksomrader_RHF$PERSONER, big.mark = " ", scientific = FALSE))) %>%
  leaflet::addLegend("bottomright", pal = pal_RHF, values = as.factor(opptaksomrader_RHF$NAVN_RHF), opacity = 1)

opptaksomrader_RHF_leaflet
# -

# ## Lager opptaksområder for HF

opptaksomrader_HF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(NAVN_HF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)),
                   PERSONER = sum(PERSONER)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(PERSONER = prettyNum(PERSONER, big.mark = " ", scientific = FALSE))

# +
pal_HF <- leaflet::colorFactor(ssb_farger$HEX, domain = as.factor(opptaksomrader_KLASS_2_kart$NAVN_HF))

opptaksomrader_HF_leaflet <- leaflet::leaflet(options = leaflet::leafletOptions(zoomControl = FALSE)) %>% 
   leaflet::addTiles() %>%
   leaflet::addPolygons(stroke = F, data = opptaksomrader_HF,
                       # color = "green",
                       weight = 1,
                       fillColor = pal_HF(opptaksomrader_HF$NAVN_HF),
                       fillOpacity = 0.5, smoothFactor = 0.5,
                       popup = paste0("Opptaksområde: ", opptaksomrader_HF$NAVN_HF, " / Befolkning: ", prettyNum(opptaksomrader_HF$PERSONER, big.mark = " ", scientific = FALSE))) %>%
  leaflet::addLegend("bottomright", pal = pal_HF, values = as.factor(opptaksomrader_HF$NAVN_HF), opacity = 1)

opptaksomrader_HF_leaflet
# -

# ## Lager opptaksområder for lokasjonsområder

opptaksomrader_lokasjon <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(OPPTAK) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)),
                   PERSONER = sum(PERSONER)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(PERSONER = prettyNum(PERSONER, big.mark = " ", scientific = FALSE))

# +
pal_lokasjon <- leaflet::colorFactor(ssb_farger$HEX, domain = as.factor(opptaksomrader_KLASS_2_kart$OPPTAK))

opptaksomrader_lokasjon_leaflet <- leaflet::leaflet(options = leaflet::leafletOptions(zoomControl = FALSE)) %>% 
   leaflet::addTiles() %>%
   leaflet::addPolygons(stroke = F, data = opptaksomrader_lokasjon,
                       # color = "green",
                       weight = 1,
                       fillColor = pal_lokasjon(opptaksomrader_lokasjon$OPPTAK),
                       fillOpacity = 0.5, smoothFactor = 0.5,
                       popup = paste0("Opptaksområde: ", opptaksomrader_lokasjon$OPPTAK, " / Befolkning: ", prettyNum(opptaksomrader_lokasjon$PERSONER, big.mark = " ", scientific = FALSE))) %>%
  leaflet::addLegend("bottomright", pal = pal_lokasjon, values = as.factor(opptaksomrader_lokasjon$OPPTAK), opacity = 1)

opptaksomrader_lokasjon_leaflet
# -

# ### Lagrer filene

# +
# htmlwidgets::saveWidget(opptaksomrader_HF_leaflet, file = paste0(here::here(), "/Opptaksområder/opptaksområder_HF.html"), selfcontained=T)

# +
sfarrow::st_write_parquet(obj=opptaksomrader_RHF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_RHF_", aargang, ".parquet"))

sfarrow::st_write_parquet(obj=opptaksomrader_HF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_HF_", aargang, ".parquet"))

sfarrow::st_write_parquet(obj=opptaksomrader_lokasjon, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_SOM_lokasjon_", aargang, ".parquet"))
