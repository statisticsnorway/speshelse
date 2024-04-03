# # Opptaksområder PHV
#
# + OBS: har ikke kjørt for 2023 utenhav

# ### Velger årgang

aargang <- 2021

# ### Flate eller utenhav

# +
utenhav <- FALSE

if (utenhav == TRUE) {
filsti_med_uten_hav <- "utenhav"
    } else if (utenhav == FALSE) {
  filsti_med_uten_hav <- "flate"
}

filsti_med_uten_hav
# -

# ### Laster inn pakker 

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


# if (file.exists(arbeidsmappe_opptak)==FALSE) {
#   dir.create(arbeidsmappe_opptak)
# }
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

# Lese inn filen som parquet med sfarrow
grunnkrets_kart <- grunnkrets_kart %>%
sf::st_zm(drop = T) %>%
sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = CRS) %>%
  dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS)

grunnkrets_kart <- rename_geometry(grunnkrets_kart, "geometry")
sf::st_geometry(grunnkrets_kart) <- "geometry"

# OBS
# grunnkrets_kart <- grunnkrets_kart %>%
# dplyr::group_by(GRUNNKRETSNUMMER, KOMMUNENR, FYLKE) %>%
# dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)))

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

# ## Kodeliste for opptaksområder i spesialisthelsetjenesten (PHV)
#
# + OBS: endre alle navn til CAPS LOCK?

# +
opptaksomrader_KLASS <- klassR::GetKlass(630, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3, 
             # OPPTAK_NUMMER = code3, 
             # OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

nrow(opptaksomrader_KLASS)
# -

# ### Sjekker antall grunnkretser mot KLASS

# +
grunnkrets_KLASS <- klassR::GetKlass(1, output_style = "wide", date = c(paste0(aargang, "-01-01")))

nrow(grunnkrets_KLASS)

mangler_fra_KLASS <- grunnkrets_KLASS %>%
dplyr::filter(!code2 %in% unique(opptaksomrader_KLASS$GRUNNKRETSNUMMER))

unique(mangler_fra_KLASS$name2)
nrow(mangler_fra_KLASS)
head(mangler_fra_KLASS)
# -

# ### Sjekker om noen grunnkretser mangler fra kartet

# +
test <- dplyr::left_join(opptaksomrader_KLASS, grunnkrets_kart, by = "GRUNNKRETSNUMMER") %>%
data.frame() %>%
dplyr::filter(is.na(KOMMUNENR))

nrow(test)
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
dplyr::left_join(T04317, by = "GRUNNKRETSNUMMER") %>%
dplyr::mutate(PERSONER = tidyr::replace_na(PERSONER, 0))

# opptaksomrader_KLASS_2 %>% head()

opptaksomrader_KLASS_2 %>%
dplyr::filter(is.na(ORGNR_HF), 
             substr(GRUNNKRETSNUMMER, 5, 8) != "9999")

# Sjekker om antall personer stemmer med tabell 04317
sum(T04317$PERSONER)-sum(opptaksomrader_KLASS_2$PERSONER)

# +
opptaksomrader_KLASS_3 <- opptaksomrader_KLASS %>%
dplyr::full_join(T04317, by = "GRUNNKRETSNUMMER") %>%
dplyr::mutate(PERSONER = tidyr::replace_na(PERSONER, 0))

opptaksomrader_KLASS_3 %>%
dplyr::filter(is.na(ORGNR_HF)) %>%
# dplyr::arrange(desc(GRUNNKRETSNUMMER))
dplyr::arrange(desc(PERSONER)) %>%
head()

# Uoppgitt grunnkrets

# +
# T04317 %>%
# dplyr::filter(!GRUNNKRETSNUMMER %in% unique(opptaksomrader_KLASS_2$GRUNNKRETSNUMMER), 
#              substr(GRUNNKRETSNUMMER, 5, 8) != "9999")

# T04317 %>%
# dplyr::filter(!GRUNNKRETSNUMMER %in% unique(opptaksomrader_KLASS_2$GRUNNKRETSNUMMER))
# -

# ### Merger opptaksområder med grunnkretskart

opptaksomrader_KLASS_2_kart <- grunnkrets_kart %>%
dplyr::left_join(opptaksomrader_KLASS_2, by = "GRUNNKRETSNUMMER")

# ## Lager opptaksområder for RHF

start.time <- Sys.time()

# +
# Beregner befolkning #
opptaksomrader_KLASS_2_RHF <- opptaksomrader_KLASS_2 %>%
dplyr::group_by(NAVN_RHF) %>%
dplyr::summarise(PERSONER = sum(PERSONER))

# unique(sf::st_geometry_type(sf::st_geometry(opptaksomrader_KLASS_2_kart)))

opptaksomrader_RHF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(NAVN_RHF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup() %>%
  dplyr::left_join(opptaksomrader_KLASS_2_RHF, by = "NAVN_RHF")


# # Lagrer filen
# sfarrow::st_write_parquet(obj=opptaksomrader_RHF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_PHV_RHF_", aargang, ".parquet"))

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  sfarrow::st_write_parquet(obj=opptaksomrader_RHF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_PHV_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
  opptaksomrader_PHV_RHF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_PHV_RHF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_PHV_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_RHF, file = opptaksomrader_PHV_RHF_filsti, sf = TRUE)
}
# -

# ### Visualiserer kartet

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
# htmlwidgets::saveWidget(opptaksomrader_RHF_leaflet, file = paste0(arbeidsmappe_opptak, "opptaksomrader_PHV_RHF_", aargang, ".html"), selfcontained=T)

opptaksomrader_RHF_leaflet
    }
# -

# ## Lager opptaksområder for HF

# +
# Beregner befolkning #
opptaksomrader_KLASS_2_HF <- opptaksomrader_KLASS_2 %>%
dplyr::group_by(NAVN_HF) %>%
dplyr::summarise(PERSONER = sum(PERSONER))

opptaksomrader_HF <- opptaksomrader_KLASS_2_kart %>%
  dplyr::group_by(NAVN_HF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry))) %>%
  dplyr::ungroup() %>%
  dplyr::left_join(opptaksomrader_KLASS_2_HF, by = "NAVN_HF")

# sfarrow::st_write_parquet(obj=opptaksomrader_HF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_PHV_HF_", aargang, ".parquet"))

# Lagrer filen
if (grepl("onprem", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
sfarrow::st_write_parquet(obj=opptaksomrader_HF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_PHV_HF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
} else if (grepl("dapla", Sys.getenv("JUPYTER_IMAGE_SPEC"))) {
opptaksomrader_PHV_HF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_PHV_HF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_PHV_HF_", filsti_med_uten_hav, "_", aargang, ".parquet")
  write_SSB(opptaksomrader_HF, file = opptaksomrader_PHV_HF_filsti, sf = TRUE)
}
# -

# ### Visualiserer kartet

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
# htmlwidgets::saveWidget(opptaksomrader_HF_leaflet, file = paste0(arbeidsmappe_opptak, "opptaksomrader_PHV_HF_", aargang, ".html"), selfcontained=T)

opptaksomrader_HF_leaflet
    }
# -

sum(T04317$PERSONER)

# Sjekker om antall personer stemmer med tabell 04317
sum(T04317$PERSONER)-sum(opptaksomrader_RHF$PERSONER)

# Sjekker om antall personer stemmer med tabell 04317
sum(T04317$PERSONER)-sum(opptaksomrader_HF$PERSONER)

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
