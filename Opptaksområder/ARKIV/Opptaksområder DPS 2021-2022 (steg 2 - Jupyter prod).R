# # Opptaksområder DPS
#
# Kjøres i prod etter at kjøringen har blitt gjort i RStudio (OBS: flytt alt til ett sted!)

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
library(sfarrow)    
library(htmlwidgets)
        })
# -

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

# ### Kodeliste for opptaksområder i spesialisthelsetjenesten (DPS)

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

opptaksomrader_KLASS_test <- opptaksomrader_KLASS %>%
select(OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
distinct()
# -

# ## DPS-områder

opptaksomrader_DPS_DPS <- sfarrow::st_read_parquet(paste0(arbeidsmappe_opptak, "opptaksomrader_DPS_DPS_", filsti_med_uten_hav, "_", aargang, ".parquet"))

ggplot() + 
geom_sf(data = opptaksomrader_DPS_DPS)

# ## DPS (HF)

opptaksomrader_DPS_HF <- opptaksomrader_DPS_DPS %>%
dplyr::left_join(opptaksomrader_KLASS_test, by = "OPPTAK")

# +
opptaksomrader_DPS_HF <- opptaksomrader_DPS_HF %>%
dplyr::group_by(NAVN_HF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)),
                   PERSONER = sum(PERSONER)) %>%
  dplyr::ungroup()

# Lagrer filen 
sfarrow::st_write_parquet(obj=opptaksomrader_DPS_HF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_DPS_HF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
# -

ggplot() + 
geom_sf(data = opptaksomrader_DPS_HF)

# ### DPS (RHF)

# +
opptaksomrader_DPS_RHF <- opptaksomrader_DPS_DPS %>%
dplyr::left_join(opptaksomrader_KLASS_test, by = "OPPTAK")

opptaksomrader_DPS_RHF <- opptaksomrader_DPS_RHF %>%
dplyr::group_by(NAVN_RHF) %>%
  dplyr::summarise(geometry = sf::st_union(sf::st_make_valid(geometry)),
                   PERSONER = sum(PERSONER)) %>%
  dplyr::ungroup()

# Lagrer filen 
sfarrow::st_write_parquet(obj=opptaksomrader_DPS_RHF, dsn=paste0(arbeidsmappe_opptak, "opptaksomrader_DPS_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet"))
# -

ggplot() + 
geom_sf(data = opptaksomrader_DPS_RHF)
