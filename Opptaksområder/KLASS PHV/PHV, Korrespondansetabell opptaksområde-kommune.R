# # Korrespondansetabell opptaksområde-kommune

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
# -

# ## Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk)

# +
opptaksomrader_KLASS <- klassR::GetKlass(630, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3,
              ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1) %>%
dplyr::mutate(GRUNNKRETSNUMMER = str_pad(GRUNNKRETSNUMMER, width = 8, "left", pad = "0"))

nrow(opptaksomrader_KLASS)

# +
kommune_KLASS <- klassR::GetKlass(131, date = c(paste0(aargang, "-01-01"))) %>%
dplyr::filter(!code %in% c("9999")) %>%
dplyr::rename(KOMMUNENUMMER = code, 
             KOMMUNENAVN = name) %>%
dplyr::select(KOMMUNENUMMER, KOMMUNENAVN)

nrow(kommune_KLASS)
# -

# ## Lager korrespondanse mellom opptaksområde og kommune 
#
# Inneholder noen dubletter

opptaksomrader_KLASS_2 <- opptaksomrader_KLASS %>%
dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
dplyr::select(ORGNR_HF, NAVN_HF, KOMMUNENUMMER) %>%
dplyr::distinct() %>%
dplyr::left_join(kommune_KLASS, by = "KOMMUNENUMMER") %>%
dplyr::filter(!KOMMUNENUMMER %in% c("KOMMUNENUMMER"), 
             KOMMUNENUMMER != "2100") 

opptaksomrader_KLASS_2 %>%
dplyr::group_by(KOMMUNENUMMER) %>%
dplyr::filter(n()>1)

# +
opptaksomrader_KLASS_2 <- opptaksomrader_KLASS_2 %>%
dplyr::rename('ns1:kilde_kode' = ORGNR_HF, 
             'ns1:kilde_tittel' = NAVN_HF, 
             'ns1:mål_kode' = KOMMUNENUMMER, 
             'ns1:mål_tittel' = KOMMUNENAVN)

openxlsx::write.xlsx(opptaksomrader_KLASS_2, file = paste0("/ssb/bruker/rdn/korrespondanse_PHV_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes
