# -*- coding: utf-8 -*-
# # Opptaksområder SOM

aargang <- 2016

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
        })

# +
T04317 <- PxWebApiData::ApiData(04317, ContentsCode = "Personer", 
                                Grunnkretser = TRUE, 
                                Tid = as.character(aargang)) [[2]] %>%
  dplyr::filter(!is.na(value)) %>%
  dplyr::rename(GRUNNKRETSNUMMER = Grunnkretser,
                PERSONER = value) %>%
  dplyr::select(GRUNNKRETSNUMMER, PERSONER)

T04317 <- T04317 %>%
dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
dplyr::filter(substr(GRUNNKRETSNUMMER, 5, 8) == "9999") %>%
dplyr::arrange(desc(PERSONER))

# T04317

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
opptaksomrader_KLASS_kommune <- opptaksomrader_KLASS %>%
dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
dplyr::group_by(OPPTAK, OPPTAK_NUMMER, KOMMUNENUMMER) %>%
tally() %>%
arrange(desc(n)) %>%
group_by(KOMMUNENUMMER)

# opptaksomrader_KLASS_kommune %>%
# tally() %>%
# dplyr::arrange(desc(n)) %>%
# dplyr::filter(n > 1) %>%
# dplyr::mutate(GRUNNKRETSNUMMER = paste0(KOMMUNENUMMER, "9999"), 
#              OPPTAK = case_when(
#              GRUNNKRETSNUMMER == "03019999" ~ "Oslo universitetssykehus	", 
#                  TRUE ~ ""
#              ))

# +
# opptaksomrader_KLASS_kommune %>%
# # dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
# dplyr::group_by(KOMMUNENUMMER) %>% 
# dplyr::mutate(n_2 = 1:n())

# +
opptaksomrader_KLASS_kommune_2 <- opptaksomrader_KLASS_kommune %>%
# dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
dplyr::group_by(KOMMUNENUMMER) %>% 
dplyr::mutate(n_2 = 1:n()) %>%
group_by(KOMMUNENUMMER) %>%
dplyr::slice(which.min(n_2)) %>%
dplyr::arrange(desc(n_2)) %>%
dplyr::mutate(GRUNNKRETSNUMMER = paste0(KOMMUNENUMMER, "9999")) %>%
dplyr::filter(GRUNNKRETSNUMMER %in% unique(T04317$GRUNNKRETSNUMMER)) %>%
dplyr::ungroup() %>%
dplyr::select(GRUNNKRETSNUMMER, OPPTAK_NUMMER) %>%
dplyr::mutate(GRUNNKRETSNAVN = "Uoppgitt grunnkrets") %>%
dplyr::rename('ns1:kode' = GRUNNKRETSNUMMER, 
             'ns1:forelder' = OPPTAK_NUMMER, 
             'ns1:navn_bokmål' = GRUNNKRETSNAVN)

openxlsx::write.xlsx(opptaksomrader_KLASS_kommune_2, file = paste0("/ssb/bruker/rdn/SOM_uoppgitte_grunnkretser_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes
# -

opptaksomrader_KLASS_kommune_2


