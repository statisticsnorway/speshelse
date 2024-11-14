# -*- coding: utf-8 -*-
# # Korrespondansetabell opptaksområde-kommune

aargang <- 2024

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

# ### Filstier

# +
# arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"
# arbeidsmappe_kart <- paste0(arbeidsmappe, "kart/", aargang, "/")

# arbeidsmappe_opptak <- paste0(arbeidsmappe, "opptaksomrader/", aargang, "/")

# if (file.exists(arbeidsmappe_opptak)==FALSE) {
#   dir.create(arbeidsmappe_opptak)
# }
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
             NAVN_RHF = name1) %>%
dplyr::mutate(GRUNNKRETSNUMMER = str_pad(GRUNNKRETSNUMMER, width = 8, "left", pad = "0"))

nrow(opptaksomrader_KLASS)
# -

opptaksomrader_KLASS_grunnkrets <- opptaksomrader_KLASS %>%
dplyr::filter(!grepl("postnummer", GRUNNKRETS_NAVN))

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

# +
opptaksomrader_KLASS_2 <- opptaksomrader_KLASS_grunnkrets %>%
dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
dplyr::select(OPPTAK_NUMMER, OPPTAK, KOMMUNENUMMER) %>%
dplyr::distinct() %>%
dplyr::left_join(kommune_KLASS, by = "KOMMUNENUMMER") %>%
dplyr::filter(!KOMMUNENUMMER %in% c("KOMMUNENUMMER"), 
             KOMMUNENUMMER != "2100") 

# Kristiansand og Trondheim fix #

kristiansand <- data.frame(OPPTAK_NUMMER = c("D62", "D63"), 
                          OPPTAK = c("Solvang", "Strømme"), 
                          KOMMUNENUMMER = c("4204", "4204"), 
                          KOMMUNENAVN = c("Kristiansand", "Kristiansand"))

trondheim <- data.frame(OPPTAK_NUMMER = c("D33", "D34"), 
                          OPPTAK = c("Nidaros", "Nidelv"), 
                          KOMMUNENUMMER = c("5001", "5001"), 
                          KOMMUNENAVN = c("Trondheim", "Trondheim")) # OBS: Trondheim - Tråanten i 2023?

opptaksomrader_KLASS_3 <- rbind(opptaksomrader_KLASS_2, kristiansand, trondheim)
# -

length(unique(opptaksomrader_KLASS_3$KOMMUNENUMMER)) # skal være lik antall kommuner

nrow(opptaksomrader_KLASS_3) # skal være høyere enn antall kommuner

# OBS: endre filsti

# +
opptaksomrader_KLASS_3 <- opptaksomrader_KLASS_3 %>%
dplyr::rename('ns1:kilde_kode' = OPPTAK_NUMMER, 
             'ns1:kilde_tittel' = OPPTAK, 
             'ns1:mål_kode' = KOMMUNENUMMER, 
             'ns1:mål_tittel' = KOMMUNENAVN)

openxlsx::write.xlsx(opptaksomrader_KLASS_3, file = paste0("/ssb/bruker/rdn/korrespondanse_DPS_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes
