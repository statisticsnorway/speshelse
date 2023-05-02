# -*- coding: utf-8 -*-
aargang <- 2019

# +
options(repr.matrix.max.rows=600, repr.matrix.max.cols=2000)

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
        })

source("/home/rdn/fellesr/R/DAPLA_funcs.R")

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
# -

# # Fix 2015-2019

# ## HELSE VEST RHF

# ### Ullensvang

if (aargang %in% 2015:2019){
opptaksomrader_KLASS_ullensvang <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1231")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S14", 
              OPPTAK = "Odda", 
              ORGNR_HF = "983974694", 
             NAVN_HF = "HELSE FONNA HF", 
             ORGNR_RHF = "983658725", 
             NAVN_RHF = "HELSE VEST RHF")
    }

# ### Gulen
#
# Gulen har blitt kategorisert under Haraldsplass sitt opptaksområde. Skal dette endres til Helse Førde? Hvilket lokalsykehus i så fall?

# +
# if (aargang %in% 2015:2019){
# opptaksomrader_KLASS_gulen <- opptaksomrader_KLASS %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1411")) %>%
# dplyr::mutate(OPPTAK_NUMMER = "S19", 
#               OPPTAK = "Førde", 
#               ORGNR_HF = "983974732", 
#              NAVN_HF = "HELSE FØRDE HF", 
#              ORGNR_RHF = "983658725", 
#              NAVN_RHF = "HELSE VEST RHF")
#     }
# -

# ### Eidfjord

# +
# if (aargang %in% 2015:2019){
# opptaksomrader_KLASS_eidfjord <- opptaksomrader_KLASS %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("4619", "1232")) # %>%
# # dplyr::mutate(OPPTAK_NUMMER = "", 
# #               OPPTAK = "", 
# #               ORGNR_HF = "", 
# #              NAVN_HF = "", 
# #              ORGNR_RHF = "", 
# #              NAVN_RHF = "")
#     }

# opptaksomrader_KLASS_eidfjord
# -

# ### Hornindal

if (aargang %in% 2015:2019){
opptaksomrader_KLASS_hornindal <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1444")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S21", 
              OPPTAK = "Nordfjord", 
              ORGNR_HF = "983974732", 
             NAVN_HF = "HELSE FØRDE HF", 
             ORGNR_RHF = "983658725", 
             NAVN_RHF = "HELSE VEST RHF")
}

# ## HELSE SØR_ØST RHF

# ### Vestby

# +
if (aargang %in% 2015:2018){
opptaksomrader_KLASS_vestby <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("0211")) %>%
dplyr::mutate(OPPTAK_NUMMER = "",  # ???
              OPPTAK = "",  # ??? AHUS?
              ORGNR_HF = "983971636", 
             NAVN_HF = "AKERSHUS UNIVERSITETSSYKEHUS HF", 
             ORGNR_RHF = "991324968", 
             NAVN_RHF = "HELSE SØR-ØST RHF")

# opptaksomrader_KLASS_vestby
    }
# -

# ### Kongsvinger opptaksområde

if (aargang %in% 2015:2018){
opptaksomrader_KLASS_kongsvinger <- opptaksomrader_KLASS %>%
filter(OPPTAK == "Kongsvinger") %>%
dplyr::mutate(ORGNR_HF = "983971709", 
             NAVN_HF = "SYKEHUSET INNLANDET HF", 
             ORGNR_RHF = "991324968", 
             NAVN_RHF = "HELSE SØR-ØST RHF")
    
    head(opptaksomrader_KLASS_kongsvinger)
    }

# ### Nes

# +
# opptaksomrader_KLASS %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0236") %>%
# head()

# +
# if (aargang %in% 2015:2018){
# opptaksomrader_KLASS_nes <- opptaksomrader_KLASS %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("0236")) %>%
# dplyr::mutate(OPPTAK_NUMMER = "", 
#               OPPTAK = "", 
#               ORGNR_HF = "983971709", 
#              NAVN_HF = "SYKEHUSET INNLANDET HF", 
#              ORGNR_RHF = "991324968", 
#              NAVN_RHF = "HELSE SØR-ØST RHF")

# opptaksomrader_KLASS_nes
#     }
# -

# ## Lager fil med fix

# +
if (aargang %in% 2019){
opptaksomrader_KLASS_uten_fix <- opptaksomrader_KLASS %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1231", "1444", "4619", "1232"))

opptaksomrader_KLASS_med_fix <- rbind(opptaksomrader_KLASS_uten_fix, opptaksomrader_KLASS_ullensvang, opptaksomrader_KLASS_hornindal)
nrow(opptaksomrader_KLASS)-nrow(opptaksomrader_KLASS_med_fix)
    }

if (aargang %in% 2015:2018){
opptaksomrader_KLASS_uten_fix <- opptaksomrader_KLASS %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1231", "1444", "0211", "4619", "1232"), # Legg til flere?
      OPPTAK != "Kongsvinger") 

opptaksomrader_KLASS_med_fix <- rbind(opptaksomrader_KLASS_uten_fix, opptaksomrader_KLASS_ullensvang, opptaksomrader_KLASS_hornindal, 
                                     opptaksomrader_KLASS_kongsvinger, opptaksomrader_KLASS_vestby) # Legg til flere
nrow(opptaksomrader_KLASS)-nrow(opptaksomrader_KLASS_med_fix)
    }

# +
test <- opptaksomrader_KLASS_med_fix %>%
  dplyr::mutate(TOM_FORELDER = "") %>%
  # dplyr::filter(opptak %in% c("Stavanger", "Ålesund")) %>%
  dplyr::select(GRUNNKRETSNUMMER, GRUNNKRETS_NAVN, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF, TOM_FORELDER)

level_1 <- test %>%
  dplyr::select(ORGNR_RHF, TOM_FORELDER, NAVN_RHF) %>%
  dplyr::rename('ns1:kode' = ORGNR_RHF, 
                'ns1:forelder' = TOM_FORELDER, 
                'ns1:navn_bokmål' = NAVN_RHF) %>%
  dplyr::distinct()

level_2 <- test %>%
  dplyr::select(ORGNR_HF, ORGNR_RHF, NAVN_HF) %>%
  dplyr::rename('ns1:kode' = ORGNR_HF, 
                'ns1:forelder' = ORGNR_RHF, 
                'ns1:navn_bokmål' = NAVN_HF) %>%
  dplyr::distinct()

level_3 <- test %>%
  dplyr::select(OPPTAK_NUMMER, ORGNR_HF, OPPTAK) %>%
  dplyr::rename('ns1:kode' = OPPTAK_NUMMER, 
                'ns1:forelder' = ORGNR_HF, 
                'ns1:navn_bokmål' = OPPTAK) %>%
  dplyr::distinct()

level_4 <- test %>%
  dplyr::select(GRUNNKRETSNUMMER, OPPTAK_NUMMER, GRUNNKRETS_NAVN) %>%
  dplyr::rename('ns1:kode' = GRUNNKRETSNUMMER, 
                'ns1:forelder' = OPPTAK_NUMMER, 
                'ns1:navn_bokmål' = GRUNNKRETS_NAVN) %>%
  dplyr::distinct()

opptaksomrader_KLASS_med_fix_KLASS <- rbind(level_1, level_2, level_3, level_4)
# -

opptaksomrader_KLASS_med_fix_KLASS


