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
if (aargang >= 2020){
opptaksomrader_KLASS_PHV_t1 <- klassR::GetKlass(630, output_style = "wide", date = c(paste0(aargang+1, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3, 
             # OPPTAK_NUMMER = code3, 
             # OPPTAK = name3, 
             ORGNR_HF_PHV = code2, 
             NAVN_HF_PHV = name2, 
             ORGNR_RHF_PHV = code1, 
             NAVN_RHF_PHV = name1)

nrow(opptaksomrader_KLASS_PHV_t1)
    } else {
    
opptaksomrader_KLASS_PHV_t1 <- readxl::read_excel( paste0("/home/rdn/speshelse/Opptaksområder/KLASS PHV/OPPTAK_PHV_GRUNNKRETS_", aargang+1, ".xlsx")) %>%
dplyr::rename(ORGNR_HF_PHV = ORGNR_HF, 
             NAVN_HF_PHV = NAVN_HF, 
             ORGNR_RHF_PHV = ORGNR_RHF, 
             NAVN_RHF_PHV = NAVN_RHF)
nrow(opptaksomrader_KLASS_PHV_t1)
}

# +
opptaksomrader_KLASS_SOM_t1 <- klassR::GetKlass(629, output_style = "wide", date = c(paste0(aargang+1, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF_SOM = code2, 
             NAVN_HF_SOM = name2, 
             ORGNR_RHF_SOM = code1, 
             NAVN_RHF_SOM = name1) %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) != 2100) %>%
select(-GRUNNKRETS_NAVN, -OPPTAK, -OPPTAK_NUMMER, -GRUNNKRETS_NAVN)

nrow(opptaksomrader_KLASS_SOM_t1)
# -

test <- dplyr::full_join(opptaksomrader_KLASS_PHV_t1, opptaksomrader_KLASS_SOM_t1, by = c("GRUNNKRETSNUMMER"))

test_kommune <- test %>%
filter(NAVN_HF_PHV != NAVN_HF_SOM) %>%
mutate(KOMMUNE = substr(GRUNNKRETSNUMMER, 1, 4))

unique(test_kommune$KOMMUNE)
nrow(test_kommune)

# +
# test_kommune
# -

grunnkrets_KLASS <- klassR::GetKlass(1, date = c(paste0(aargang, "-01-01"), paste0(aargang+1, "-01-01")), correspond = TRUE) %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = sourceCode, 
             GRUNNKRETSNUMMER = targetCode, 
             targetName = targetName)

nrow(test_kommune)

test_kommune %>%
filter(GRUNNKRETSNUMMER %in% unique(grunnkrets_KLASS$GRUNNKRETSNUMMER_T1))
# grunnkrets_KLASS

# ### Lager opptak 2019-2020

# +
opptaksomrader_KLASS_PHV <- test_kommune %>%
dplyr::rename(ORGNR_HF = ORGNR_HF_PHV, 
             NAVN_HF = NAVN_HF_PHV, 
             ORGNR_RHF = ORGNR_RHF_PHV, 
             NAVN_RHF = NAVN_RHF_PHV) %>%
select(-ORGNR_HF_SOM, -NAVN_HF_SOM, -ORGNR_RHF_SOM, -NAVN_RHF_SOM, -KOMMUNE)

head(opptaksomrader_KLASS_PHV)

# +
opptaksomrader_KLASS_SOM <- klassR::GetKlass(629, 
                                             output_style = "wide", 
                                             # output_level = 2,
                                             date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1) %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) != 2100, 
       !GRUNNKRETSNUMMER %in% unique(test_kommune$GRUNNKRETSNUMMER)) %>%
select(-OPPTAK, -OPPTAK_NUMMER)

nrow(opptaksomrader_KLASS_SOM)
# -

head(opptaksomrader_KLASS_SOM)

# +
opptaksomrader_KLASS_PHV_alle <- rbind(opptaksomrader_KLASS_SOM, opptaksomrader_KLASS_PHV)

if (aargang %in% 2015:2017){
  opptaksomrader_KLASS_PHV_alle <- opptaksomrader_KLASS_PHV_alle %>%
    dplyr::mutate(ORGNR_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "1632" ~ "883974832", TRUE ~ ORGNR_HF), 
                NAVN_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "1632" ~ "ST OLAVS HOSPITAL HF", TRUE ~ NAVN_HF))
}

if (aargang %in% 2018:2019){
  opptaksomrader_KLASS_PHV_alle <- opptaksomrader_KLASS_PHV_alle %>%
    dplyr::mutate(ORGNR_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "5019" ~ "883974832", TRUE ~ ORGNR_HF), 
                NAVN_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "5019" ~ "ST OLAVS HOSPITAL HF", TRUE ~ NAVN_HF))
}

if (aargang %in% 2015:2018){
  opptaksomrader_KLASS_PHV_alle <- opptaksomrader_KLASS_PHV_alle %>%
    dplyr::mutate(ORGNR_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "0236" ~ "983971636", TRUE ~ ORGNR_HF), 
                NAVN_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "0236" ~ "AKERSHUS UNIVERSITETSSYKEHUS HF", TRUE ~ NAVN_HF))
}

if (aargang %in% 2015:2019){
    # Rindal
    opptaksomrader_KLASS_PHV_alle <- opptaksomrader_KLASS_PHV_alle %>%
    dplyr::mutate(ORGNR_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "5061" ~ "997005562", TRUE ~ ORGNR_HF), 
                NAVN_HF = case_when(substr(GRUNNKRETSNUMMER, 1, 4) == "5061" ~ "HELSE MØRE OG ROMSDAL HF", TRUE ~ NAVN_HF))
    }

openxlsx::write.xlsx(opptaksomrader_KLASS_PHV_alle, file = paste0("/home/rdn/speshelse/Opptaksområder/KLASS PHV/OPPTAK_PHV_GRUNNKRETS_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes

# +
# opptaksomrader_KLASS_PHV_alle %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) == "1633")

# opptaksomrader_KLASS_PHV_alle %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) == "5020")
# -

opptaksomrader_KLASS_PHV_alle_kommune <- opptaksomrader_KLASS_PHV_alle %>%
dplyr::mutate(KOMMUNE = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
distinct(KOMMUNE, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF)

openxlsx::write.xlsx(opptaksomrader_KLASS_PHV_alle_kommune, file = paste0("/home/rdn/speshelse/Opptaksområder/KLASS PHV/OPPTAK_PHV_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes

opptaksomrader_KLASS_PHV_alle_kommune %>%
filter(KOMMUNE == "0236")
# filter(NAVN_HF == "AKERSHUS UNIVERSITETSSYKEHUS HF")

# ## Lager KLASS-fil

# +
test <- opptaksomrader_KLASS_PHV_alle %>%
  dplyr::mutate(TOM_FORELDER = "") %>%
  # dplyr::filter(opptak %in% c("Stavanger", "Ålesund")) %>%
  dplyr::select(GRUNNKRETSNUMMER, GRUNNKRETS_NAVN, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF, TOM_FORELDER)

level_1 <- test %>%
  dplyr::select(ORGNR_RHF, TOM_FORELDER, NAVN_RHF) %>%
  dplyr::distinct() %>%
  dplyr::rename('ns1:kode' = ORGNR_RHF, 
                'ns1:forelder' = TOM_FORELDER, 
                'ns1:navn_bokmål' = NAVN_RHF)

level_2 <- test %>%
  dplyr::select(ORGNR_HF, ORGNR_RHF, NAVN_HF) %>%
  dplyr::rename('ns1:kode' = ORGNR_HF, 
                'ns1:forelder' = ORGNR_RHF, 
                'ns1:navn_bokmål' = NAVN_HF) %>%
  dplyr::distinct()

level_3 <- test %>%
  dplyr::select(GRUNNKRETSNUMMER, ORGNR_HF, GRUNNKRETS_NAVN) %>%
  dplyr::rename('ns1:kode' = GRUNNKRETSNUMMER, 
                'ns1:forelder' = ORGNR_HF, 
                'ns1:navn_bokmål' = GRUNNKRETS_NAVN) %>%
  dplyr::distinct()

opptaksomrader_KLASS_med_fix_KLASS <- rbind(level_1, level_2, level_3)

# opptaksomrader_KLASS_med_fix_KLASS

openxlsx::write.xlsx(opptaksomrader_KLASS_med_fix_KLASS, file = paste0("/home/rdn/speshelse/Opptaksområder/KLASS PHV/KLASS_OPPTAK_PHV_GRUNNKRETS_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes
# -

opptaksomrader_KLASS_PHV_alle %>%
filter(GRUNNKRETSNUMMER == "03010101")
# group_by(GRUNNKRETSNUMMER) %>%
# tally() %>%
# arrange(desc(n))

paste0("/home/rdn/speshelse/Opptaksområder/KLASS PHV/KLASS_OPPTAK_PHV_GRUNNKRETS_", aargang, ".xlsx")
