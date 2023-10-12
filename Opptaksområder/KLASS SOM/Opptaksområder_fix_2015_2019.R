# -*- coding: utf-8 -*-
aargang <- 2016

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

# +
# grunnkrets_KLASS <- klassR::GetKlass(1, output_level = 2, date = c(paste0(aargang, "-01-01"))) %>%
# rename(GRUNNKRETSNUMMER = code, 
#       GRUNKKRETSNAVN = name) %>%
# select(GRUNNKRETSNUMMER, GRUNKKRETSNAVN)

# # head(grunnkrets_KLASS)

# grunnkrets_KLASS_t1 <- klassR::GetKlass(1, output_level = 2, date = c(paste0(aargang-1, "-01-01"))) %>%
# rename(GRUNNKRETSNUMMER = code, 
#       GRUNKKRETSNAVN = name) %>%
# select(GRUNNKRETSNUMMER, GRUNKKRETSNAVN)

# head(grunnkrets_KLASS)

# grunnkrets_KLASS_korrespondanse <- klassR::GetKlass(1, date = c(paste0(aargang-1, "-01-01"), paste0(aargang, "-01-01")), correspond = TRUE) %>%
# dplyr::rename(GRUNNKRETSNUMMER_T1 = sourceCode, 
#              GRUNNKRETSNUMMER = targetCode, 
#              targetName = targetName)

# # grunnkrets_KLASS <- grunnkrets_KLASS %>%
# # dplyr::filter(!GRUNNKRETSNUMMER_T1 %in% c("14440104") |  !GRUNNKRETSNUMMER %in% c("14490114")) # Fjerner Kjøs fra Markane
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

if (aargang %in% 2015){
opptaksomrader_KLASS_gulen <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1411")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S19", 
              OPPTAK = "Førde", 
              ORGNR_HF = "983974732", 
             NAVN_HF = "HELSE FØRDE HF", 
             ORGNR_RHF = "983658725", 
             NAVN_RHF = "HELSE VEST RHF")
    }

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

# ## HELSE SØR-ØST RHF

# ### Vestby

# +
if (aargang %in% 2015:2018){
opptaksomrader_KLASS_vestby <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("0211")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S36", 
              OPPTAK = "Akershus", 
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

# ## HELSE MIDT-NORGE RHF

# ### Halsa

# +
if (aargang %in% 2015:2019){
opptaksomrader_KLASS_halsa <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1571")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S26", 
              OPPTAK = "Kristiansund", 
              ORGNR_HF = "997005562", 
             NAVN_HF = "HELSE MØRE OG ROMSDAL HF", 
             ORGNR_RHF = "983658776", 
             NAVN_RHF = "HELSE MIDT-NORGE RHF")
}

opptaksomrader_KLASS_halsa
# -

# ### Roan
#
# Helse Nord-Trøndelag for somatikk og St. Olavs for psykisk helsevern. 

# +
if (aargang %in% 2015:2019){
opptaksomrader_KLASS_roan <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("5019", "1632")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S25", 
              OPPTAK = "Namsos", 
              ORGNR_HF = "983974791", 
             NAVN_HF = "HELSE NORD-TRØNDELAG HF", 
             ORGNR_RHF = "983658776", 
             NAVN_RHF = "HELSE MIDT-NORGE RHF")
}

opptaksomrader_KLASS_roan
# -

# ### Verran 

# +
if (aargang %in% 2015:2019){
opptaksomrader_KLASS_verran <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1724", "5039")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S25", 
              OPPTAK = "Namsos", 
              ORGNR_HF = "983974791", 
             NAVN_HF = "HELSE NORD-TRØNDELAG HF", 
             ORGNR_RHF = "983658776", 
             NAVN_RHF = "HELSE MIDT-NORGE RHF")
}

# opptaksomrader_KLASS_verran
# -

# ### Osen 

# +
opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) == "1633")

opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) == "5020")

# +
# if (aargang %in% 2015:2019){
# opptaksomrader_KLASS_osen <- opptaksomrader_KLASS %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("5020")) %>%
# dplyr::mutate(OPPTAK_NUMMER = "S25", 
#               OPPTAK = "Namsos", 
#               ORGNR_HF = "983974791", 
#              NAVN_HF = "HELSE NORD-TRØNDELAG HF", 
#              ORGNR_RHF = "983658776", 
#              NAVN_RHF = "HELSE MIDT-NORGE RHF")
# }

# opptaksomrader_KLASS_osen
# -

# ### Leksvik 

# +
# leksvik_endring <- grunnkrets_KLASS_korrespondanse %>% # grunnkrets_KLASS_korrespondanse, grunnkrets_KLASS_t1
# filter(substr(GRUNNKRETSNUMMER_T1, 1, 4) %in% c("1718"))

# unique(leksvik_endring$GRUNNKRETSNUMMER)

gamle_leksvik_2018_2019 <- c('50540500','50540501','50540502','50540503','50540504','50540505','50540506','50540507','50540508','50540509','50540510','50540600','50540601','50540602','50540603','50540604')
# -

opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1718")) %>%
head()

# +
if (aargang %in% 2015:2017){
opptaksomrader_KLASS_leksvik <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1718")) %>%
dplyr::mutate(OPPTAK_NUMMER = "S24", 
              OPPTAK = "Levanger", 
              ORGNR_HF = "983974791", 
             NAVN_HF = "HELSE NORD-TRØNDELAG HF", 
             ORGNR_RHF = "983658776", 
             NAVN_RHF = "HELSE MIDT-NORGE RHF")
}

if (aargang %in% 2018:2019){
opptaksomrader_KLASS_leksvik <- opptaksomrader_KLASS %>%
filter(GRUNNKRETSNUMMER %in% gamle_leksvik_2018_2019) %>%
dplyr::mutate(OPPTAK_NUMMER = "S24", 
              OPPTAK = "Levanger", 
              ORGNR_HF = "983974791", 
             NAVN_HF = "HELSE NORD-TRØNDELAG HF", 
             ORGNR_RHF = "983658776", 
             NAVN_RHF = "HELSE MIDT-NORGE RHF")
}

opptaksomrader_KLASS_leksvik
# -

# ## Lager fil med fix

# +
if (aargang %in% 2019){
opptaksomrader_KLASS_uten_fix <- opptaksomrader_KLASS %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1231", # Ullensvang
                                              "1444", # Hornindal
                                              "1571", # Halsa  
                                              "5019", "1632", # Roan
                                              "1724", "5039")) %>% # Verran
    filter(!GRUNNKRETSNUMMER %in% gamle_leksvik_2018_2019)

opptaksomrader_KLASS_med_fix <- rbind(opptaksomrader_KLASS_uten_fix, opptaksomrader_KLASS_ullensvang, opptaksomrader_KLASS_hornindal, opptaksomrader_KLASS_halsa, 
                                      opptaksomrader_KLASS_roan, opptaksomrader_KLASS_verran, opptaksomrader_KLASS_leksvik)
nrow(opptaksomrader_KLASS)-nrow(opptaksomrader_KLASS_med_fix)
    }

if (aargang %in% 2018){
opptaksomrader_KLASS_uten_fix <- opptaksomrader_KLASS %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1231", # Ullensvang
                                              "1444", # Hornindal
                                              "1571", # Halsa  
                                              "0211", # Vestby
                                              "5019", "1632", # Roan
                                              "1724", "5039")) %>% # Verran
    filter(OPPTAK != "Kongsvinger", !GRUNNKRETSNUMMER %in% gamle_leksvik_2018_2019)

opptaksomrader_KLASS_med_fix <- rbind(opptaksomrader_KLASS_uten_fix, opptaksomrader_KLASS_ullensvang, opptaksomrader_KLASS_hornindal, opptaksomrader_KLASS_halsa, 
                                      opptaksomrader_KLASS_roan, opptaksomrader_KLASS_verran, opptaksomrader_KLASS_vestby, opptaksomrader_KLASS_kongsvinger, opptaksomrader_KLASS_leksvik)
nrow(opptaksomrader_KLASS)-nrow(opptaksomrader_KLASS_med_fix)
    }

if (aargang %in% 2016:2017){
opptaksomrader_KLASS_uten_fix <- opptaksomrader_KLASS %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1231", # Ullensvang
                                              "1444", # Hornindal
                                              "1571", # Halsa  
                                              "0211", # Vestby
                                              "1718", # Leksvik
                                              "5019", "1632", # Roan
                                              "1724", "5039")) %>% # Verran
    filter(OPPTAK != "Kongsvinger")

opptaksomrader_KLASS_med_fix <- rbind(opptaksomrader_KLASS_uten_fix, opptaksomrader_KLASS_ullensvang, opptaksomrader_KLASS_hornindal, opptaksomrader_KLASS_halsa, opptaksomrader_KLASS_vestby,
                                      opptaksomrader_KLASS_leksvik, opptaksomrader_KLASS_roan, opptaksomrader_KLASS_verran, opptaksomrader_KLASS_kongsvinger)
nrow(opptaksomrader_KLASS)-nrow(opptaksomrader_KLASS_med_fix)
    }

if (aargang %in% 2015){
opptaksomrader_KLASS_uten_fix <- opptaksomrader_KLASS %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1231", # Ullensvang
                                              "1444", # Hornindal
                                              "1571", # Halsa  
                                              "0211", # Vestby
                                              "1718", # Leksvik
                                              "1411", # Gulen
                                              "5019", "1632", # Roan
                                              "1724", "5039")) %>% # Verran
    filter(OPPTAK != "Kongsvinger")

opptaksomrader_KLASS_med_fix <- rbind(opptaksomrader_KLASS_uten_fix, opptaksomrader_KLASS_ullensvang, opptaksomrader_KLASS_hornindal, opptaksomrader_KLASS_halsa, 
                                      opptaksomrader_KLASS_roan, opptaksomrader_KLASS_verran, opptaksomrader_KLASS_leksvik, opptaksomrader_KLASS_vestby, opptaksomrader_KLASS_gulen,
                                      opptaksomrader_KLASS_kongsvinger)
nrow(opptaksomrader_KLASS)-nrow(opptaksomrader_KLASS_med_fix)
    }
# -

# ## Lager KLASS-fil

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

head(opptaksomrader_KLASS_med_fix_KLASS)

openxlsx::write.xlsx(opptaksomrader_KLASS_med_fix_KLASS, file = paste0("/home/rdn/speshelse/Opptaksområder/KLASS SOM/SOM_opptak_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes

# +
test_2016 <- readxl::read_excel(paste0("/home/rdn/speshelse/Opptaksområder/KLASS SOM/SOM_opptak_", aargang, ".xlsx"))

test_2016 %>%
mutate(test = nchar('ns1:kode')) %>%
filter(test == 7)

# -

openxlsx::write.xlsx(opptaksomrader_KLASS_med_fix_KLASS, file = ,
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes
