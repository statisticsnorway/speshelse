# # Opptaksområder 2019

aargang <- 2020

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

opptaksomrader_KLASS %>%
dplyr::filter(GRUNNKRETSNUMMER == "30039999")

# ### Korrespondanse mellom t og t-1 fra KLASS

# +
grunnkrets_KLASS <- klassR::GetKlass(1, date = c(paste0(aargang-1, "-01-01"), paste0(aargang, "-01-01")), correspond = TRUE) %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = sourceCode, 
             GRUNNKRETSNUMMER = targetCode, 
             targetName = targetName)

nrow(grunnkrets_KLASS)

grunnkrets_KLASS <- grunnkrets_KLASS %>%
dplyr::filter(!GRUNNKRETSNUMMER_T1 %in% c("18500111") |  !GRUNNKRETSNUMMER %in% c("18061610"), # Fjerner Kjerrvika fra Storå
             !GRUNNKRETSNUMMER_T1 %in% c("18500109") |  !GRUNNKRETSNUMMER %in% c("18750211"),  # Fjerner Indre Tysfjord fra Kjerrvika
             !GRUNNKRETSNUMMER_T1 %in% c("18500119") |  !GRUNNKRETSNUMMER %in% c("18750211")) # Fjerner Bjørntoppen fra Kjerrvika

nrow(grunnkrets_KLASS)
# -

# ### Merger opptaksområder med KLASS (t-1)

# +
opptaksomrader_KLASS_2 <- opptaksomrader_KLASS %>%
dplyr::left_join(grunnkrets_KLASS, by = "GRUNNKRETSNUMMER") %>%
dplyr::mutate(GRUNNKRETSNUMMER_T1 = case_when(
is.na(GRUNNKRETSNUMMER_T1) ~ GRUNNKRETSNUMMER, 
    TRUE ~ GRUNNKRETSNUMMER_T1
)) %>%
dplyr::select(GRUNNKRETSNUMMER, GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
dplyr::distinct(GRUNNKRETSNUMMER_T1, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF)

nrow(opptaksomrader_KLASS_2)

# +
# opptaksomrader_KLASS_2 %>%
# dplyr::filter(GRUNNKRETSNUMMER_T1 == "18500109")
# -

# ### Laster inn KLASS (t-1)

# +
grunnkrets_KLASS_T1 <- klassR::GetKlass(1, output_style = "wide", date = c(paste0(aargang-1, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = code2, 
             GRUNNKRETS_NAVN_T1 = name2) %>%
dplyr::select(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1) 

nrow(grunnkrets_KLASS_T1)
nrow(opptaksomrader_KLASS_2)

# +
# opptaksomrader_KLASS_2 %>%
# group_by(GRUNNKRETSNUMMER_T1) %>% # grunnkrets_KLASS_T1_2
# filter(n()>1)

# +
grunnkrets_KLASS_T1_2 <- grunnkrets_KLASS_T1 %>%
dplyr::left_join(opptaksomrader_KLASS_2, by = "GRUNNKRETSNUMMER_T1") %>%
dplyr::filter(!is.na(OPPTAK))

nrow(grunnkrets_KLASS_T1_2)

head(grunnkrets_KLASS_T1_2)

# grunnkrets_KLASS_T1_2 <- grunnkrets_KLASS_T1 %>%
# dplyr::distinct(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1, OPPTAK)
# -

# ### Lagrer filen 

# +
test <- grunnkrets_KLASS_T1_2 %>%
  dplyr::mutate(TOM_FORELDER = "") %>%
  # dplyr::filter(opptak %in% c("Stavanger", "Ålesund")) %>%
  dplyr::select(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1, OPPTAK_NUMMER, ORGNR_HF, NAVN_HF, OPPTAK, ORGNR_RHF, NAVN_RHF, TOM_FORELDER)

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
  dplyr::select(GRUNNKRETSNUMMER_T1, OPPTAK_NUMMER, GRUNNKRETS_NAVN_T1) %>%
  dplyr::rename('ns1:kode' = GRUNNKRETSNUMMER_T1, 
                'ns1:forelder' = OPPTAK_NUMMER, 
                'ns1:navn_bokmål' = GRUNNKRETS_NAVN_T1) %>%
  dplyr::distinct()

KLASS <- rbind(level_1, level_2, level_3, level_4)
# -

openxlsx::write.xlsx(KLASS, file = "/ssb/bruker/rdn/opptak_2019.xlsx",
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes











# +
uoppgitt_grunnkrets <- grunnkrets_KLASS_T1 %>%
dplyr::left_join(opptaksomrader_KLASS_2, by = "GRUNNKRETSNUMMER_T1") %>%
dplyr::filter(is.na(OPPTAK)) %>%
dplyr::select(GRUNNKRETSNUMMER_T1)

nrow(uoppgitt_grunnkrets)

head(uoppgitt_grunnkrets)

kommune_test <- grunnkrets_KLASS_T1_2 %>%
# dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER_T1, 1, 4)) %>%
# dplyr::group_by(KOMMUNENUMMER, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>% # OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF
# dplyr::tally() %>%
# arrange(KOMMUNENUMMER) %>%
# dplyr::group_by(KOMMUNENUMMER, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>% # 
# slice(which.max(n)) %>%
# dplyr::mutate(GRUNNKRETSNUMMER_T1 = paste0(KOMMUNENUMMER, "9999")) %>%
# ungroup() %>%
# dplyr::select(-KOMMUNENUMMER) %>%
dplyr::filter(GRUNNKRETSNUMMER_T1 %in% unique(uoppgitt_grunnkrets$GRUNNKRETSNUMMER_T1))

nrow(kommune_test)

# head(kommune_test)

# +
# kommune_test <- grunnkrets_KLASS_T1 %>%
# dplyr::filter(!is.na(OPPTAK)) %>%
# dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER_T1, 1, 4)) %>%
# dplyr::group_by(KOMMUNENUMMER, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
# dplyr::tally() %>%
# arrange(KOMMUNENUMMER) %>%
# slice(which.max(n)) %>%
# dplyr::mutate(GRUNNKRETSNUMMER_T1 = paste0(KOMMUNENUMMER, "9999")) %>%
# ungroup() %>%
# dplyr::select(-KOMMUNENUMMER, -n)

# nrow(kommune_test)

# uoppgitt_grunnkrets <- grunnkrets_KLASS_T1 %>%
# dplyr::filter(is.na(OPPTAK)) %>%
# dplyr::select(GRUNNKRETSNUMMER_T1)

# nrow(uoppgitt_grunnkrets)

# uoppgitt_grunnkrets %>%
# dplyr::filter(GRUNNKRETSNUMMER_T1 == "02269999")

# uoppgitt_grunnkrets <- uoppgitt_grunnkrets %>%
# dplyr::left_join(kommune_test, by = "GRUNNKRETSNUMMER_T1")

# uoppgitt_grunnkrets %>%
# dplyr::filter(GRUNNKRETSNUMMER_T1 == "02269999")

# nrow(uoppgitt_grunnkrets)

# +
# uoppgitt_grunnkrets %>%
# group_by(GRUNNKRETSNUMMER_T1) %>% # grunnkrets_KLASS_T1_2
# filter(n()>1)

# +
# kommune_test %>%
# group_by(KOMMUNENUMMER) %>% # grunnkrets_KLASS_T1_2
# filter(n()>1)

# +
# grunnkrets_KLASS_T1_2 %>%
# # dplyr::distinct(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1, OPPTAK) %>%
# group_by(GRUNNKRETSNUMMER_T1) %>% # grunnkrets_KLASS_T1_2
# filter(n()>1)

# +
# grunnkrets_KLASS_T1_2 <- grunnkrets_KLASS_T1 %>%
# dplyr::mutate(OPPTAK = case_when(
# GRUNNKRETS_NAVN_T1 == "Indre Tysfjord" ~ "Narvik",
# GRUNNKRETS_NAVN_T1 == "Kjerrvika" ~ "Narvik",
# GRUNNKRETS_NAVN_T1 == "Bjørntoppen" ~ "Narvik",
# TRUE ~ OPPTAK
# )) 

grunnkrets_KLASS_T1_2 %>%
dplyr::distinct(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1, OPPTAK) %>%
group_by(GRUNNKRETSNUMMER_T1) %>% # grunnkrets_KLASS_T1_2
filter(n()>1)

nrow(grunnkrets_KLASS_T1_2)

# +
# grunnkrets_KLASS_T1_2 %>%
# dplyr::filter(substr(GRUNNKRETSNUMMER_T1, 1, 4) == "0226") # Akershus

# +
# # grunnkrets_KLASS_T1_2 %>%
# # # dplyr::filter(GRUNNKRETSNUMMER_T1 == "01059999")
# # dplyr::filter(GRUNNKRETSNUMMER == "30039999")

# grunnkrets_KLASS_T1_2 %>%
# dplyr::filter(!is.na(OPPTAK)) %>%
# dplyr::mutate(KOMMUNENUMMER = substr(GRUNNKRETSNUMMER_T1, 1, 4)) %>%
# dplyr::select(OPPTAK, KOMMUNENUMMER) %>%
# dplyr::distinct() %>%
# group_by(KOMMUNENUMMER) %>% # grunnkrets_KLASS_T1_2
# filter(n()>1)
# -

# OBS: Uoppgitt grunnkrets per kommune?

grunnkrets_KLASS_T1_2 %>%
dplyr::filter(is.na(GRUNNKRETSNUMMER)) %>%
dplyr::filter(substr(GRUNNKRETSNUMMER_T1, 5, 8) != "9999")

# +
# nrow(opptaksomrader_KLASS_2)

nrow(opptaksomrader_KLASS_2)

opptaksomrader_KLASS_2 %>%
dplyr::filter(GRUNNKRETSNUMMER_T1 == "18500109")

# opptaksomrader_KLASS_2 %>%
# dplyr::filter(is.na(OPPTAK_NUMMER))

# opptaksomrader_KLASS_2 %>%
# dplyr::filter(is.na(GRUNNKRETSNUMMER_T1))

# +
test <- opptaksomrader_KLASS_2 %>%
group_by(GRUNNKRETSNUMMER_T1) %>% 
filter(n()>1)

nrow(test)
# -

test


