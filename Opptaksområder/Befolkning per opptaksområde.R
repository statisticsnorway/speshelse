# -*- coding: utf-8 -*-
# # Befolkning per opptaksområde

aargang <- 2020

# +
# renv::restore("/ssb/bruker/rdn/speshelse")

options(scipen=999)

suppressPackageStartupMessages({
  library(tidyverse)
  library(PxWebApiData)
  library(lubridate)
  # library(fellesr)
})
# -

arbeidsmappe <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/befolkning_per_opptaksomrade/")
befolkning_per_grunnkrets_filsti <- paste0(arbeidsmappe, "inndata/befolkning_per_grunnkrets_", aargang, ".parquet")
befolkning_per_opptaksomrade_masterfil_filsti <- paste0(arbeidsmappe, "masterfil/befolkning_per_opptaksomrade_masterfil_", aargang, ".parquet")

befolkning_per_postkrets_filsti <- paste0(arbeidsmappe, "/inndata/befolkning_per_postkrets_", aargang, ".parquet")

# +
befolkning_per_grunnkrets <- arrow::read_parquet(befolkning_per_grunnkrets_filsti)

sum(befolkning_per_grunnkrets$PERSONER)
# -

head(befolkning_per_grunnkrets)

befolkning_per_postkrets <- arrow::read_parquet(befolkning_per_postkrets_filsti) %>%
# ungroup() %>%
# dplyr::select(-KOMMUNENR) %>%
dplyr::rename(GRUNNKRETSNUMMER = POSTNR)
sum(befolkning_per_postkrets$PERSONER)

nchar(aargang)

# +
T07459 <- PxWebApiData::ApiData(07459, ContentsCode = "Personer",
                                Kjonn = T,
                                Alder = T,
                                Region = T,
                               Tid = as.character(aargang)) [[2]] %>%
dplyr::mutate(Alder = as.numeric(Alder), 
             Alder = replace_na(Alder, 105)) %>%
dplyr::filter(nchar(Region) == 4)

T07459_diff <- T07459 %>%
  dplyr::filter(Region %in% c("5001", "4204")) %>%
  dplyr::group_by(Region, Alder, Kjonn) %>%
  dplyr::summarise(PERSONER = sum(value)) %>%
filter(PERSONER != 0)
# -

sum(T07459_diff$PERSONER)-sum(befolkning_per_postkrets$PERSONER)

# +
befolkning_per_postkrets_diff <- befolkning_per_postkrets %>%
  dplyr::mutate(ALDER = case_when(ALDER >= 105 ~ 105, TRUE ~ ALDER)) %>%
  dplyr::group_by(KOMMUNENR, ALDER, KJOENN) %>%
  dplyr::summarise(PERSONER_EGEN = sum(PERSONER))

sum(T07459_diff$PERSONER)-sum(befolkning_per_postkrets_diff$PERSONER_EGEN)
# -

nrow(T07459_diff)
nrow(befolkning_per_postkrets_diff)

# +
# befolkning_per_postkrets_diff <- befolkning_per_postkrets_diff # %>%
# # distinct(KOMMUNENR, ALDER, KJOENN) %>%
# # filter(!is.na(ALDER))
# -

nrow(T07459_diff)
nrow(befolkning_per_postkrets_diff)

# +
diff <- dplyr::full_join(T07459_diff, befolkning_per_postkrets_diff, by = c("Region" = "KOMMUNENR", 
                                                                           "Alder" = "ALDER", 
                                                                           "Kjonn" = "KJOENN")) %>%
  dplyr::mutate(PERSONER = replace_na(PERSONER, 0), 
                diff = PERSONER-PERSONER_EGEN,
                PERSONER_EGEN_2 = PERSONER_EGEN+diff,
               OPPTAK_NUMMER = case_when(Region == "4204" ~ "D62",
                                        Region == "5001" ~ "D33"), 
               OPPTAK = case_when(Region == "4204" ~ "Solvang",
                                        Region == "5001" ~ "Nidaros"))

# sum(diff$diff)
# sum(diff$PERSONER)
# sum(diff$PERSONER_EGEN)

# diff
# -

# ## SOM

# ### Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk)

# +
opptaksomrader_SOM_KLASS <- klassR::GetKlass(629, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

nrow(opptaksomrader_SOM_KLASS)

opptaksomrader_SOM_KLASS <- dplyr::left_join(befolkning_per_grunnkrets, opptaksomrader_SOM_KLASS, by = "GRUNNKRETSNUMMER")
sum(befolkning_per_grunnkrets$PERSONER)-sum(opptaksomrader_SOM_KLASS$PERSONER)
# -

opptaksomrader_SOM_KLASS %>%
group_by(OPPTAK_NUMMER, OPPTAK) %>%
tally() %>%
ungroup() %>%
arrange(OPPTAK_NUMMER)

# ### Lager befolkningstall per lokasjonsområde

# +
befolkning_per_opptaksomrade_SOM_TOT <- opptaksomrader_SOM_KLASS %>%
  dplyr::group_by(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999", 
                KJOENN = "0") %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_SOM_kjonn <- opptaksomrader_SOM_KLASS %>%
  dplyr::group_by(KJOENN, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999",) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_SOM_alder <- opptaksomrader_SOM_KLASS %>%
  dplyr::group_by(ALDER, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(KJOENN = "0",
                ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
              ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_SOM_kjonn_alder <- opptaksomrader_SOM_KLASS %>%
  dplyr::group_by(KJOENN, ALDER, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
              ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_SOM_lokasjon <- rbind(befolkning_per_opptaksomrade_SOM_TOT, befolkning_per_opptaksomrade_SOM_kjonn, befolkning_per_opptaksomrade_SOM_alder, befolkning_per_opptaksomrade_SOM_kjonn_alder) %>%
dplyr::select(-ALDER) %>%
dplyr::mutate(TJENESTE = "SOM", 
             LEVEL = "Lokasjon")
# -

# ### Aggregerer til HF- og RHF-nivå

# +
befolkning_per_opptaksomrade_SOM_HF <- befolkning_per_opptaksomrade_SOM_lokasjon %>%
  dplyr::group_by(TJENESTE, KJOENN, ALDER_KODE, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(OPPTAK_NUMMER = "", 
                OPPTAK = "",
                LEVEL = "HF")

befolkning_per_opptaksomrade_SOM_RHF <- befolkning_per_opptaksomrade_SOM_lokasjon %>%
  dplyr::group_by(TJENESTE, KJOENN, ALDER_KODE, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ORGNR_HF = "",
                NAVN_HF = "",
                OPPTAK_NUMMER = "", 
                OPPTAK = "",
                LEVEL = "RHF")

# Legger sammen alle SOM
befolkning_per_opptaksomrade_SOM <- rbind(befolkning_per_opptaksomrade_SOM_lokasjon, befolkning_per_opptaksomrade_SOM_HF, befolkning_per_opptaksomrade_SOM_RHF)

# +
# befolkning_per_opptaksomrade_SOM %>%
# filter(LEVEL == "Lokasjon", 
#       ALDER_KODE == "999", 
#       KJOENN == "0") %>%
# summarise(PERSONER = sum(PERSONER))
# -

# ## VOP
#
# ### Kodeliste for opptaksområder i spesialisthelsetjenesten (PHV)

# +
opptaksomrader_PHV_KLASS <- klassR::GetKlass(630, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
  dplyr::rename(GRUNNKRETSNUMMER = code3, 
                GRUNNKRETS_NAVN = name3, 
                ORGNR_HF = code2, 
                NAVN_HF = name2, 
                ORGNR_RHF = code1, 
                NAVN_RHF = name1)

nrow(opptaksomrader_PHV_KLASS)

opptaksomrader_PHV_KLASS <- dplyr::left_join(befolkning_per_grunnkrets, opptaksomrader_PHV_KLASS, by = "GRUNNKRETSNUMMER")
sum(befolkning_per_grunnkrets$PERSONER)-sum(opptaksomrader_PHV_KLASS$PERSONER)
# -

# ### Lager befolkningstall per HF

# +
befolkning_per_opptaksomrade_PHV_TOT <- opptaksomrader_PHV_KLASS %>%
  dplyr::group_by(ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999", 
                KJOENN = "0") %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_PHV_kjonn <- opptaksomrader_PHV_KLASS %>%
  dplyr::group_by(KJOENN, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999",) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_PHV_alder <- opptaksomrader_PHV_KLASS %>%
  dplyr::group_by(ALDER, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(KJOENN = "0",
                ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
                ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_PHV_kjonn_alder <- opptaksomrader_PHV_KLASS %>%
  dplyr::group_by(KJOENN, ALDER, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
                ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_PHV_HF <- rbind(befolkning_per_opptaksomrade_PHV_TOT, befolkning_per_opptaksomrade_PHV_kjonn, befolkning_per_opptaksomrade_PHV_alder, befolkning_per_opptaksomrade_PHV_kjonn_alder) %>%
  dplyr::mutate(TJENESTE = dplyr::case_when(
    ALDER <= 17 ~ "BUP",
    ALDER >= 18 ~ "VOP"), 
    LEVEL = "HF", 
    OPPTAK_NUMMER = "", 
    OPPTAK = "") %>%
  dplyr::select(-ALDER)
# -

# ### Aggregerer til RHF-nivå

# +
befolkning_per_opptaksomrade_PHV_RHF <- befolkning_per_opptaksomrade_PHV_HF %>%
  dplyr::group_by(TJENESTE, KJOENN, ALDER_KODE, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ORGNR_HF = "",
                NAVN_HF = "",
                OPPTAK_NUMMER = "", 
                OPPTAK = "",
                LEVEL = "RHF")

befolkning_per_opptaksomrade_PHV <- rbind(befolkning_per_opptaksomrade_PHV_HF, befolkning_per_opptaksomrade_PHV_RHF)
# -

# ## TSB
#
# ### Kodeliste for opptaksområder i spesialisthelsetjenesten (PHV)

# +
opptaksomrader_TSB_KLASS <- klassR::GetKlass(631, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

nrow(opptaksomrader_TSB_KLASS)

opptaksomrader_TSB_KLASS <- dplyr::left_join(befolkning_per_grunnkrets, opptaksomrader_TSB_KLASS, by = "GRUNNKRETSNUMMER")
sum(befolkning_per_grunnkrets$PERSONER)-sum(opptaksomrader_TSB_KLASS$PERSONER)
# -

# ### Lager befolkningstall per HF

# +
befolkning_per_opptaksomrade_TSB_TOT <- opptaksomrader_TSB_KLASS %>%
  dplyr::group_by(ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999", 
                KJOENN = "0") %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_TSB_kjonn <- opptaksomrader_TSB_KLASS %>%
  dplyr::group_by(KJOENN, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999",) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_TSB_alder <- opptaksomrader_TSB_KLASS %>%
  dplyr::group_by(ALDER, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(KJOENN = "0",
                ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
                ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_TSB_kjonn_alder <- opptaksomrader_TSB_KLASS %>%
  dplyr::group_by(KJOENN, ALDER, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
                ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_TSB_HF <- rbind(befolkning_per_opptaksomrade_TSB_TOT, befolkning_per_opptaksomrade_TSB_kjonn, befolkning_per_opptaksomrade_TSB_alder, befolkning_per_opptaksomrade_TSB_kjonn_alder) %>%
  dplyr::mutate(TJENESTE = "TSB", 
               LEVEL = "HF", 
             OPPTAK_NUMMER = "", 
                OPPTAK = "") %>%
dplyr::select(-ALDER)
# -

# ### Aggregerer til RHF-nivå

# +
befolkning_per_opptaksomrade_TSB_RHF <- befolkning_per_opptaksomrade_TSB_HF %>%
  dplyr::group_by(TJENESTE, KJOENN, ALDER_KODE, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ORGNR_HF = "",
                NAVN_HF = "",
                OPPTAK_NUMMER = "", 
                OPPTAK = "",
                LEVEL = "RHF")

befolkning_per_opptaksomrade_TSB <- rbind(befolkning_per_opptaksomrade_TSB_HF, befolkning_per_opptaksomrade_TSB_RHF)
# -

# ## DPS
# OBS: legg til fiks for postnummere!
#
# ### Kodeliste for opptaksområder i spesialisthelsetjenesten (DPS)

diff <- diff %>%
select(Region, OPPTAK_NUMMER, OPPTAK, Alder, Kjonn, diff) %>%
dplyr::rename(KJOENN = Kjonn, 
             ALDER = Alder, 
             GRUNNKRETSNUMMER = Region, 
             PERSONER = diff) %>%
dplyr::mutate(ORGNR_HF = case_when(GRUNNKRETSNUMMER == "4204" ~ "983975240", 
                                  GRUNNKRETSNUMMER == "5001" ~ "883974832"), 
             NAVN_HF = case_when(GRUNNKRETSNUMMER == "4204" ~ "SØRLANDET SYKEHUS HF", 
                                  GRUNNKRETSNUMMER == "5001" ~ "ST OLAVS HOSPITAL HF"), 
             ORGNR_RHF = case_when(GRUNNKRETSNUMMER == "4204" ~ "991324968", 
                                  GRUNNKRETSNUMMER == "5001" ~ "983658776"), 
             NAVN_RHF = case_when(GRUNNKRETSNUMMER == "4204" ~ "HELSE SØR-ØST RHF", 
                                  GRUNNKRETSNUMMER == "5001" ~ "HELSE MIDT-NORGE RHF"))

befolkning_per_postkrets <- befolkning_per_postkrets %>%
ungroup() %>%
select(-KOMMUNENR)

# +
opptaksomrader_DPS_KLASS <- klassR::GetKlass(632, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

# kristiansand <- opptaksomrader_DPS_KLASS %>%
#   filter(GRUNNKRETSNUMMER == "4608")
# unique(kristiansand$GRUNNKRETSNUMMER)

befolkning_per_grunnkrets_postkrets <- befolkning_per_grunnkrets %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("4204", "5001"))

befolkning_per_grunnkrets_postkrets <- rbind(befolkning_per_grunnkrets_postkrets, befolkning_per_postkrets)

opptaksomrader_DPS_KLASS <- dplyr::left_join(befolkning_per_grunnkrets_postkrets, opptaksomrader_DPS_KLASS, by = "GRUNNKRETSNUMMER")
sum(T07459$value)-sum(opptaksomrader_DPS_KLASS$PERSONER)
# -

opptaksomrader_DPS_KLASS <- rbind(opptaksomrader_DPS_KLASS, diff)
sum(T07459$value)-sum(opptaksomrader_DPS_KLASS$PERSONER)

# +
test <- opptaksomrader_DPS_KLASS %>%
group_by(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
tally() %>%
ungroup() %>%
arrange(OPPTAK_NUMMER)

test
# -

# ### Lager befolkningstall per DPS-område

# +
befolkning_per_opptaksomrade_DPS_TOT <- opptaksomrader_DPS_KLASS %>%
  dplyr::group_by(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999", 
                KJOENN = "0") %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_DPS_kjonn <- opptaksomrader_DPS_KLASS %>%
  dplyr::group_by(KJOENN, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER = "999", 
                ALDER_KODE = "999",) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_DPS_alder <- opptaksomrader_DPS_KLASS %>%
  dplyr::group_by(ALDER, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(KJOENN = "0",
                ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
                ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_DPS_kjonn_alder <- opptaksomrader_DPS_KLASS %>%
  dplyr::group_by(KJOENN, ALDER, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ALDER_KODE = stringr::str_pad(ALDER, width = 3, "left", pad = "0"),
                ALDER_KODE = dplyr::case_when(as.numeric(ALDER_KODE) >= 105 ~ "105+", TRUE ~ ALDER_KODE)) %>%
  dplyr::arrange(PERSONER)

befolkning_per_opptaksomrade_DPS_DPS <- rbind(befolkning_per_opptaksomrade_DPS_TOT, befolkning_per_opptaksomrade_DPS_kjonn, befolkning_per_opptaksomrade_DPS_alder, befolkning_per_opptaksomrade_DPS_kjonn_alder) %>%
dplyr::select(-ALDER) %>%  
dplyr::mutate(TJENESTE = "DPS", 
               LEVEL = "DPS")
# -

# ### Aggregerer til HF- og RHF-nivå

# +
befolkning_per_opptaksomrade_DPS_HF <- befolkning_per_opptaksomrade_DPS_DPS %>%
  dplyr::group_by(TJENESTE, KJOENN, ALDER_KODE, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(OPPTAK_NUMMER = "", 
                OPPTAK = "",
                LEVEL = "HF")

befolkning_per_opptaksomrade_DPS_RHF <- befolkning_per_opptaksomrade_DPS_DPS %>%
  dplyr::group_by(TJENESTE, KJOENN, ALDER_KODE, ORGNR_RHF, NAVN_RHF) %>%
  dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
  dplyr::mutate(ORGNR_HF = "",
                NAVN_HF = "",
                OPPTAK_NUMMER = "", 
                OPPTAK = "",
                LEVEL = "RHF")

befolkning_per_opptaksomrade_DPS <- rbind(befolkning_per_opptaksomrade_DPS_DPS, befolkning_per_opptaksomrade_DPS_HF, befolkning_per_opptaksomrade_DPS_RHF)
# -

# ### Legger sammen alle opptaksområdene

befolkning_per_opptaksomrade <- rbind(befolkning_per_opptaksomrade_SOM, befolkning_per_opptaksomrade_PHV, befolkning_per_opptaksomrade_TSB, befolkning_per_opptaksomrade_DPS)

# ### Lagrer filen

arrow::write_parquet(befolkning_per_opptaksomrade, befolkning_per_opptaksomrade_masterfil_filsti)

befolkning_per_opptaksomrade %>%
filter(ALDER_KODE == "999", KJOENN == 0) %>%
group_by(TJENESTE, LEVEL, ALDER_KODE, KJOENN) %>%
summarise(PERSONER = sum(PERSONER)) %>%
mutate(PERSONER_STATBANK = sum(T07459$value))
