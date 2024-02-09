# -*- coding: utf-8 -*-
# # Befolkning per opptaksområde
#
# Gjør programmet kjørbart på Dapla? Krever pseudonomisert befolkningsfil
#
# Dersom programmet skal kjøres for ny årgang (og `befolkning_per_grunnkrets` og `befolkning_per_postkrets` ikke finnes) må programmet kjøres i RStudio for å ikke kræsje!
#
# Kjør 2015-2023 på nytt!

# +
aargang_master <- 2023

aargang <- aargang_master # aargang_master overskriver aargang dersom denne er definert i script som kjøres i produksjonsløpet
# -

DPS_OK <- 2021:2023

# +
last_opp_til_statbank <- FALSE

# publiseringsdato <- "2023-06-26"
tabellid <- "13982"
lastebruker <- "LAST330"

if (last_opp_til_statbank == TRUE & exists("username_encryptedpassword") == FALSE){
  username_encryptedpassword <- fellesr:::statbank_encrypt_request(laste_bruker = lastebruker)
}
# -

# ### Kommunenummer for Kristiansand og Trondheim
#
# OBS: dersom Kristiansand eller Trondheim får nye kommunenummer må det oppdateres i cellen nedenfor.

# +
if (aargang >= 2020){
  kristiansand_kommnr <- "4204"
  trondheim_kommnr <- "5001"
}

if (aargang %in% 2018:2019){
  kristiansand_kommnr <- "1001"
  trondheim_kommnr <- "5001"
}

if (aargang %in% 2015:2017){
  kristiansand_kommnr <- "1001"
  trondheim_kommnr <- "1601"
}

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
befolkning_per_opptaksomrade_masterfil_filsti <- paste0(arbeidsmappe, "masterfil/befolkning_per_opptaksomrade_masterfil_", aargang, ".parquet")

befolkning_per_postkrets_filsti <- paste0(arbeidsmappe, "/inndata/befolkning_per_postkrets_", aargang, ".parquet")
befolkning_per_grunnkrets_filsti <- paste0(arbeidsmappe, "/inndata/befolkning_per_grunnkrets_", aargang, ".parquet")

# +
if (file.exists(befolkning_per_grunnkrets_filsti) == FALSE){
    source("/ssb/bruker/rdn/speshelse/Opptaksområder/Befolkning per opptaksområde/Befolkning per grunnkrets.R")
}

befolkning_per_grunnkrets <- arrow::read_parquet(befolkning_per_grunnkrets_filsti)

sum(befolkning_per_grunnkrets$PERSONER)

if (aargang %in% 2019){
befolkning_per_grunnkrets <- befolkning_per_grunnkrets %>%
    dplyr::mutate(GRUNNKRETSNUMMER = dplyr::case_when(GRUNNKRETSNUMMER == "09060109" ~ "09061101", TRUE ~ GRUNNKRETSNUMMER))
}

if (aargang %in% 2018){
befolkning_per_grunnkrets <- befolkning_per_grunnkrets %>%
    dplyr::mutate(GRUNNKRETSNUMMER = dplyr::case_when(GRUNNKRETSNUMMER == "03014201" ~ "03014211", TRUE ~ GRUNNKRETSNUMMER))
}

if (aargang %in% 2016){
befolkning_per_grunnkrets <- befolkning_per_grunnkrets %>%
    dplyr::mutate(GRUNNKRETSNUMMER = dplyr::case_when(GRUNNKRETSNUMMER == "01010403" ~ "01010102", TRUE ~ GRUNNKRETSNUMMER))
}

# +
# befolkning_per_grunnkrets %>%
# filter(GRUNNKRETSNUMMER == "03014201")

# +
# head(befolkning_per_grunnkrets)

# +
if (file.exists(befolkning_per_postkrets_filsti) == FALSE){
    source("/ssb/bruker/rdn/speshelse/Opptaksområder/Befolkning per opptaksområde/Befolkning per postkrets.R", encoding = "UTF-8")
}

befolkning_per_postkrets <- arrow::read_parquet(befolkning_per_postkrets_filsti) %>%
dplyr::rename(GRUNNKRETSNUMMER = POSTNR)
sum(befolkning_per_postkrets$PERSONER)
# -

befolkning_per_postkrets %>%
group_by(KOMMUNENR) %>%
summarise(PERSONER = sum(PERSONER)) 

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
  dplyr::filter(Region %in% c(trondheim_kommnr, kristiansand_kommnr)) %>%
  dplyr::group_by(Region, Alder, Kjonn) %>%
  dplyr::summarise(PERSONER = sum(value)) %>%
filter(PERSONER != 0)

# +
sum(T07459_diff$PERSONER)
sum(befolkning_per_postkrets$PERSONER)

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
diff_postkrets <- dplyr::full_join(T07459_diff, befolkning_per_postkrets_diff, by = c("Region" = "KOMMUNENR",
                                                                           "Alder" = "ALDER",
                                                                           "Kjonn" = "KJOENN")) %>%
  dplyr::mutate(PERSONER = replace_na(PERSONER, 0),
                diff = PERSONER-PERSONER_EGEN,
                PERSONER_EGEN_2 = PERSONER_EGEN+diff,
               OPPTAK_NUMMER = case_when(Region == kristiansand_kommnr ~ "D62",
                                        Region == trondheim_kommnr ~ "D33"),
               OPPTAK = case_when(Region == kristiansand_kommnr ~ "Solvang",
                                        Region == trondheim_kommnr ~ "Nidaros"))

# sum(diff$diff)
# sum(diff$PERSONER)
# sum(diff$PERSONER_EGEN)

# diff

# +
befolkning_per_grunnkrets %>%
filter(GRUNNKRETSNUMMER == "09060109")

# OBS: 2018 - 03014201
# OBS: 2017 - 04239999
# OBS: 2016 - 01010403
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

# +
opptaksomrader_SOM_KLASS_missing <- opptaksomrader_SOM_KLASS %>%
filter(is.na(OPPTAK)) %>%
group_by(GRUNNKRETSNUMMER) %>%
tally()

if (nrow(opptaksomrader_SOM_KLASS_missing) != 0){
    print(opptaksomrader_SOM_KLASS_missing)
    stop()
}
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
# test <- befolkning_per_opptaksomrade_SOM_HF %>%
# filter(KJOENN == 0, 
#        NAVN_HF %in% c("LOVISENBERG DIAKONALE SYKEHUS AS", "SYKEHUSET INNLANDET HF"), 
#        ALDER_KODE != 999) %>%
# mutate(ALDER = as.numeric(ALDER_KODE))

# options(repr.plot.width = 20, repr.plot.height = 10)

# ggplot(test, aes(x = ALDER, y = PERSONER, color = NAVN_HF, group = NAVN_HF)) +
#   geom_line() +
#   labs(x = "Age", y = "Number of People") +
#   scale_color_manual(values = c("LOVISENBERG DIAKONALE SYKEHUS AS" = "black", "SYKEHUSET INNLANDET HF" = "green")) +
#   theme_minimal()

# +
# unique(befolkning_per_opptaksomrade_SOM_HF$NAVN_HF)

# +
# test <- befolkning_per_opptaksomrade_SOM_HF %>%
# filter(KJOENN == 0, 
#        # NAVN_HF %in% c("LOVISENBERG DIAKONALE SYKEHUS AS", 
#        #                "SYKEHUSET INNLANDET HF", 
#        #               "DIAKONHJEMMET SYKEHUS AS", 
#        #               "OSLO UNIVERSITETSSYKEHUS HF"), 
#        ALDER_KODE != 999) %>%
# mutate(ALDER = as.numeric(ALDER_KODE)) %>%
# mutate(ALDER_NY = case_when(ALDER >= 67 ~ "Andel 67+", 
#                               ALDER_KODE == "105+" ~ "Andel 67+", 
#                             ALDER %in% 0:5 ~ "Andel 0-5 år", 
#                             ALDER %in% 6:15 ~ "Andel 6-15 år", 
#                             ALDER %in% 16:66 ~"Andel 16-66 år",
#       TRUE ~ "OBS"))

# +
# tot_per_HF <- test %>%
# group_by(NAVN_HF) %>%
# summarise(PERSONER_TOT = sum(PERSONER))

# test %>%
# group_by(NAVN_HF, ALDER_NY) %>%
# summarise(PERSONER = sum(PERSONER)) %>%
# left_join(tot_per_HF, by = "NAVN_HF") %>%
# mutate(ANDEL = round(PERSONER/PERSONER_TOT*100, digits = 1)) %>%
# filter(ALDER_NY == "Andel 67+")
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

# +
opptaksomrader_PHV_KLASS_missing <- opptaksomrader_PHV_KLASS %>%
filter(is.na(NAVN_HF)) %>%
group_by(GRUNNKRETSNUMMER) %>%
tally() # %>%
# mutate(GRUNNKRETSNUMMER = substr(GRUNNKRETSNUMMER, 2, 8))

if (nrow(opptaksomrader_PHV_KLASS_missing) != 0){
    print(opptaksomrader_PHV_KLASS_missing)
    stop()
}
# -

opptaksomrader_PHV_KLASS_missing %>%
mutate(GRUNNKRETSNUMMER = substr(GRUNNKRETSNUMMER, 2, 8))

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
    as.numeric(ALDER) <= 17 ~ "BUP",
    as.numeric(ALDER) >= 18 ~ "VOP"),
    LEVEL = "HF",
    OPPTAK_NUMMER = "",
    OPPTAK = "") %>%
  dplyr::select(-ALDER)

# +
# befolkning_per_opptaksomrade_PHV_TOT <- befolkning_per_opptaksomrade_PHV_HF %>%
#   filter(LEVEL == "HF", KJOENN == 0) %>%
#   dplyr::group_by(TJENESTE, KJOENN, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
#   dplyr::summarise(PERSONER = sum(PERSONER), .groups = 'drop') %>%
#   dplyr::mutate(ALDER = "999",
#                 ALDER_KODE = "999",
#                 KJOENN = "0") %>%
#   dplyr::arrange(PERSONER)

# head(befolkning_per_opptaksomrade_PHV_TOT)

# sum(befolkning_per_opptaksomrade_PHV_TOT$PERSONER)

# befolkning_per_opptaksomrade_PHV_TOT %>%
# group_by(TJENESTE, KJOENN) %>%
# summarise(PERSONER= sum(PERSONER))
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

# +
# befolkning_per_opptaksomrade_PHV %>%
# filter(TJENESTE == "BUP", ALDER_KODE != "999") %>%
# head()

# +
# befolkning_per_opptaksomrade_PHV %>%
# filter(TJENESTE == "BUP")
# -

# ## TSB
#
# ### Kodeliste for opptaksområder i spesialisthelsetjenesten (TSB)

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

# +
opptaksomrader_TSB_KLASS_missing <- opptaksomrader_TSB_KLASS %>%
filter(is.na(NAVN_HF)) %>%
group_by(GRUNNKRETSNUMMER) %>%
tally()

if (nrow(opptaksomrader_TSB_KLASS_missing) != 0){
    print(opptaksomrader_TSB_KLASS_missing)
    stop()
}
# -

opptaksomrader_TSB_KLASS_missing %>%
mutate(GRUNNKRETSNUMMER = substr(GRUNNKRETSNUMMER, 2, 8))

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
# OBS: missing for alder (og negativt tall (-1) for personer???)
#
# ### Kodeliste for opptaksområder i spesialisthelsetjenesten (DPS)

# +
if (aargang %in% DPS_OK){
diff_postkrets_2 <- diff_postkrets %>%
select(Region, OPPTAK_NUMMER, OPPTAK, Alder, Kjonn, diff) %>%
dplyr::rename(KJOENN = Kjonn,
             ALDER = Alder,
             GRUNNKRETSNUMMER = Region,
             PERSONER = diff) %>%
dplyr::mutate(ORGNR_HF = case_when(GRUNNKRETSNUMMER == kristiansand_kommnr ~ "983975240",
                                  GRUNNKRETSNUMMER == trondheim_kommnr ~ "883974832"),
             NAVN_HF = case_when(GRUNNKRETSNUMMER == kristiansand_kommnr ~ "SØRLANDET SYKEHUS HF",
                                  GRUNNKRETSNUMMER == trondheim_kommnr ~ "ST OLAVS HOSPITAL HF"),
             ORGNR_RHF = case_when(GRUNNKRETSNUMMER == kristiansand_kommnr ~ "991324968",
                                  GRUNNKRETSNUMMER == trondheim_kommnr ~ "983658776"),
             NAVN_RHF = case_when(GRUNNKRETSNUMMER == kristiansand_kommnr ~ "HELSE SØR-ØST RHF",
                                  GRUNNKRETSNUMMER == trondheim_kommnr ~ "HELSE MIDT-NORGE RHF"))

befolkning_per_postkrets <- befolkning_per_postkrets %>%
ungroup() %>%
select(-KOMMUNENR)

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
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c(kristiansand_kommnr, trondheim_kommnr))

befolkning_per_grunnkrets_postkrets <- rbind(befolkning_per_grunnkrets_postkrets, befolkning_per_postkrets)

opptaksomrader_DPS_KLASS <- dplyr::left_join(befolkning_per_grunnkrets_postkrets, opptaksomrader_DPS_KLASS, by = "GRUNNKRETSNUMMER")
sum(T07459$value)-sum(opptaksomrader_DPS_KLASS$PERSONER)

obs <- opptaksomrader_DPS_KLASS %>%
filter(is.na(OPPTAK_NUMMER))

obs

unique(obs$GRUNNKRETSNUMMER)

# opptaksomrader_DPS_KLASS %>%
# filter(is.na(OPPTAK_NUMMER))

opptaksomrader_DPS_KLASS <- rbind(opptaksomrader_DPS_KLASS, diff_postkrets_2)
sum(T07459$value)-sum(opptaksomrader_DPS_KLASS$PERSONER)

unique(opptaksomrader_DPS_KLASS$OPPTAK_NUMMER)

test <- opptaksomrader_DPS_KLASS %>%
group_by(OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
tally() %>%
ungroup() %>%
arrange(OPPTAK_NUMMER)

test
    }
# -

# ### Lager befolkningstall per DPS-område

# +
if (aargang %in% DPS_OK){
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
    }
# -

# ### Aggregerer til HF- og RHF-nivå

# +
if (aargang %in% DPS_OK){
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
    }
# -

# ### Legger sammen alle opptaksområdene

# +
if (!aargang %in% DPS_OK){
befolkning_per_opptaksomrade <- rbind(befolkning_per_opptaksomrade_SOM, befolkning_per_opptaksomrade_PHV, befolkning_per_opptaksomrade_TSB)
    }

if (aargang %in% DPS_OK){
befolkning_per_opptaksomrade <- rbind(befolkning_per_opptaksomrade_SOM, befolkning_per_opptaksomrade_PHV, befolkning_per_opptaksomrade_TSB, befolkning_per_opptaksomrade_DPS)
    }
# -

# ### Lagrer filen

arrow::write_parquet(befolkning_per_opptaksomrade, befolkning_per_opptaksomrade_masterfil_filsti)

befolkning_per_opptaksomrade %>%
filter(ALDER_KODE == "999", KJOENN == 0) %>%
group_by(TJENESTE, LEVEL, ALDER_KODE, KJOENN) %>%
summarise(PERSONER = sum(PERSONER)) %>%
mutate(PERSONER_STATBANK = sum(T07459$value))

# +
# befolkning_per_opptaksomrade %>%
# filter(TJENESTE == "BUP")
# -

befolkning_per_opptaksomrade_masterfil_filsti

# +
# head(befolkning_per_opptaksomrade)

# +
# befolkning_per_opptaksomrade %>%
# filter(TJENESTE == "SOM", LEVEL == "HF", ALDER_KODE != "999", KJOENN != "0") %>%
# group_by(ORGNR_RHF, NAVN_RHF, ORGNR_HF, NAVN_HF) %>%
# summarise(PERSONER = sum(PERSONER))
# -

# ### Lager lastefiler

source("./lager_lastefiler.R")

# ### Lasteprogram

if (last_opp_til_statbank == TRUE){
source("./lasteprogram.R")
    }

print("😀")
