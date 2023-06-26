# -*- coding: utf-8 -*-
# aargang <- 2021
if (exists("aargang_master")==TRUE){
aargang <- aargang_master    
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

# +
arbeidsmappe <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/befolkning_per_opptaksomrade/")
# befolkning_per_grunnkrets_filsti <- paste0(arbeidsmappe, "inndata/befolkning_per_grunnkrets_", aargang, ".parquet")
befolkning_per_opptaksomrade_masterfil_filsti <- paste0(arbeidsmappe, "masterfil/befolkning_per_opptaksomrade_masterfil_", aargang, ".parquet")

statbank_mappe <- paste0(arbeidsmappe, "statbank/")
statbank_mappe

if (file.exists(statbank_mappe)==FALSE) {
dir.create(statbank_mappe)
}
# -

befolkning_per_opptaksomrade_masterfil <- arrow::read_parquet(befolkning_per_opptaksomrade_masterfil_filsti)

# ## Hele landet
#
# speshelse141.dat (13982: Spesialisthelsetjenesten. Befolkning per opptaksområde. Hele landet)

unique(befolkning_per_opptaksomrade_masterfil$LEVEL)
unique(befolkning_per_opptaksomrade_masterfil$TJENESTE)
unique(befolkning_per_opptaksomrade_masterfil$KJOENN)
unique(befolkning_per_opptaksomrade_masterfil$ALDER_KODE)

# +
# befolkning_per_opptaksomrade_masterfil %>%
# filter(is.na(ALDER_KODE))
# -

head(befolkning_per_opptaksomrade_masterfil)

# +
speshelse141 <- befolkning_per_opptaksomrade_masterfil %>%
filter(LEVEL == "HF", 
       # TJENESTE == "SOM", # Legg til egen for VOP/BUP 
       # KJOENN == "0", 
       ALDER_KODE != "999") %>%
group_by(TJENESTE, KJOENN, ALDER_KODE) %>%
summarise(PERSONER = sum(PERSONER), .groups = "drop") %>%
mutate(REGION = "H00", TID = aargang, PERSONER_SPES = "") %>%
select(REGION, TJENESTE, KJOENN, ALDER_KODE, TID, PERSONER, PERSONER_SPES)

speshelse141

arrow::write_parquet(speshelse141, paste0(statbank_mappe, "speshelse141.parquet"))
# -

# ## Helseregion
#
# Erstatt orgnr med regionsnummer!

# +
speshelse142 <- befolkning_per_opptaksomrade_masterfil %>%
filter(LEVEL == "HF", 
       # TJENESTE == "SOM", # Legg til egen for VOP/BUP 
       # KJOENN == "0", 
       ALDER_KODE != "999") %>%
group_by(ORGNR_RHF, TJENESTE, KJOENN, ALDER_KODE) %>%
summarise(PERSONER = sum(PERSONER), .groups = "drop") %>%
mutate(TID = aargang, PERSONER_SPES = "") %>%
dplyr::mutate(REGION = case_when(
ORGNR_RHF == "883658752" ~ "H05", 
ORGNR_RHF == "983658725" ~ "H03", 
ORGNR_RHF == "983658776" ~ "H04", 
ORGNR_RHF == "991324968" ~ "H12"
)) %>%
select(REGION, TJENESTE, KJOENN, ALDER_KODE, TID, PERSONER, PERSONER_SPES) 


speshelse142

arrow::write_parquet(speshelse142, paste0(statbank_mappe, "speshelse142.parquet"))
# -

# ## Helseforetak
#
# speshelse143.dat (13982: Spesialisthelsetjenesten. Befolkning per opptaksområde. Helseforetak)

# +
speshelse143 <- befolkning_per_opptaksomrade_masterfil %>%
filter(LEVEL == "HF", 
       # TJENESTE == "SOM", # Legg til egen for VOP/BUP 
       # KJOENN == "0", 
       ALDER_KODE != "999") %>%
group_by(ORGNR_HF, TJENESTE, KJOENN, ALDER_KODE) %>%
summarise(PERSONER = sum(PERSONER), .groups = "drop") %>%
mutate(TID = aargang, PERSONER_SPES = "") %>%
select(ORGNR_HF, TJENESTE, KJOENN, ALDER_KODE, TID, PERSONER, PERSONER_SPES)

head(speshelse143)

arrow::write_parquet(speshelse143, paste0(statbank_mappe, "speshelse143.parquet"))
# -

# ## Lokalsykehus
#
# speshelse144.dat (13982: Spesialisthelsetjenesten. Befolkning per opptaksområde. Lokalsykehus

# +
speshelse144 <- befolkning_per_opptaksomrade_masterfil %>%
filter(LEVEL == "Lokasjon", 
       # TJENESTE == "SOM", # Legg til egen for VOP/BUP 
       # KJOENN == "0", 
       ALDER_KODE != "999") %>%
group_by(OPPTAK_NUMMER, TJENESTE, KJOENN, ALDER_KODE) %>%
summarise(PERSONER = sum(PERSONER), .groups = "drop") %>%
mutate(TID = aargang, PERSONER_SPES = "") %>%
select(OPPTAK_NUMMER, TJENESTE, KJOENN, ALDER_KODE, TID, PERSONER, PERSONER_SPES)

head(speshelse144)

arrow::write_parquet(speshelse144, paste0(statbank_mappe, "speshelse144.parquet"))
# -

# ## DPS
#
# speshelse145.dat (13982: Spesialisthelsetjenesten. Befolkning per opptaksområde. Opptaksområde dps)

# +
if (aargang %in% DPS_OK){
speshelse145 <- befolkning_per_opptaksomrade_masterfil %>%
filter(LEVEL == "DPS", 
       # TJENESTE == "DPS", # Legg til egen for VOP/BUP 
       # KJOENN == "0", 
       ALDER_KODE != "999") %>%
filter(!is.na(ALDER_KODE)) %>%
group_by(OPPTAK_NUMMER, TJENESTE, KJOENN, ALDER_KODE) %>%
summarise(PERSONER = sum(PERSONER), .groups = "drop") %>%
mutate(TID = aargang, PERSONER_SPES = "") %>%
select(OPPTAK_NUMMER, TJENESTE, KJOENN, ALDER_KODE, TID, PERSONER, PERSONER_SPES)
}

if (!aargang %in% DPS_OK){
speshelse145 <- data.frame(OPPTAK_NUMMER = "D69",  TJENESTE = "VOP", KJOENN = "2", ALDER_KODE = "105+", TID = aargang, PERSONER = "", PERSONER_SPES = "")
speshelse145
    }

arrow::write_parquet(speshelse145, paste0(statbank_mappe, "speshelse145.parquet"))
