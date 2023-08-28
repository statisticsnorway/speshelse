aargang <- 2023

library(tidyverse)

arbeidsmappe <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/befolkning_per_opptaksomrade/")
befolkning_per_grunnkrets_filsti <- paste0(arbeidsmappe, "inndata/befolkning_per_grunnkrets_", aargang, ".parquet")
befolkning_per_opptaksomrade_masterfil_filsti <- paste0(arbeidsmappe, "masterfil/befolkning_per_opptaksomrade_masterfil_", aargang, ".parquet")

befolkning_per_opptaksomrade_masterfil <- arrow::read_parquet(befolkning_per_opptaksomrade_masterfil_filsti)

befolkning_per_opptaksomrade_lokasjon <- befolkning_per_opptaksomrade_masterfil %>%
filter(LEVEL == "Lokasjon", ALDER_KODE == "999", KJOENN == "0") %>%
group_by(ORGNR_RHF, NAVN_RHF, ORGNR_HF, NAVN_HF, OPPTAK_NUMMER, OPPTAK) %>%
summarise(PERSONER = sum(PERSONER))

# Lagrer filen
write.csv2(befolkning_per_opptaksomrade_lokasjon, 
           file = paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/bestillinger/befolkning_per_opptaksomrade_lokasjon_", aargang, ".csv"), 
           fileEncoding = "UTF-8", 
           row.names = FALSE,
           na = "")
