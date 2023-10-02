# aargang <- 2023
if (exists("aargang_master")==TRUE){
aargang <- aargang_master    
}



# +
# last_opp_til_statbank <- TRUE

# if (last_opp_til_statbank == TRUE & exists("username_encryptedpassword") == FALSE){
#   username_encryptedpassword <- fellesr:::statbank_encrypt_request(laste_bruker = lastebruker)
# }

# +
# renv::restore("/ssb/bruker/rdn/speshelse")

options(scipen=999)

suppressPackageStartupMessages({
  library(tidyverse)
  library(PxWebApiData)
  library(lubridate)
  library(fellesr)
})

# +
arbeidsmappe <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/befolkning_per_opptaksomrade/")
# befolkning_per_grunnkrets_filsti <- paste0(arbeidsmappe, "inndata/befolkning_per_grunnkrets_", aargang, ".parquet")
befolkning_per_opptaksomrade_masterfil_filsti <- paste0(arbeidsmappe, "masterfil/befolkning_per_opptaksomrade_masterfil_", aargang, ".parquet")

statbank_mappe <- paste0(arbeidsmappe, "statbank/")
statbank_mappe

# +
# Lager publisert mappe #
publisert_mappe <- paste0(arbeidsmappe, "statbank/", publiseringsdato, "/")

if (file.exists(publisert_mappe)==FALSE) {
dir.create(publisert_mappe)
}

# Flytt alle masterfiler fra paste0(arbeidsmappe, "masterfil") til paste0(publisert_mappe, "masterfil")
if (file.exists(paste0(publisert_mappe, "masterfil"))==FALSE) {
dir.create(paste0(publisert_mappe, "masterfil"))
}

# Flytt alle .dat-filer fra paste0(arbeidsmappe, "statbank") til paste0(publisert_mappe, "lastefiler")
if (file.exists(paste0(publisert_mappe, "lastefiler"))==FALSE) {
dir.create(paste0(publisert_mappe, "lastefiler"))
}
# -

speshelse141 <- arrow::read_parquet(paste0(statbank_mappe, "speshelse141.parquet"))
speshelse142 <- arrow::read_parquet(paste0(statbank_mappe, "speshelse142.parquet"))
speshelse143 <- arrow::read_parquet(paste0(statbank_mappe, "speshelse143.parquet"))
speshelse144 <- arrow::read_parquet(paste0(statbank_mappe, "speshelse144.parquet"))
speshelse145 <- arrow::read_parquet(paste0(statbank_mappe, "speshelse145.parquet"))

# +
transfer_log_speshelse14 <- statbank_lasting(lastefil = list(speshelse141, speshelse142, speshelse143, speshelse144, speshelse145),
# transfer_log_speshelse14 <- statbank_lasting(lastefil = list(speshelse141, speshelse142, speshelse143, speshelse144),
                                 tabell_id = tabellid,
                                 laste_bruker = lastebruker,
                                 publiseringsdato = publiseringsdato,
                                 validering = TRUE,
                                 ask = FALSE,
                                 username_encryptedpassword = username_encryptedpassword)

if (class(transfer_log_speshelse14) != "response"){
    print(transfer_log_speshelse14)
    # print(unique(transfer_log_speshelse14$HelseReg))
}
