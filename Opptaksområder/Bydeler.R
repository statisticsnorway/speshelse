# # Bydeler

library(tidyverse)

aargang <- 2024

bydeler_KLASS <- klassR::GetKlass(1, 
                                  correspond = 103,
                                  date = c(paste0(aargang, "-01-01"))) %>%
rename(GRUNNKRETSNUMMER = sourceCode, 
      BYDELSNUMMER = targetCode, 
      BYDELSNAVN = targetName) %>%
select(GRUNNKRETSNUMMER, BYDELSNUMMER, BYDELSNAVN)

# ## Somatikk

# +
# klassR::GetKlass(1, 
#                                   correspond = 103,
#                                   date = c(paste0(aargang, "-01-01"))) %>%
# rename(GRUNNKRETSNUMMER = sourceCode, 
#        GRUNNKRETSNAVN = sourceName,
#       BYDELSNUMMER = targetCode, 
#       BYDELSNAVN = targetName) %>%
# select(GRUNNKRETSNUMMER, GRUNNKRETSNAVN, BYDELSNUMMER, BYDELSNAVN) %>%
# filter(BYDELSNAVN == "Sentrum")

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

opptaksomrader_KLASS_SOM <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
filter(GRUNNKRETSNUMMER != "03019999") %>%
dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") %>%
distinct(BYDELSNUMMER, BYDELSNAVN, OPPTAK) %>%
arrange(BYDELSNUMMER) %>%
rename(OPPTAK_SOM = OPPTAK)

opptaksomrader_KLASS_SOM
# -

# ## PHV

# +
# klassR::GetKlass(1, 
#                                   correspond = 103,
#                                   date = c(paste0(aargang, "-01-01"))) %>%
# rename(GRUNNKRETSNUMMER = sourceCode, 
#        GRUNNKRETSNAVN = sourceName,
#       BYDELSNUMMER = targetCode, 
#       BYDELSNAVN = targetName) %>%
# select(GRUNNKRETSNUMMER, GRUNNKRETSNAVN, BYDELSNUMMER, BYDELSNAVN) %>%
# filter(BYDELSNAVN == "Sentrum")

# +
opptaksomrader_KLASS <- klassR::GetKlass(630, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3, 
             # OPPTAK_NUMMER = code3, 
             # OPPTAK = name3, 
             ORGNR_HF_PHV = code2, 
             NAVN_HF_PHV = name2, 
             ORGNR_RHF_PHV = code1, 
             NAVN_RHF_PHV = name1)

opptaksomrader_KLASS_PHV <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
filter(GRUNNKRETSNUMMER != "03019999") %>%
dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") %>%
distinct(BYDELSNUMMER, BYDELSNAVN, NAVN_HF_PHV) %>%
# group_by(BYDELSNUMMER, BYDELSNAVN, NAVN_HF_PHV) %>%
# tally() %>%
arrange(BYDELSNUMMER) %>%
rename(OPPTAK_PHV = NAVN_HF_PHV)

opptaksomrader_KLASS_PHV

# +
# opptaksomrader_KLASS %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
# filter(GRUNNKRETSNUMMER != "03019999") %>%
# dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") %>%
# filter(is.na(BYDELSNUMMER))
# # filter(BYDELSNAVN == "Sagene" & NAVN_HF_PHV == "LOVISENBERG DIAKONALE SYKEHUS AS")
# -

# ## TSB

# +
# klassR::GetKlass(1, 
#                                   correspond = 103,
#                                   date = c(paste0(aargang, "-01-01"))) %>%
# rename(GRUNNKRETSNUMMER = sourceCode, 
#        GRUNNKRETSNAVN = sourceName,
#       BYDELSNUMMER = targetCode, 
#       BYDELSNAVN = targetName) %>%
# select(GRUNNKRETSNUMMER, GRUNNKRETSNAVN, BYDELSNUMMER, BYDELSNAVN) %>%
# filter(BYDELSNAVN == "Sentrum")

# +
# obs <- klassR::GetKlass(631, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
# dplyr::rename(GRUNNKRETSNUMMER = code3, 
#              GRUNNKRETS_NAVN = name3, 
#              # OPPTAK_NUMMER = code3, 
#              # OPPTAK = name3, 
#              ORGNR_HF_TSB = code2, 
#              NAVN_HF_TSB = name2, 
#              ORGNR_RHF_TSB = code1, 
#              NAVN_RHF_TSB = name1) %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
# filter(GRUNNKRETSNUMMER != "03019999") %>%
# dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") 

# obs %>%
# filter(BYDELSNAVN == "St. Hanshaugen" & NAVN_HF_TSB == "LOVISENBERG DIAKONALE SYKEHUS AS")

# obs %>%
# group_by(BYDELSNUMMER, BYDELSNAVN, NAVN_HF_TSB) %>%
# tally()

# +
opptaksomrader_KLASS <- klassR::GetKlass(631, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code3, 
             GRUNNKRETS_NAVN = name3, 
             # OPPTAK_NUMMER = code3, 
             # OPPTAK = name3, 
             ORGNR_HF_TSB = code2, 
             NAVN_HF_TSB = name2, 
             ORGNR_RHF_TSB = code1, 
             NAVN_RHF_TSB = name1)

opptaksomrader_KLASS_TSB <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
filter(GRUNNKRETSNUMMER != "03019999") %>%
dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") %>%
distinct(BYDELSNUMMER, BYDELSNAVN, NAVN_HF_TSB) %>%
arrange(BYDELSNUMMER) %>%
rename(OPPTAK_TSB = NAVN_HF_TSB)

opptaksomrader_KLASS_TSB

# +
# opptaksomrader_KLASS %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
# filter(GRUNNKRETSNUMMER != "03019999") %>%
# dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") %>%
# # filter(is.na(BYDELSNUMMER))
# filter(BYDELSNAVN == "St. Hanshaugen" & NAVN_HF_TSB == "LOVISENBERG DIAKONALE SYKEHUS AS")
# -

# ## DPS

# +
if (aargang %in% 2021:2023){
opptaksomrader_KLASS <- klassR::GetKlass(632, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

opptaksomrader_KLASS_DPS <- opptaksomrader_KLASS %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
filter(GRUNNKRETSNUMMER != "03019999") %>%
dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") %>%
distinct(BYDELSNUMMER, BYDELSNAVN, OPPTAK) %>%
arrange(BYDELSNUMMER) %>%
rename(OPPTAK_DPS = OPPTAK)
    
    opptaksomrader_KLASS_DPS
    }
# -
# ## Legger sammen alle

if (aargang %in% 2021:2023){
opptaksomrader_KLASS_SOM %>%
dplyr::left_join(opptaksomrader_KLASS_PHV, by = c("BYDELSNUMMER", "BYDELSNAVN")) %>%
dplyr::left_join(opptaksomrader_KLASS_TSB, by = c("BYDELSNUMMER", "BYDELSNAVN")) %>%
dplyr::left_join(opptaksomrader_KLASS_DPS, by = c("BYDELSNUMMER", "BYDELSNAVN"))
    } else {
opptaksomrader_KLASS_SOM %>%
dplyr::left_join(opptaksomrader_KLASS_PHV, by = c("BYDELSNUMMER", "BYDELSNAVN")) %>%
dplyr::left_join(opptaksomrader_KLASS_TSB, by = c("BYDELSNUMMER", "BYDELSNAVN"))    
}




