# -*- coding: utf-8 -*-
# # Skjemadata til Samdata

aargang <- 2022

# +
suppressPackageStartupMessages({
  library(tidyverse)
})

# OBS: erstatt med fellesr når den er installert på nytt!
source("/ssb/bruker/rdn/fellesr/R/dynarev_uttrekk.R")
# -

# ### Logger på Oracle

# Logg på for å få tilgang til Oracle 
con <- dynarev_uttrekk(con_ask = "con")

# ### Kodeliste for tjenesteområder i spesialisthelsetjenesten

tjenesteomrader <- klassR::GetKlass(610, output_level = 2, date = c(paste0(aargang, "-01-01"))) %>%
  dplyr::rename(SKJEMA = code, 
                TJENESTE = parentCode, 
                DELREG = name) %>%
  dplyr::select(-level) %>%
  dplyr::mutate(DELREG = paste0(DELREG, substr(aargang, 3, 4))) %>%
  dplyr::filter(TJENESTE %in% c("BUP", 
                                # "REH", 
                                "SOM", 
                                "TSB", 
                                "VOP")) %>%
  dplyr::mutate(O_P = grepl("O$", SKJEMA, perl=TRUE)) %>%
  dplyr::mutate(O_P = case_when(
    O_P == TRUE ~ "O", 
    O_P == FALSE ~ "P", 
  ),
  SKJEMA_NR = readr::parse_number(SKJEMA), 
  SKJEMA_NAVN = paste0(SKJEMA_NR, O_P)) 

# ## Skjema 38O

# +
skjema38O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38O",]$SKJEMA,
                             skjema_cols = T, 
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema38O_skjema <- data.frame(skjema38O[1])
skjema38O_dubletter <- data.frame(skjema38O[2])
# -

# ## Skjema 38P

# +
skjema38P <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38P",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38P",]$SKJEMA,
                             skjema_cols = T, 
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema38P_skjema <- data.frame(skjema38P[1])
skjema38P_dubletter <- data.frame(skjema38P[2])
# -

# ## Skjema 44O

# +
skjema44O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44O",]$SKJEMA,
                             skjema_cols = T, 
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema44O_skjema <- data.frame(skjema44O[1])
skjema44O_dubletter <- data.frame(skjema44O[2])
# -

# ## Skjema 44P

# +
skjema44P <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44P",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44P",]$SKJEMA,
                             skjema_cols = T, 
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema44P_skjema <- data.frame(skjema44P[1])
skjema44P_dubletter <- data.frame(skjema44P[2])
# -

# ## Skjema 45O

# +
skjema45O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45O",]$SKJEMA,
                             skjema_cols = T, 
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema45O_skjema <- data.frame(skjema45O[1])
skjema45O_dubletter <- data.frame(skjema45O[2])
# -

# ## Skjema 45P

# +
skjema45P <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45P",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45P",]$SKJEMA,
                             skjema_cols = T, 
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema45P_skjema <- data.frame(skjema45P[1])
skjema45P_dubletter <- data.frame(skjema45P[2])
# -

# ## Skjema 46O

# +
skjema46O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "46O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "46O",]$SKJEMA,
                             skjema_cols = T, 
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema46O_skjema <- data.frame(skjema46O[1])
skjema46O_dubletter <- data.frame(skjema46O[2])
