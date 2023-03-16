# -*- coding: utf-8 -*-
# # Skjemadata til Samdata

aargang <- 2022

# +
options(repr.matrix.max.rows=600, repr.matrix.max.cols=2000)

# Gjenoppretter virtuelt miljø
renv::restore()

suppressPackageStartupMessages({
  library(tidyverse)
  library(fellesr)
  library(klassR)
})

# OBS: erstatt med fellesr når den er installert på nytt!
# source("/ssb/bruker/rdn/fellesr/R/dynarev_uttrekk.R")
# -

# ### Lager årgangsmappe

# +
arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/samdata/"
aargangsmappe <- paste0(arbeidsmappe, aargang, "/")

if (file.exists(aargangsmappe)==FALSE) {
  dir.create(aargangsmappe)
}
# -

datomarkering <- toupper(format(Sys.Date(), "%d%b%y"))
datomarkering

skjema38O_filsti <- paste0(aargangsmappe, "Skj38o_", datomarkering, ".txt")
skjema38P_filsti <- paste0(aargangsmappe, "Skj38p_", datomarkering, ".txt")
skjema44O_filsti <- paste0(aargangsmappe, "Skj44o_", datomarkering, ".txt")
skjema44P_filsti <- paste0(aargangsmappe, "Skj44p_", datomarkering, ".txt")
skjema45O_filsti <- paste0(aargangsmappe, "Skj45o_", datomarkering, ".txt")
skjema45P_filsti <- paste0(aargangsmappe, "Skj45p_", datomarkering, ".txt")
skjema46O_filsti <- paste0(aargangsmappe, "Skj46o_", datomarkering, ".txt")
skjema46P_filsti <- paste0(aargangsmappe, "Skj46p_", datomarkering, ".txt")
skjema39_filsti <- paste0(aargangsmappe, "Skj39_", datomarkering, ".txt")
skjema0X_filsti <- paste0(aargangsmappe, "Skj0X_", datomarkering, ".txt")


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

tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38O",]$DELREG

# ## Laster inn variabelliste per skjema
source(here::here("Samdata", "Variabelliste per skjema.R"))

# ## Skjema 38O

# +
skjema38O <- fellesr::dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38O",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema38O_skjema <- data.frame(skjema38O[1]) %>%
  dplyr::select(all_of(variabelliste_38O))

skjema38O_dubletter <- data.frame(skjema38O[2])


# +
# colnames(skjema38O_skjema)
# -

# ### Lagrer filen
if (nrow(skjema38O_dubletter)==0){
write.table(skjema38O_skjema, skjema38O_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 38P

# +
skjema38P <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38P",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38P",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema38P_skjema <- data.frame(skjema38P[1]) %>%
  dplyr::select(all_of(variabelliste_38P))

skjema38P_dubletter <- data.frame(skjema38P[2])


# +
# colnames(skjema38P_skjema)
# -

# ### Lagrer filen
if (nrow(skjema38P_dubletter)==0){
write.table(skjema38P_skjema, skjema38P_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}


# ## Skjema 44O

# +
skjema44O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44O",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema44O_skjema <- data.frame(skjema44O[1]) %>%
  dplyr::select(all_of(variabelliste_44O))

skjema44O_dubletter <- data.frame(skjema44O[2])

# +
# colnames(skjema44O_skjema)
# -

# ### Lagrer filen
if (nrow(skjema44O_dubletter)==0){
write.table(skjema44O_skjema, skjema44O_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 44P

# +
skjema44P <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44P",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "44P",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema44P_skjema <- data.frame(skjema44P[1]) %>%
  dplyr::select(all_of(variabelliste_44P))

skjema44P_dubletter <- data.frame(skjema44P[2])


# +
# colnames(skjema44P_skjema)
# -

# ### Lagrer filen
if (nrow(skjema44P_dubletter)==0){
write.table(skjema44P_skjema, skjema44P_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 45O

# +
skjema45O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45O",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema45O_skjema <- data.frame(skjema45O[1]) %>%
  dplyr::select(all_of(variabelliste_45O))

skjema45O_dubletter <- data.frame(skjema45O[2])

# +
# colnames(skjema45O_skjema)
# -

# ### Lagrer filen
if (nrow(skjema45O_dubletter)==0){
write.table(skjema45O_skjema, skjema45O_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 45P

# +
skjema45P <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45P",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "45P",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema45P_skjema <- data.frame(skjema45P[1]) %>%
  dplyr::select(all_of(variabelliste_45P))

skjema45P_dubletter <- data.frame(skjema45P[2])

# +
# colnames(skjema45P_skjema)
# -

# ### Lagrer filen
if (nrow(skjema45P_dubletter)==0){
write.table(skjema45P_skjema, skjema45P_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 46O

# +
skjema46O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "46O",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "46O",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema46O_skjema <- data.frame(skjema46O[1]) %>%
  dplyr::select(all_of(variabelliste_46O))

skjema46O_dubletter <- data.frame(skjema46O[2])

# +
# colnames(skjema46O_skjema)
# -

# ### Lagrer filen
if (nrow(skjema46O_dubletter)==0){
write.table(skjema46O_skjema, skjema46O_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 46P

# +
skjema46P <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "46P",]$DELREG,
                             skjema = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "46P",]$SKJEMA,
                             skjema_cols = T,
                             dublettsjekk = T,
                             con_ask = FALSE)

skjema46P_skjema <- data.frame(skjema46P[1]) %>%
  dplyr::select(all_of(variabelliste_46P))

skjema46P_dubletter <- data.frame(skjema46P[2])

# -

# ### Lagrer filen
if (nrow(skjema46P_dubletter)==0){
write.table(skjema46P_skjema, skjema46P_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 39

# +
skjema39 <- dynarev_uttrekk(delregnr = paste0(24, substr(aargang, 3, 4)),
                            skjema = "HELSE39",
                            skjema_cols = T,
                            dublettsjekk = T,
                            con_ask = FALSE)

skjema39_skjema <- data.frame(skjema39[1]) %>%
  dplyr::select(all_of(variabelliste_39))

skjema39_dubletter <- data.frame(skjema39[2])
# -

# ### Lagrer filen
if (nrow(skjema39_dubletter)==0){
write.table(skjema39_skjema, skjema39_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 0X

# +
skjema0X <- dynarev_uttrekk(delregnr = paste0(24, substr(aargang, 3, 4)),
                            skjema = "HELSE0X",
                            skjema_cols = T,
                            dublettsjekk = c("FORETAKSNR", "ART_SEKTOR", "FUNKSJON_KAPITTEL"),
                            con_ask = FALSE)

skjema0X_skjema <- data.frame(skjema0X[1]) %>%
  dplyr::select(all_of(variabelliste_0X))

colnames(skjema0X_skjema)

skjema0X_dubletter <- data.frame(skjema0X[2])
# -

# ### Lagrer filen
if (nrow(skjema0X_dubletter)==0){
write.table(skjema0X_skjema, skjema0X_filsti,
            sep = "¤",
            fileEncoding = "UTF-8",
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}
