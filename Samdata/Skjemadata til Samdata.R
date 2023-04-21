# -*- coding: utf-8 -*-
# # Skjemadata til Samdata

# ### Velger årgang

aargang <- 2022

# +
options(repr.matrix.max.rows=600, repr.matrix.max.cols=2000)

# Gjenoppretter virtuelt miljø
# renv::restore()

suppressPackageStartupMessages({
  library(tidyverse)
  # library(fellesr)
  library(klassR)
})

# OBS: erstatt med fellesr når den er installert på nytt!
source("/ssb/bruker/rdn/fellesr/R/dynarev_uttrekk.R")

# encoding <- "UTF-8"
encoding <- "latin1"
# -

# ### Lager årgangsmappe
#
# Skjemaene lagres i mappen `aargangsmappe` (se under). Disse må flyttes over fra Linux til X-disken for å kunne sendes via filslusen. 

# +
arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/samdata/"
aargangsmappe <- paste0(arbeidsmappe, aargang, "/")

if (file.exists(aargangsmappe)==FALSE) {
  dir.create(aargangsmappe)
}

aargangsmappe
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
skjema38O <- dynarev_uttrekk(delregnr = tjenesteomrader[tjenesteomrader$SKJEMA_NAVN == "38O",]$DELREG,
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
            fileEncoding = encoding,
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
            fileEncoding = encoding,
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
            fileEncoding = encoding,
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
            fileEncoding = encoding,
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
            fileEncoding = encoding,
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
            fileEncoding = encoding,
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
            fileEncoding = encoding,
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
            fileEncoding = encoding,
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
                            skjema_cols = TRUE,
                            dublettsjekk = TRUE,
                            con_ask = FALSE, 
                            raadata = TRUE)

skjema39_skjema <- data.frame(skjema39[1]) %>%
  dplyr::select(all_of(variabelliste_39))

skjema39_dubletter <- data.frame(skjema39[2])
# -

# ### Lagrer filen
if (nrow(skjema39_dubletter)==0){
write.table(skjema39_skjema, skjema39_filsti,
            sep = "¤",
            fileEncoding = encoding,
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}

# ## Skjema 0X
#
# OBS: fjernet kolonnen F551 for 2022. Kan også INST_NR og INST_NAVN slettes?

# Laster inn artskontokoder fra KLASS
artskontokoder <- klassR::GetKlass(606, output_level = 3, date = c(paste0(aargang, "-01-01"))) %>%
  dplyr::rename(ART_SEKTOR = code) %>%
  dplyr::distinct(ART_SEKTOR)

# +
# Dublettsjekk
skjema0X_dubletter <- dynarev_uttrekk(delregnr = paste0(24, substr(aargang, 3, 4)),
                            skjema = "HELSE0X",
                            skjema_cols = TRUE,
                            sfu_cols = c("NAVN"),
                            skjema_sfu_merge = TRUE,
                            dublettsjekk = c("ENHETS_ID", "FORETAKSNR", "ART_SEKTOR", "FUNKSJON_KAPITTEL"),
                            raadata = TRUE, # OBS: Samdata skal ha rådata, men reviderte data ligger ikke i Oracle
                            con_ask = FALSE)

skjema0X_dubletter <- data.frame(skjema0X_dubletter[2])
# -

skjema0X_skjema <- dynarev_uttrekk(delregnr = paste0(24, substr(aargang, 3, 4)),
                            skjema = "HELSE0X",
                            skjema_cols = TRUE,
                            sfu_cols = c("NAVN"),
                            skjema_sfu_merge = TRUE,
                            # dublettsjekk = c("ENHETS_ID", "FORETAKSNR", "ART_SEKTOR", "FUNKSJON_KAPITTEL"),
                            con_ask = FALSE)

unique(skjema0X_skjema$REGION)

# +
skjema0X_skjema_2021 <- dynarev_uttrekk(delregnr = paste0(24, substr(2019, 3, 4)),
                            skjema = "HELSE0X",
                            skjema_cols = TRUE,
                            sfu_cols = c("NAVN"),
                            skjema_sfu_merge = TRUE,
                            # dublettsjekk = c("ENHETS_ID", "FORETAKSNR", "ART_SEKTOR", "FUNKSJON_KAPITTEL"),
                            con_ask = FALSE)

unique(skjema0X_skjema_2021$REGION)
# -

skjema0X_skjema_2 <- dplyr::full_join(skjema0X_skjema, artskontokoder, by = "ART_SEKTOR")

skjema0X_data_long <- skjema0X_skjema_2 %>%
select(AARGANG, VERDATO, FORETAKSNR, NAVN, AARGANG, REGION, ART_SEKTOR, FUNKSJON_KAPITTEL, BELOP) %>%
dplyr::rename(FINST_NAVN = NAVN, 
             FUNK_NR = FUNKSJON_KAPITTEL) %>%
dplyr::mutate(INST_NAVN = FINST_NAVN, 
             INST_NR = FORETAKSNR, 
             ART_SEKTOR = paste0("F", ART_SEKTOR), 
             REGION_NR = substr(REGION, 1, 2)) %>%
# tidyr::spread(FORETAKSNR, FINST_NAVN, REGION_NR, FUNKSJON_KAPITTEL, ART_SEKTOR) # kolonnene som skal splittes opp noteres her. OBS: duplikater er ikke tillatt
tidyr::pivot_wider(id_cols = c("AARGANG", "VERDATO", "FORETAKSNR", "FINST_NAVN", "INST_NAVN", "INST_NR", "REGION_NR", "FUNK_NR"), 
                   names_from = ART_SEKTOR,
                   values_from = BELOP) %>%
filter(!is.na(FORETAKSNR)) %>%
dplyr::select(all_of(variabelliste_0X))

# Erstatter missing med 0
skjema0X_data_long[is.na(skjema0X_data_long)] <- 0

# ### Lagrer filen
if (nrow(skjema0X_dubletter)==0){
write.table(skjema0X_data_long, skjema0X_filsti,
            sep = "¤",
            fileEncoding = encoding,
            # col.names = FALSE,
            row.names = FALSE,
            na = "",
            dec = ",",
            quote = F)
}
