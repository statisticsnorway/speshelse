# -*- coding: utf-8 -*-
# # Endringslogg (Samdata)

# ### Hvordan kjøre scriptet

# 1. Velg årgang og skjema i prompt
# 2. Rapport i Excel-format lagres i mappen:
# X:/330/Speshelse/1. Avklare behov/Samarbeid_eksternt/SINTEF/20XX/Endringslogg

# ### Laster inn pakkene som brukes i scriptet

suppressPackageStartupMessages({
library(arsenal)
library(dplyr)
library(openxlsx)
})


# ### Angir aargang, skjema og dato til filene (i prompt, default angir kun standardverdi)

aargang <- as.numeric(readline("Skriv inn årgang:"))
skjema <- readline("Skriv inn skjema:") # F.eks. 44o


# ### Lager en mappe for output

arbeidsmappe <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/samdata/", aargang, "/")
arbeidsmappe

# +
endringslogg_mappe <- paste0(arbeidsmappe, "/Endringslogg")

if (file.exists(endringslogg_mappe)==FALSE) {
  dir.create(endringslogg_mappe)
}
# -


# ## Laster inn filene
#
# OBS: erstatt filsti!

# Funksjoner for riktig aargang og skjema #
laste_mappe <- function(aargang) {
  infile <- paste0("X:/330/Speshelse/1. Avklare behov/Samarbeid_eksternt/SINTEF/", aargang, "/Data/")
  return(infile)
}
laste_skjema <- function(skjema) {
  infile <- paste0("Skj", skjema, "_")
  return(infile)
}

# Liste (med fildetaljer) over alle versjonene av skjema XX (laste_skjema) som ligger i mappen:
# X:/330/Speshelse/1. Avklare behov/Samarbeid_eksternt/SINTEF/20XX/Data (laste_mappe) #
filliste <- file.info(list.files(laste_mappe(aargang), pattern = laste_skjema(skjema), full.names = T))
filliste <- filliste[(filliste$size > 0),] # fjerner ev. filer som er blanke (size = 0)

# Henter ut den nyeste og nest nyeste versjonen fra listen (mtime = modified timestamp) #
nyeste_versjon <- rownames(filliste)[which.max(filliste$mtime)]
nyeste_versjon # sjekk i konsollen at riktig fil har blitt angitt

nx <- length(filliste$mtime) # lengden til mtime
nest_nyeste_versjon <- rownames(filliste[filliste$mtime == sort(filliste$mtime)[nx-1],]) # henter ut den nest nyeste filen
nest_nyeste_versjon # sjekk i konsollen at riktig fil har blitt angitt. "character(0)" betyr at filen ikke finnes

# Stopper scriptet dersom nyeste fil ikke finnes #
if(identical(nyeste_versjon, character(0)) == TRUE) {
  stop(showDialog("Script stoppet:", paste("Nyeste versjon av skjema", skjema, "finnes ikke", sep = " ")))}
# Stopper scriptet dersom nest nyeste fil ikke finnes #
if(identical(nest_nyeste_versjon, character(0)) == TRUE) {
  stop(showDialog("Script stoppet:", paste("Nest nyeste versjon av skjema", skjema, "finnes ikke", sep = " ")))}


# +
# Laster inn filene ved hjelp av objektene nyeste_versjon og nest_nyeste_versjon #
ny_fil <- read.csv(nyeste_versjon,
                   sep = "¤",
                   na.strings=c(".", "", " "),
                   fill = TRUE,
                   header = TRUE,
                   quote = "",
                   check.names = FALSE)

gammel_fil <- read.csv(nest_nyeste_versjon,
                       sep = "¤",
                       na.strings=c(".", "", " "),
                       fill = TRUE,
                       header = TRUE,
                       quote = "",
                       check.names = FALSE)
# -

# Legger til kolonnenavn (= "Mangler kolonnenavn") dersom det mangler #
colnames(ny_fil) [colnames(ny_fil) == ""] <- "Mangler kolonnenavn"
colnames(gammel_fil) [colnames(gammel_fil) == ""] <- "Mangler kolonnenavn"

# Endrer alle variablene til character #
ny_fil <- ny_fil %>%
  mutate_all(as.character)

gammel_fil <- gammel_fil %>%
  mutate_all(as.character)

### Lager oversikt over FINST/FORETAKETS-info (til rapportene) ###
# Info-variabler som skal med for filer med FINST_ORGNR som enhet
finst_info <- c("FINST_ORGNR", "FINST_NAVN", "HELSEREGION_NAVN")

# Skjemaer med FORETAKSORGNR som enhet:
skjemaliste <- c("38o", "44o", "45o", "46o")
# Info-variabler som skal med for filer med FORETAKETS_ORGNR som enhet
foretak_info <- c("FORETAKETS_ORGNR", "FORETAKETS_NAVN", "HELSEREGION_NAVN")

if (skjema %in% skjemaliste) {
  ny_fil_foretak_info <- ny_fil[,c(foretak_info)]
  gammel_fil_foretak_info <- gammel_fil[,c(foretak_info)]
  foretak_info <- rbind(ny_fil_foretak_info, gammel_fil_foretak_info)
  foretak_info <- foretak_info[!duplicated(foretak_info), ]
} else {
  ny_fil_finst_info <- ny_fil[,c(finst_info)]
  gammel_fil_finst_info <- gammel_fil[,c(finst_info)]
  finst_info <- rbind(ny_fil_finst_info, gammel_fil_finst_info)
  finst_info <- finst_info[!duplicated(finst_info), ]
}


# ## Sammenligner ny_fil og gammel_fil (etter FINST_ORGNR)

if (skjema %in% skjemaliste) {
  endringslogg <- summary(comparedf(ny_fil, gammel_fil, by = "FORETAKETS_ORGNR"))
} else {
  endringslogg <- summary(comparedf(ny_fil, gammel_fil, by = "FINST_ORGNR"))
}

# ## Antall enheter og variabler per fil

summary <- data.frame(endringslogg["frame.summary.table"]) # Summary of data.frames

summary["frame.summary.table.version"] <- NULL

# Endrer kolonnenavn #
summary <- summary %>%
  rename(
    Fil = frame.summary.table.arg,
    'Antall variabler' = frame.summary.table.ncol,
    'Antall enheter' = frame.summary.table.nrow)

# Erstatter ny_fil/gammel_fil med filnavn #
summary$Fil <- ifelse(summary$Fil == "ny_fil", paste0("Nyeste versjon (", gsub(".*/", "", nyeste_versjon), ")"),
                      ifelse(summary$Fil == "gammel_fil", paste0("Nest nyeste versjon (", gsub(".*/", "", nest_nyeste_versjon), ")"), summary$Fil))

# Funksjon for å angi navnet på Excel-filen som eksporteres (endringslogg for valgt skjema + dagens dato) #
filnavn_eksport <- function(skjema) {
  infile <- paste0("Endringslogg Skj", skjema, "_", format(Sys.Date(),"%d%m%y"), ".xlsx")
  return(infile)
}


# Lager workbook #
wb <- openxlsx::createWorkbook("Endringslogg")

# Lagrer filen (i mappen opprettet i starten av scriptet) #
# Dersom filen allerede finnes stoppes scriptet #
if (file.exists(paste0(sti, "/", filnavn_eksport(skjema), sep = ""))==FALSE){
    
    write.xlsx(summary, file = filnavn_eksport(skjema), sheetName = "Antall enheter og variabler", row.names = FALSE, showNA=FALSE)
    
      # Legger til fane for Antall enheter og variabler #
  openxlsx::addWorksheet(wb, "Antall enheter og variabler")
  openxlsx::writeData(wb,"Antall enheter og variabler", summary)
    
    } else {
  stop(showDialog("Filen finnes allerede", paste("Filen", filnavn_eksport(skjema), "finnes allerede", sep = " ")))
  }

# ### Hvilke variabler finnes i en, men ikke i begge filene

variabler_mangler <- data.frame(endringslogg["vars.ns.table"]) # Variables not shared

# Rydder i filen #
variabler_mangler["vars.ns.table.position"] <- NULL
variabler_mangler["vars.ns.table.class"] <- NULL

# Endrer kolonnenavn #
variabler_mangler <- variabler_mangler %>%
  rename(
    'Finnes i fil' = vars.ns.table.version,
    Variabel = vars.ns.table.variable)

# Lager variabelen "Mangler i fil " og erstatter x/y med filnavn #
variabler_mangler$'Mangler i fil' <- ifelse(variabler_mangler$'Finnes i fil' == "x", paste0("Nest nyeste versjon (", gsub(".*/", "", nest_nyeste_versjon), ")"),
                                            ifelse(variabler_mangler$'Finnes i fil' == "y", paste0("Nyeste versjon (", gsub(".*/", "", nyeste_versjon), ")"),
                                                                                                   ""))

variabler_mangler$'Finnes i fil' <- ifelse(variabler_mangler$'Finnes i fil' == "x", paste0("Nyeste versjon (", gsub(".*/", "", nyeste_versjon), ")"),
                                           ifelse(variabler_mangler$'Finnes i fil' == "y", paste0("Nest nyeste versjon (", gsub(".*/", "", nest_nyeste_versjon), ")"),
                                                                                                  ""))
# Endrer rekkefølgen på variablene #
variabler_mangler <- variabler_mangler[,c("Variabel", "Finnes i fil", "Mangler i fil")]

# Lagrer filen dersom det er noen variabler som mangler i en av filene #
if (nrow(variabler_mangler) > 0) {
  # write.xlsx(variabler_mangler, file = filnavn_eksport(skjema), sheetName = "Variabler mangler", append=TRUE, row.names = FALSE, showNA=FALSE)

  # Legger til fane for Antall enheter og variabler #
  openxlsx::addWorksheet(wb, "Variabler mangler")
  openxlsx::writeData(wb,"Variabler mangler", variabler_mangler)
    
    } else {
  showDialog("Hvilke variabler finnes i en, men ikke i begge filene", "Ingen variabler mangler")
}


# ## Hvilke enheter (FINST_ORGNR/FORETAKETS_ORGNR) som ikke finnes i begge filene

enheter_mangler <- data.frame(endringslogg["obs.table"]) # Observations not shared

enheter_mangler["obs.table.observation"] <- NULL

if (skjema %in% skjemaliste) {
  enheter_mangler <- enheter_mangler %>%
    rename(
      'Finnes i fil' = obs.table.version,
      FORETAKETS_ORGNR = obs.table.FORETAKETS_ORGNR)
} else {
  enheter_mangler <- enheter_mangler %>%
    rename(
      'Finnes i fil' = obs.table.version,
      FINST_ORGNR = obs.table.FINST_ORGNR)
}

enheter_mangler$'Mangler i fil' <- ifelse(enheter_mangler$'Finnes i fil' == "x", paste0("Nest nyeste versjon (", gsub(".*/", "", nest_nyeste_versjon), ")"),
                                          ifelse(enheter_mangler$'Finnes i fil' == "y", paste0("Nyeste versjon (", gsub(".*/", "", nyeste_versjon), ")"), ""))

enheter_mangler$'Finnes i fil' <- ifelse(enheter_mangler$'Finnes i fil' == "x", paste0("Nyeste versjon (", gsub(".*/", "", nyeste_versjon), ")"),
                                         ifelse(enheter_mangler$'Finnes i fil' == "y", paste0("Nest nyeste versjon (", gsub(".*/", "", nest_nyeste_versjon), ")"), ""))

# Legger til FINST/FORETAKETS-info (left join) #
if (skjema %in% skjemaliste) {
  enheter_mangler <- Reduce(function(x, y) left_join(x, y, by = c("FORETAKETS_ORGNR"),
                                                     all = TRUE), list(
                                                       enheter_mangler,
                                                       foretak_info))
  enheter_mangler <- enheter_mangler[,c("FORETAKETS_ORGNR", "FORETAKETS_NAVN", "HELSEREGION_NAVN", "Finnes i fil", "Mangler i fil")]
                            
                            } else {
  enheter_mangler <- Reduce(function(x, y) left_join(x, y, by = c("FINST_ORGNR"),
                                                     all = TRUE), list(
                                                       enheter_mangler,
                                                       finst_info))
  enheter_mangler <- enheter_mangler[,c("FINST_ORGNR", "FINST_NAVN", "HELSEREGION_NAVN", "Finnes i fil", "Mangler i fil")]
                            
                            }
                            
                            # Lagrer filen dersom det er noen enheter som mangler i en av filene #
if (nrow(enheter_mangler) > 0) {
 #  write.xlsx(enheter_mangler, file = filnavn_eksport(skjema), sheetName = "Forskjellige enheter", append = TRUE, row.names = FALSE, showNA = FALSE)

  # Legger til fane for Antall enheter og variabler #
  openxlsx::addWorksheet(wb, "Forskjellige enheter")
  openxlsx::writeData(wb,"Forskjellige enheter", enheter_mangler)
    
    } else {
  showDialog("Hvilke enheter (FINST_ORGNR/FORETAKETS_ORGNR) finnes ikke i begge filene", "Ingen enheter mangler")
}

# ### Antall forskjellige observasjoner per variabel

forskjeller_per_variabel <- data.frame(endringslogg["diffs.byvar.table"]) # Differences detected by variable

forskjeller_per_variabel["diffs.byvar.table.var.y"] <- NULL

forskjeller_per_variabel <- forskjeller_per_variabel %>%
  rename(
    Variabel = diffs.byvar.table.var.x,
    'Antall forskjeller' = diffs.byvar.table.n,
    'Antall forskjeller som skyldes missing' = diffs.byvar.table.NAs)

# Beholder kun variabler med forskjeller #
forskjeller_per_variabel <- forskjeller_per_variabel[(forskjeller_per_variabel$'Antall forskjeller' > 0), ]

# Sorterer etter antall forskjeller #
forskjeller_per_variabel <- forskjeller_per_variabel[order(forskjeller_per_variabel$'Antall forskjeller',
                                                           decreasing = T),]

if (nrow(forskjeller_per_variabel) > 0) {
  # write.xlsx(forskjeller_per_variabel, file = filnavn_eksport(skjema), sheetName = "Forskjeller per variabel", append=TRUE, row.names = FALSE, showNA=FALSE)

  # Legger til fane for Antall enheter og variabler #
  openxlsx::addWorksheet(wb, "Forskjeller per variabel")
  openxlsx::writeData(wb,"Forskjeller per variabel", forskjeller_per_variabel)
    
    } else {
  showDialog("Antall forskjellige observasjoner per variabel", "Ingen forskjeller mellom noen av variablene")
}

# ### Hvilke observasjoner for (FINST_ORGNR/FORETAKETS_ORGNR) som er ulike i begge filene

ulike_observasjoner <- data.frame(endringslogg["diffs.table"])

# +
if (nrow(ulike_observasjoner) > 0) {
# Rydder i filen #
ulike_observasjoner["diffs.table.var.y"] <- NULL
ulike_observasjoner["diffs.table.row.x"] <- NULL
ulike_observasjoner["diffs.table.row.y"] <- NULL
    
ulike_observasjoner$teller_forskjeller <- 1

    # Legger til FINST-info (left join) #
if (skjema %in% skjemaliste) {
  ulike_observasjoner <- ulike_observasjoner %>%
    rename(
      FORETAKETS_ORGNR = diffs.table.FORETAKETS_ORGNR,
      Variabel = diffs.table.var.x,
      'Verdi nyeste versjon' = diffs.table.values.x,
      'Verdi nest nyeste versjon' =  diffs.table.values.y)
  antall_ulike_observasjoner_per_enhet <- ulike_observasjoner %>%
    group_by(FORETAKETS_ORGNR) %>%
    summarise('Antall variabler med forskjellige verdier (per enhet)' = sum(teller_forskjeller))
  ulike_observasjoner <- Reduce(function(x, y) left_join(x, y, by = c("FORETAKETS_ORGNR"),
                                                         all = TRUE), list(
                                                           ulike_observasjoner,
                                                           antall_ulike_observasjoner_per_enhet,
                                                           foretak_info))
  ulike_observasjoner <- ulike_observasjoner[,c("FORETAKETS_ORGNR", "FORETAKETS_NAVN", "HELSEREGION_NAVN", "Variabel", "Verdi nyeste versjon", "Verdi nest nyeste versjon", "Antall variabler med forskjellige verdier (per enhet)")]
  ulike_observasjoner <- ulike_observasjoner[order(ulike_observasjoner$FORETAKETS_NAVN, decreasing = F),]
} else {
  ulike_observasjoner <- ulike_observasjoner %>%
    rename(
      FINST_ORGNR = diffs.table.FINST_ORGNR,
      Variabel = diffs.table.var.x,
      'Verdi nyeste versjon' = diffs.table.values.x,
      'Verdi nest nyeste versjon' =  diffs.table.values.y)
  antall_ulike_observasjoner_per_enhet <- ulike_observasjoner %>%
    group_by(FINST_ORGNR) %>%
    summarise('Antall variabler med forskjellige verdier (per enhet)' = sum(teller_forskjeller))
  ulike_observasjoner["teller_forskjeller"] <- NULL
  ulike_observasjoner <- Reduce(function(x, y) left_join(x, y, by = c("FINST_ORGNR"),
                                                         all = TRUE), list(
                                                           ulike_observasjoner,
                                                           antall_ulike_observasjoner_per_enhet,
                                                           finst_info))
  ulike_observasjoner <- ulike_observasjoner[,c("FINST_ORGNR", "FINST_NAVN", "HELSEREGION_NAVN", "Variabel", "Verdi nyeste versjon", "Verdi nest nyeste versjon", "Antall variabler med forskjellige verdier (per enhet)")]
  ulike_observasjoner <- ulike_observasjoner[order(ulike_observasjoner$FINST_NAVN, decreasing = F),]
  }
                                
                                colnames(ulike_observasjoner)
# Legger til filnavn i kolonnenavnene #
colnames(ulike_observasjoner) [colnames(ulike_observasjoner) == "Verdi nyeste versjon"] <- paste0("Verdi nyeste versjon (", gsub(".*/", "", nyeste_versjon), ")")
                                
                                
                                if (nrow(filliste) > 1) {
  colnames(ulike_observasjoner) [colnames(ulike_observasjoner) == "Verdi nest nyeste versjon"] <- paste0("Verdi nest nyeste versjon (", gsub(".*/", "", nest_nyeste_versjon), ")")
} else {
  colnames(ulike_observasjoner) [colnames(ulike_observasjoner) == "Verdi nest nyeste versjon"] <- paste0("Verdi nest nyeste versjon (", gsub(".*/", "", nyeste_versjon_fra_forrige_mappe), ")")
}
}
    
                                ulike_observasjoner[] <- lapply(ulike_observasjoner, function(x) if(class(x) == 'AsIs') unlist(x) else x)
                                    
                                    # Lagrer filen dersom det er noen observasjoner som er ulike i filene #
if (nrow(ulike_observasjoner) > 0) {
  # write.xlsx(ulike_observasjoner, file = filnavn_eksport(skjema), sheetName = "Ulike observasjoner", append=TRUE, row.names = FALSE, showNA=FALSE)

  # Legger til fane for Antall enheter og variabler #
  openxlsx::addWorksheet(wb, "Ulike observasjoner")
  openxlsx::writeData(wb,"Ulike observasjoner", ulike_observasjoner)
    
    } else {
  showDialog("Hvilke observasjoner for (FINST_ORGNR/FORETAKETS_ORGNR) som er ulike i begge filene", "Alle variablene har identiske verdier i begge filene")
}
                                    
                                    # Lagrer filen #
openxlsx::saveWorkbook(wb, filnavn_eksport(skjema), overwrite = TRUE)
# -

# ### Rydder i global environment ###

rm(list = ls())
