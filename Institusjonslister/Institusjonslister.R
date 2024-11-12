# -*- coding: utf-8 -*-
# # Institusjonslister

# Beskrivelse av scriptet... Sendes til Hdir i forbindelse med Samarbeids-/evalueringsmøte?
#
# Det lages én fil for offentlige institusjoner og én fil for private institusjoner i spesialisthelsetjenesten. Filene lagres med datomarkering i mappen **filsti_institusjonslister** (se under)

suppressPackageStartupMessages({
  library(rio)
  library(dplyr)
  library(stringr)
  library(fellesr)
})

# ## Angir årgang og kobler til Oracle

aargang <- 2024

# Logg på for å få tilgang til Oracle 
con <- fellesr::dynarev_uttrekk(con_ask = "con") # fellesr::

# ## Filsti for institusjonslister
#
# Oppretter årgangsmappe dersom den ikke eksisterer.

# +
arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/institusjonslister/"

filsti_institusjonslister <- paste0(arbeidsmappe, aargang, "/")

filsti_institusjonslister
# -

if (file.exists(filsti_institusjonslister)==FALSE) {
  dir.create(filsti_institusjonslister)
}

# ## Laster inn oversikt over alle rapporteringsenheter (bortsett fra private uten oppdragsdokument)

HF <- klassR::GetKlass(603, output_style = "wide") %>%
  dplyr::rename(ORGNR_FORETAK = code3, 
                NAVN = name3, 
                RHF = name2, 
                Helseregion = code1) %>%
  dplyr::mutate(Foretakstype = "HF") %>%
  dplyr::select(ORGNR_FORETAK, NAVN, RHF, Helseregion, Foretakstype)

RHF <- klassR::GetKlass(603, output_level = 2) %>%
  dplyr::rename(ORGNR_FORETAK = code, 
                NAVN = name, 
                Helseregion = parentCode) %>%
  dplyr::mutate(Foretakstype = "RHF", 
                RHF = NAVN) %>%
  dplyr::select(ORGNR_FORETAK, NAVN, RHF, Helseregion, Foretakstype)

# +
RHF_region <- RHF %>%
select(Helseregion, RHF)

stotteforetak <- klassR::GetKlass(605, date = c(paste0(aargang, "-01-01"))) %>%
  dplyr::filter(!is.na(parentCode), nchar(parentCode) <= 2) %>%
  dplyr::rename(ORGNR_FORETAK = code,
                NAVN = name,
                Helseregion = parentCode) %>%
  dplyr::mutate(Foretakstype = "Støtteforetak") %>% # OBS: legg til orgnummer?
  dplyr::left_join(RHF_region, by = "Helseregion") %>%
  dplyr::mutate(RHF = case_when(is.na(RHF) ~ "FELLESEIDE STØTTEFORETAK", TRUE ~ RHF)) %>%
  dplyr::filter(!grepl("RHF", NAVN)) %>%
  dplyr::select(ORGNR_FORETAK, NAVN, RHF, Helseregion, Foretakstype)

# # HELSE MIDT-NORGE RHF HELSEPLATTFORMEN
# stotteforetak_2 <- klassR::GetKlass(605, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
#   dplyr::rename(ORGNR_FORETAK = code3, 
#                 NAVN = name3, 
#                 RHF = name2, 
#                 Helseregion = code1) %>%
#   dplyr::mutate(Foretakstype = "Støtteforetak") %>%
#   dplyr::select(ORGNR_FORETAK, NAVN, RHF, Helseregion, Foretakstype)

# stotteforetak <- rbind(stotteforetak_1, stotteforetak_2)
# stotteforetak
# -

offentlig <- rbind(HF, RHF, stotteforetak)

oppdrag <- klassR::GetKlass(604, output_style = "wide") %>%
  dplyr::rename(ORGNR_FORETAK = code2, 
                NAVN = name2, 
                RHF = name1, 
                Helseregion = code1) %>%
  dplyr::mutate(Foretakstype = "Oppdrag", 
                RHF = paste0(RHF, " RHF")) %>%
  dplyr::select(ORGNR_FORETAK, NAVN, RHF, Helseregion, Foretakstype)

helsereg <- rbind(HF, RHF, stotteforetak, oppdrag) %>%
  dplyr::group_by(Helseregion, RHF) %>%
  dplyr::tally()

helsereg

# ## Laster inn delregisteret

# OBS: hent delregnr fra KLASS 610?

delreg <- fellesr::dynarev_uttrekk(delregnr = c(paste0(24, substr(aargang, 3, 4)), paste0(19377, substr(aargang, 3, 4))),
                                   skjema = T, 
                                   skjema_cols = F,
                                   enhets_type = c("FRTK", "BEDR"), 
                                   sfu_cols = T, 
                                   con_ask = F)

delreg %>%
filter(ORGNR == "918098275")

delreg <- delreg %>%
  dplyr::filter(is.na(KVITT_TYPE)) %>%
  dplyr::filter(!is.na(ORGNR)) %>%
  dplyr::mutate(NAVN1 = as.character(NAVN1), 
                NAVN2 = as.character(NAVN2),
                NAVN3 = as.character(NAVN3), 
                NAVN1 = tidyr::replace_na(NAVN1, ""), 
                NAVN2 = tidyr::replace_na(NAVN2, ""), 
                NAVN3 = tidyr::replace_na(NAVN3, ""))

# Lager institusjonsnavn #
delreg$NYTT_NAVN <- paste0(delreg$NAVN1, " ", 
                           delreg$NAVN2, " ", 
                           delreg$NAVN3) 

# Fjerner mellomrom på slutten av strengen #
delreg$NYTT_NAVN <- stringr::str_trim(delreg$NYTT_NAVN, side = c("right"))
# Og inne i strengen #
delreg$NYTT_NAVN <- stringr::str_squish(delreg$NYTT_NAVN)

delreg_tester <- delreg %>%
  dplyr::select(SN07_1, H_VAR1_A, ORGNR, ORGNR_FORETAK, NYTT_NAVN, NAVN, NAVN1, NAVN2, NAVN3, NAVN4, NAVN5) %>%
  dplyr::filter(!is.na(H_VAR1_A)) %>%
  dplyr::mutate(test = stringr::str_length(SN07_1), 
                sn07_1_ny <- stringr::str_pad(SN07_1, width = 6, "right", pad = "0"))

# ## Legger til Standard for næringsgruppering (SN) fra KLASS

klass_sn <- klassR::GetKlass(6, output_level = 5) %>%
  dplyr::select(-parentCode, -level) %>%
  dplyr::rename(SN07_1 = code, 
                SN07_1_navn = name) %>%
  dplyr::mutate(SN07_1 = as.character(SN07_1))

delreg  <- dplyr::left_join(delreg, klass_sn, by = "SN07_1")

# ## Offentlige RHF, HF og hjelpeforetak

# ### Beholder HF, RHF og hjelpeforetak

offentlig <- offentlig %>%
  dplyr::select(ORGNR_FORETAK, Foretakstype, Helseregion, RHF) # NAVN

delreg$ORGNR_FORETAK <- as.character(delreg$ORGNR_FORETAK)
delreg_offentlig <- dplyr::inner_join(offentlig, delreg, by = c("ORGNR_FORETAK"))

# +
# delreg_offentlig %>%
# filter(ORGNR_FORETAK == "983658776")
# -

delreg_offentlig_test <- delreg_offentlig %>%
  dplyr::select(Foretakstype, Helseregion, RHF, ORGNR_FORETAK, H_VAR1_A, ORGNR, NAVN, NYTT_NAVN,
                # SKJEMA_TYPE, 
                SN07_1, SN07_1_navn, F_POSTNR, F_POSTSTED) %>%
  dplyr::rename(Foretakstype = Foretakstype,
                Helseregion = Helseregion,
                Helseregion_navn = RHF,
                Foretaksorgnr = ORGNR_FORETAK,
                Rapporteringsnr = H_VAR1_A,
                Bedriftsorgnr = ORGNR,
                HF_navn = NAVN,
                Institusjonsnavn = NYTT_NAVN,
                # Skjematype = SKJEMA_TYPE,
                Næringskode = SN07_1,
                Næringsnavn = SN07_1_navn,
                Postnummer = F_POSTNR, 
                Poststed = F_POSTSTED)

# ### Beholder kun enheter med rapporteringsnummer

delreg_offentlig_test <- delreg_offentlig_test %>%
  dplyr::filter(!is.na(Rapporteringsnr))

# Sjekker for dubletter i Institusjonsnavn (?) #
delreg_offentlig_test_duplikater <- delreg_offentlig_test %>%
  janitor::get_dupes(Institusjonsnavn)
print(paste0("Dubletter finnes for: ", unique(delreg_offentlig_test_duplikater$Institusjonsnavn)))

# Sorterer etter helseregion #
delreg_offentlig_test <- delreg_offentlig_test %>%
  dplyr::arrange(Helseregion)

# ## Lagrer filen

openxlsx::write.xlsx(delreg_offentlig_test,
                     file = paste0(filsti_institusjonslister, aargang, " Offentlige institusjoner spesialisthelsetjenesten (", format(Sys.Date(), "%d%m%y"), ").xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE)

# ## Private (med og uten oppdragsdokument)

delreg_private <- delreg %>%
  dplyr::filter(!ORGNR_FORETAK %in% unique(offentlig$ORGNR_FORETAK)) %>%
  dplyr::left_join(oppdrag, by = "ORGNR_FORETAK") %>%
  dplyr::mutate(Foretakstype = case_when(
    ORGNR_FORETAK %in% unique(oppdrag$ORGNR_FORETAK) ~ "Private med oppdragsdokument", 
    TRUE ~ "Private med kjøpsavtale"))

unique(delreg_private$SKJEMA_TYPE)

delreg_private_test <- delreg_private %>%
  select(Foretakstype, Helseregion, RHF, ORGNR_FORETAK, H_VAR1_A, ORGNR, NYTT_NAVN,
         SKJEMA_TYPE, SN07_1, SN07_1_navn, F_POSTNR, F_POSTSTED, F_KOMMUNENR) %>%
  mutate(FYLKE = substr(F_KOMMUNENR, 1, 2)) %>%
  # Deretter bruk FYLKE i case_when
  mutate(HELSEREGION = case_when(
    FYLKE %in% c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "30", "31", "32", "33", "34", "38", "42", "40") ~ "Helse Sør-Øst",
    FYLKE %in% c("11", "12", "14", "46") ~ "Helse Vest",
    FYLKE %in% c("15", "16", "17", "50") ~ "Helse Midt-Norge",
    FYLKE %in% c("18", "19", "20", "54", "55", "56") ~ "Helse Nord",
    FYLKE == "25" ~ "Utlandet",
    TRUE ~ "Missing eller annet"))  %>% 
  mutate(DOGNBEHANDLING = case_when(
    nchar(H_VAR1_A) < 4 ~ "Nei",
    nchar(H_VAR1_A) == 9 ~ "Ja"))  %>% 
  dplyr::rename(Foretakstype = Foretakstype,
                Helseregion = Helseregion,
                Helseregion_navn = RHF,
                Foretaksorgnr = ORGNR_FORETAK,
                Rapporteringsnr = H_VAR1_A,
                Bedriftsorgnr = ORGNR,
                Institusjonsnavn = NYTT_NAVN,
                Skjematype = SKJEMA_TYPE,
                Næringskode = SN07_1,
                Næringsnavn = SN07_1_navn,
                Postnummer = F_POSTNR,
                Poststed = F_POSTSTED)

# ### Beholder kun enheter med rapporteringsnummer

# +
# delreg_private_test_obs <- delreg_private_test %>%
#   dplyr::filter(is.na(Rapporteringsnr))

# delreg_private_test_obs
# -

delreg_private_test <- delreg_private_test %>%
  dplyr::filter(!is.na(Rapporteringsnr))

# ### Lager liste over rapporteringsenhetene
# For å kunne koble på tjenesteområde på alle underenhetene.

rapporteringsenheter <- delreg_private_test  %>% 
  mutate(Tjenesteomraade = case_when(
    Skjematype == "381" ~ "TSB",
    Skjematype == "461" ~ "Somatikk",
    Skjematype == "441" ~ "VOP",
    Skjematype == "451" ~ "BUP",
    Skjematype == "47"  ~ "Rehabilitering",
    Rapporteringsnr == "47" ~ "Rehabilitering",
    Rapporteringsnr == "46P" ~ "Somatikk"))  %>% 
filter(Tjenesteomraade != "NA")  %>% 
distinct(Rapporteringsnr, Tjenesteomraade)

delreg_private_tjenesteomraade <- left_join(delreg_private_test, rapporteringsenheter, join_by(Rapporteringsnr))

# +
rapporteringsenheter  %>% 
filter(Rapporteringsnr == "974124262")

delreg_private_test  %>% 
filter(Rapporteringsnr == "974124262")

delreg_private_tjenesteomraade  %>% 
filter(Rapporteringsnr == "974124262")
# -

# ### Sjekker for dubletter i Institusjonsnavn (?)

delreg_private_test_duplikater <- delreg_private_tjenesteomraade %>%
  janitor::get_dupes(Institusjonsnavn)
print(paste0("Dubletter finnes for: ", unique(delreg_private_test_duplikater$Institusjonsnavn)))

# Sorterer etter helseregion #
delreg_private_tjenesteomraade <- delreg_private_tjenesteomraade %>%
  dplyr::arrange(Helseregion)

# ## Lagrer filen

openxlsx::write.xlsx(delreg_private_tjenesteomraade,
                     file = paste0(filsti_institusjonslister, aargang, " Private institusjoner spesialisthelsetjenesten (", format(Sys.Date(), "%d%m%y"), ").xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE)


