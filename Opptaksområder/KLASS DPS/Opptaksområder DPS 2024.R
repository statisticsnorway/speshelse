# -*- coding: utf-8 -*-
# # Opptaksområder 2024

aargang <- 2024

# +
options(repr.matrix.max.rows=600, repr.matrix.max.cols=2000)

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
    library(fellesr)
        })
# -

# ## Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk) t-1

# +
opptaksomrader_KLASS <- klassR::GetKlass(632, output_style = "wide", date = c(paste0(aargang-1, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

opptaksomrader_KLASS_test <- opptaksomrader_KLASS %>%
filter(!GRUNNKRETS_NAVN %in% c("Trondheim (postnummer)", "Kristiansand (postnummer)"))

opptaksomrader_KLASS_postnummer <- opptaksomrader_KLASS %>%
filter(GRUNNKRETS_NAVN %in% c("Trondheim (postnummer)", "Kristiansand (postnummer)"))

nrow(opptaksomrader_KLASS)
# -

# ### Sjekker antall grunnkretser i kodelistene for opptaksområde vs. grunnkretser
#
# Ok 2019: 50379999 - Uoppgitt grunnkrets finnes ikke i grunnkretskodelisten, burde vært der

# +
# aargang-1

# +
# grunnkrets_KLASS_T1 <- klassR::GetKlass(1, output_level = 2, date = c(paste0(aargang-1, "-01-01"))) %>%
# dplyr::rename(GRUNNKRETSNUMMER = code, 
#              GRUNNKRETS_NAVN = name) %>%
# select(GRUNNKRETSNUMMER, GRUNNKRETS_NAVN) %>%
# filter(substr(GRUNNKRETSNUMMER, 1, 2) != "21") %>%
# filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("1001", "5001", "4204"))

# kommune_KLASS <- klassR::GetKlass(131, date = c(paste0(aargang-1, "-01-01"))) %>%
# rename(KOMMUNENR = code, 
#       KOMMUNENAVN = name) %>%
# select(KOMMUNENR, KOMMUNENAVN)

# nrow(grunnkrets_KLASS_T1)-nrow(opptaksomrader_KLASS)

# opptaksomrader_KLASS_test <- opptaksomrader_KLASS %>%
# filter(!GRUNNKRETS_NAVN %in% c("Trondheim (postnummer)", "Kristiansand (postnummer)"))

# nrow(grunnkrets_KLASS_T1)
# nrow(opptaksomrader_KLASS_test)

# opptaksomrader_KLASS_test %>%
# mutate(KOMMNR = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
# group_by(KOMMNR) %>%
# tally() %>%
# filter(!KOMMNR %in% unique(kommune_KLASS$KOMMUNENR))

# opptaksomrader_KLASS_test %>%
# filter(!GRUNNKRETSNUMMER %in% unique(grunnkrets_KLASS_T1$GRUNNKRETSNUMMER)) %>%
# filter(GRUNNKRETS_NAVN != "Uoppgitt grunnkrets")
# -

# ### Laster inn grunnkretser for nyeste årgang (T0)

# +
grunnkrets_KLASS_T0 <- klassR::GetKlass(1, output_level = 2, date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code, 
             GRUNNKRETS_NAVN = name) %>%
select(GRUNNKRETSNUMMER, GRUNNKRETS_NAVN) %>%
filter(substr(GRUNNKRETSNUMMER, 1, 2) != "21") %>%
filter(!substr(GRUNNKRETSNUMMER, 1, 4) %in% c("4204", "5001"))


nrow(grunnkrets_KLASS_T0)
# -

# ### Korrespondanse mellom t og t-1 fra KLASS

# +
grunnkrets_KLASS <- klassR::GetKlass(1, date = c(paste0(aargang-1, "-01-01"), paste0(aargang, "-01-01")), correspond = TRUE) %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = sourceCode, 
             GRUNNKRETSNUMMER = targetCode, 
             targetName = targetName) %>%
dplyr::left_join(opptaksomrader_KLASS_test, by = c("GRUNNKRETSNUMMER_T1" = "GRUNNKRETSNUMMER"))

nrow(grunnkrets_KLASS)

# +
nytt_navn <- grunnkrets_KLASS %>%
dplyr::filter(GRUNNKRETSNUMMER_T1 == GRUNNKRETSNUMMER) %>%
dplyr::rename(GAMMELT_NAVN= sourceName, 
             NYTT_NAVN = targetName)

nrow(nytt_navn)
nytt_navn
# -

endringer <- grunnkrets_KLASS %>%
dplyr::filter(GRUNNKRETSNUMMER_T1 !=GRUNNKRETSNUMMER )
nrow(endringer)

# +
# # SLETT
# unique(endringer$GRUNNKRETSNUMMER_T1)

# # LEGG TIL
# unique(endringer$GRUNNKRETSNUMMER)

# endringer

# +
endringer_2 <- endringer %>%
select('GRUNNKRETSNUMMER','GRUNNKRETS_NAVN','OPPTAK_NUMMER','OPPTAK','ORGNR_HF', 'NAVN_HF','ORGNR_RHF','NAVN_RHF')

opptaksomrader_KLASS_2 <- opptaksomrader_KLASS_test %>%
filter(!GRUNNKRETSNUMMER %in% unique(endringer$GRUNNKRETSNUMMER_T1))

nrow(endringer_2)+nrow(opptaksomrader_KLASS_2)
# -

# Fiks disse med korrespondanse med kommune?

# +
kommune_KLASS <- klassR::GetKlass(131, date = c(paste0(aargang-1, "-01-01"), paste0(aargang, "-01-01")), correspond = TRUE) 

# kommune_KLASS
# -

opptaksomrader_KLASS_kommune <- opptaksomrader_KLASS_test %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = GRUNNKRETSNUMMER) %>%
filter(substr(GRUNNKRETSNUMMER_T1, 5, 8) == "9999") %>%
mutate(KOMMUNENUMMER_T1 = substr(GRUNNKRETSNUMMER_T1, 1, 4))

# +
kommune_KLASS <- klassR::GetKlass(131, date = c(paste0(aargang-1, "-01-01"), paste0(aargang, "-01-01")), correspond = TRUE) %>%
dplyr::rename(KOMMUNENUMMER_T1 = sourceCode, 
             KOMMUNENUMMER = targetCode, 
             targetName = targetName) %>%
dplyr::left_join(opptaksomrader_KLASS_kommune, by = "KOMMUNENUMMER_T1") %>%
dplyr::mutate(GRUNNKRETSNUMMER = paste0(KOMMUNENUMMER, "9999")) 

opptaksomrader_KLASS_3 <- opptaksomrader_KLASS_2 %>%
filter(!GRUNNKRETSNUMMER %in% unique(kommune_KLASS$GRUNNKRETSNUMMER_T1))

uoppgitt_grunnkrets <- kommune_KLASS %>%
select('GRUNNKRETSNUMMER','GRUNNKRETS_NAVN','OPPTAK_NUMMER','OPPTAK','ORGNR_HF', 'NAVN_HF','ORGNR_RHF','NAVN_RHF')


nrow(uoppgitt_grunnkrets)

# +
opptaksomrader_ny <- rbind(endringer_2, opptaksomrader_KLASS_3, uoppgitt_grunnkrets)
nrow(opptaksomrader_ny)

grunnkrets_KLASS_T0 %>%
filter(!GRUNNKRETSNUMMER %in% unique(opptaksomrader_ny$GRUNNKRETSNUMMER))

opptaksomrader_ny %>%
filter(!GRUNNKRETSNUMMER %in% unique(grunnkrets_KLASS_T0$GRUNNKRETSNUMMER))
# -

# ## Sjekker bydeler i Oslo

bydeler_KLASS <- klassR::GetKlass(1, 
                                  correspond = 103,
                                  date = c(paste0(aargang, "-01-01"))) %>%
rename(GRUNNKRETSNUMMER = sourceCode, 
      BYDELSNUMMER = targetCode, 
      BYDELSNAVN = targetName) %>%
select(GRUNNKRETSNUMMER, BYDELSNUMMER, BYDELSNAVN)

# +
opptaksomrader_ny_bydel <- opptaksomrader_ny %>%
filter(substr(GRUNNKRETSNUMMER, 1, 4) == "0301") %>%
filter(GRUNNKRETSNUMMER != "03019999") %>%
dplyr::left_join(bydeler_KLASS, by = "GRUNNKRETSNUMMER") %>%
distinct(BYDELSNUMMER, BYDELSNAVN, OPPTAK) %>%
arrange(BYDELSNUMMER) %>%
rename(OPPTAK_DPS = OPPTAK)

opptaksomrader_ny_bydel
# -

# ### Legger til postnummerområder

opptaksomrader_ny_2 <- rbind(opptaksomrader_ny, opptaksomrader_KLASS_postnummer)

# ## Sjekk om antall kommuner blir riktig

# +
kommune_KLASS <- klassR::GetKlass(131, date = c(paste0(aargang, "-01-01"))) %>%
rename(KOMMUNENR = code, 
      KOMMUNENAVN = name) %>%
select(KOMMUNENR, KOMMUNENAVN)

nrow(kommune_KLASS)

opptaksomrader_ny_kommune <- opptaksomrader_ny %>%
mutate(KOMMNR = substr(GRUNNKRETSNUMMER, 1, 4)) %>%
group_by(KOMMNR) %>%
tally() %>%
arrange(n)

nrow(opptaksomrader_ny_kommune)
# -

kommune_KLASS %>%
filter(!KOMMUNENR %in% unique(opptaksomrader_ny_kommune$KOMMNR))

# ## Sjekker ny årgang mot kart

# ### Last inn kart over opptaksområde på laveste nivå + grunnkretsene i listen for T og T1!
#
# st_intersection? Sjekk at ingen av opptaksområdene bytter opptaksområde!

# +
arbeidsmappe_kart <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/kart/", aargang, "/")
arbeidsmappe_kart_t1 <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/kart/", aargang-1, "/")

grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_flate_", aargang, "/")
grunnkrets_kart_t1_filsti <- paste0(arbeidsmappe_kart_t1, "ABAS_grunnkrets_flate_", aargang-1, "/")

opptaksomrader_DPS_lokasjon_filsti <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang-1, 
                                             "/opptaksomrader_DPS_lokasjon_flate_", aargang-1, ".parquet")
# -

opptaksomrader_DPS_lokasjon_filsti

opptaksomrader_DPS_lokasjon <- arrow::open_dataset(opptaksomrader_DPS_lokasjon_filsti) %>%
    sfarrow::read_sf_dataset() %>%
sf::st_transform(crs = 4326)

# +
# grunnkrets_kart <- open_dataset(grunnkrets_kart_filsti) %>%
#     sfarrow::read_sf_dataset() %>%
# sf::st_zm(drop = T) %>%
# sf::st_cast("MULTIPOLYGON") %>%
#   sf::st_transform(crs = 4326) %>%
#   dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS) %>%
# dplyr::filter(GRUNNKRETSNUMMER %in% unique(endringer$GRUNNKRETSNUMMER))

# arbeidsmappe_kart_t1 <- paste0("ssb-prod-dapla-felles-data-delt/GIS/Kart/", aargang-1, "/")
# grunnkrets_kart_t1_filsti <- paste0(arbeidsmappe_kart_t1, "ABAS_grunnkrets_flate_", aargang-1, "/")

# grunnkrets_kart_t1 <- open_dataset(grunnkrets_kart_t1_filsti) %>%
#     sfarrow::read_sf_dataset() %>%
# sf::st_zm(drop = T) %>%
# sf::st_cast("MULTIPOLYGON") %>%
#   sf::st_transform(crs = 4326) %>%
#   dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS) %>%
# dplyr::filter(GRUNNKRETSNUMMER %in% unique(endringer$GRUNNKRETSNUMMER_T1))
# -
# ### Lagrer filen


opptaksomrader_ny_2 %>%
head()

# + endofcell="--"
test <- opptaksomrader_ny_2 %>%
  dplyr::mutate(TOM_FORELDER = "") %>%
  # dplyr::filter(opptak %in% c("Stavanger", "Ålesund")) %>%
  dplyr::select(GRUNNKRETSNUMMER, GRUNNKRETS_NAVN, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF, TOM_FORELDER)

level_1 <- test %>%
  dplyr::select(ORGNR_RHF, TOM_FORELDER, NAVN_RHF) %>%
  dplyr::rename('ns1:kode' = ORGNR_RHF, 
                'ns1:forelder' = TOM_FORELDER, 
                'ns1:navn_bokmål' = NAVN_RHF) %>%
  dplyr::distinct()

level_2 <- test %>%
  dplyr::select(ORGNR_HF, ORGNR_RHF, NAVN_HF) %>%
  dplyr::rename('ns1:kode' = ORGNR_HF, 
                'ns1:forelder' = ORGNR_RHF, 
                'ns1:navn_bokmål' = NAVN_HF) %>%
  dplyr::distinct()

level_3 <- test %>%
  dplyr::select(OPPTAK_NUMMER, ORGNR_HF, OPPTAK) %>%
  dplyr::rename('ns1:kode' = OPPTAK_NUMMER, 
                'ns1:forelder' = ORGNR_HF, 
                'ns1:navn_bokmål' = OPPTAK) %>%
  dplyr::distinct()

level_4 <- test %>%
  dplyr::select(GRUNNKRETSNUMMER, OPPTAK_NUMMER, GRUNNKRETS_NAVN) %>%
  dplyr::rename('ns1:kode' = GRUNNKRETSNUMMER, 
                'ns1:forelder' = OPPTAK_NUMMER, 
                'ns1:navn_bokmål' = GRUNNKRETS_NAVN) %>%
  dplyr::distinct()

opptaksomrader_KLASS_med_fix_KLASS <- rbind(level_1, level_2, level_3, level_4)
# -

head(opptaksomrader_KLASS_med_fix_KLASS)
# --

openxlsx::write.xlsx(opptaksomrader_KLASS_med_fix_KLASS, file = paste0("/ssb/bruker/rdn/opptak_DPS_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes
