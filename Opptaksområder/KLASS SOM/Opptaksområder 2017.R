# # Opptaksområder 2017

aargang <- 2017

# +
options(repr.matrix.max.rows=600, repr.matrix.max.cols=2000)

suppressPackageStartupMessages({ 
library(tidyverse)
library(readxl)
library(klassR)
library(sf)
library(leaflet)
        })
# -

# ### Filstier

# +
arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"
arbeidsmappe_kart <- paste0(arbeidsmappe, "kart/", aargang, "/")

arbeidsmappe_opptak <- paste0(arbeidsmappe, "opptaksomrader/", aargang, "/")

if (file.exists(arbeidsmappe_opptak)==FALSE) {
  dir.create(arbeidsmappe_opptak)
}
# -

# ## Laster inn kart (grunnkrets)

# +
start.time <- Sys.time()

  rename_geometry <- function(g, name){
    current = attr(g, "sf_column")
    names(g)[names(g)==current] = name
    sf::st_geometry(g)=name
    g
  }

# grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_utenhav_", aargang, ".parquet")
grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_flate_", aargang, ".parquet")

# Lese inn filen som parquet med sfarrow
grunnkrets_kart <- sfarrow::st_read_parquet(grunnkrets_kart_filsti) %>%
sf::st_zm(drop = T) %>%
sf::st_cast("MULTIPOLYGON") %>%
  sf::st_transform(crs = 4326) %>%
  dplyr::rename(GRUNNKRETSNUMMER = GRUNNKRETS)

grunnkrets_kart <- rename_geometry(grunnkrets_kart, "geometry")
sf::st_geometry(grunnkrets_kart) <- "geometry"

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken

# +
# grunnkrets_kart_test <- grunnkrets_kart %>%
# dplyr::filter(GRUNNKRETSNUMMER %in% c("14440104", "14490113")) %>%
# sf::st_transform(crs = 4326)

# +
# T04317 <- PxWebApiData::ApiData(04317, ContentsCode = "Personer", 
#                                 Grunnkretser = TRUE, 
#                                 Tid = as.character(aargang+1)) [[2]] %>%
#   dplyr::filter(!is.na(value)) %>%
#   dplyr::rename(GRUNNKRETSNUMMER = Grunnkretser,
#                 PERSONER = value) %>%
#   dplyr::select(GRUNNKRETSNUMMER, PERSONER)

# T04317 %>%
# dplyr::filter(GRUNNKRETSNUMMER %in% c("14440108", "14490114"))

# +
# T04317_t1 <- PxWebApiData::ApiData(04317, ContentsCode = "Personer", 
#                                 Grunnkretser = TRUE, 
#                                 Tid = as.character(aargang)) [[2]] %>%
#   dplyr::filter(!is.na(value)) %>%
#   dplyr::rename(GRUNNKRETSNUMMER = Grunnkretser,
#                 PERSONER = value) %>%
#   dplyr::select(GRUNNKRETSNUMMER, PERSONER)

# T04317_t1 %>%
# dplyr::filter(GRUNNKRETSNUMMER %in% c("14440104", "14490113"))
# -

# ## Kodeliste for opptaksområder i spesialisthelsetjenesten (somatikk)

# +
opptaksomrader_KLASS <- klassR::GetKlass(629, output_style = "wide", date = c(paste0(aargang+1, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER = code4, 
             GRUNNKRETS_NAVN = name4, 
             OPPTAK_NUMMER = code3, 
             OPPTAK = name3, 
             ORGNR_HF = code2, 
             NAVN_HF = name2, 
             ORGNR_RHF = code1, 
             NAVN_RHF = name1)

nrow(opptaksomrader_KLASS)
# -

# ### Korrespondanse mellom t og t-1 fra KLASS

# +
grunnkrets_KLASS <- klassR::GetKlass(1, date = c(paste0(aargang, "-01-01"), paste0(aargang+1, "-01-01")), correspond = TRUE) %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = sourceCode, 
             GRUNNKRETSNUMMER = targetCode, 
             targetName = targetName)

# grunnkrets_KLASS <- grunnkrets_KLASS %>%
# dplyr::filter(!GRUNNKRETSNUMMER_T1 %in% c("14440104") |  !GRUNNKRETSNUMMER %in% c("14490114")) # Fjerner Kjøs fra Markane

nrow(grunnkrets_KLASS)
# -

# OK

grunnkrets_KLASS %>%
group_by(GRUNNKRETSNUMMER) %>% # grunnkrets_KLASS_T1_2
filter(n()>1)

grunnkrets_KLASS %>%
group_by(GRUNNKRETSNUMMER_T1) %>% # grunnkrets_KLASS_T1_2
filter(n()>1)

# ### Merger opptaksområder med KLASS (t-1)

# +
opptaksomrader_KLASS_2 <- opptaksomrader_KLASS %>%
dplyr::left_join(grunnkrets_KLASS, by = "GRUNNKRETSNUMMER") %>%
dplyr::mutate(GRUNNKRETSNUMMER_T1 = case_when(
is.na(GRUNNKRETSNUMMER_T1) ~ GRUNNKRETSNUMMER, 
    TRUE ~ GRUNNKRETSNUMMER_T1
)) %>%
dplyr::select(GRUNNKRETSNUMMER, GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF) %>%
dplyr::distinct(GRUNNKRETSNUMMER_T1, OPPTAK_NUMMER, OPPTAK, ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF)

nrow(opptaksomrader_KLASS_2)

# +
# opptaksomrader_KLASS_2 %>%
# dplyr::filter(GRUNNKRETSNUMMER_T1 == "18500109")
# -

# ### Laster inn KLASS (t-1)

# +
grunnkrets_KLASS_T1 <- klassR::GetKlass(1, output_style = "wide", date = c(paste0(aargang, "-01-01"))) %>%
dplyr::rename(GRUNNKRETSNUMMER_T1 = code2, 
             GRUNNKRETS_NAVN_T1 = name2) %>%
dplyr::select(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1) 

nrow(grunnkrets_KLASS_T1)
nrow(opptaksomrader_KLASS_2)

# +
# opptaksomrader_KLASS_2 %>%
# group_by(GRUNNKRETSNUMMER_T1) %>% # grunnkrets_KLASS_T1_2
# filter(n()>1)

# +
nrow(grunnkrets_KLASS_T1)

grunnkrets_KLASS_T1_2 <- grunnkrets_KLASS_T1 %>%
dplyr::left_join(opptaksomrader_KLASS_2, by = "GRUNNKRETSNUMMER_T1") %>%
dplyr::filter(!is.na(OPPTAK))

nrow(grunnkrets_KLASS_T1_2)

# head(grunnkrets_KLASS_T1_2)

# grunnkrets_KLASS_T1_2 <- grunnkrets_KLASS_T1 %>%
# dplyr::distinct(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1, OPPTAK)
# -

grunnkrets_KLASS_T1_2 %>%
group_by(GRUNNKRETSNUMMER_T1) %>% # grunnkrets_KLASS_T1_2
filter(n()>1)

# +
# grunnkrets_KLASS_T1_2
# -

# ### Lagrer filen 

# +
test <- grunnkrets_KLASS_T1_2 %>%
  dplyr::mutate(TOM_FORELDER = "") %>%
  dplyr::select(GRUNNKRETSNUMMER_T1, GRUNNKRETS_NAVN_T1, OPPTAK_NUMMER, ORGNR_HF, NAVN_HF, OPPTAK, ORGNR_RHF, NAVN_RHF, TOM_FORELDER)

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
  dplyr::select(GRUNNKRETSNUMMER_T1, OPPTAK_NUMMER, GRUNNKRETS_NAVN_T1) %>%
  dplyr::rename('ns1:kode' = GRUNNKRETSNUMMER_T1, 
                'ns1:forelder' = OPPTAK_NUMMER, 
                'ns1:navn_bokmål' = GRUNNKRETS_NAVN_T1) %>%
  dplyr::distinct()

KLASS <- rbind(level_1, level_2, level_3, level_4)
# -

openxlsx::write.xlsx(KLASS, file = paste0("/ssb/bruker/rdn/opptak_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes
