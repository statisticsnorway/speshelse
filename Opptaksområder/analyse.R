# -*- coding: utf-8 -*-
# # Analyse

library(tidyverse)

aargang <- 2024

befolkning_per_opptaksomrade_masterfil_filsti <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/befolkning_per_opptaksomrade/masterfil/befolkning_per_opptaksomrade_masterfil_", aargang, ".parquet")

befolkning_per_opptaksomrade_masterfil <- arrow::read_parquet(befolkning_per_opptaksomrade_masterfil_filsti) %>% 
  filter(TJENESTE == "SOM", KJOENN == 0, ALDER_KODE != "999", LEVEL == "HF") %>% 
  mutate(ALDER_KODE = as.numeric(gsub("\\+$", "", as.character(ALDER_KODE)))) %>% 
  mutate(ALDERSGRUPPERING = case_when(ALDER_KODE <= 5 ~ "0-5 år",
                                      ALDER_KODE %in% 6:15 ~ "6-15 år",
                                      ALDER_KODE %in% 16:66 ~ "16-66 år",
                                      ALDER_KODE >= 67 ~ "67 år eller eldre"))


# ## Aldersfordeling per HF (aldersgrupper)

befolkning_per_HF_tot <- befolkning_per_opptaksomrade_masterfil %>% 
  group_by(NAVN_HF) %>% 
  summarise(Befolkning = sum(PERSONER))


befolkning_per_HF_aldersgrupper <- befolkning_per_opptaksomrade_masterfil %>% 
  group_by(NAVN_HF, NAVN_RHF, ALDERSGRUPPERING) %>% 
  summarise(PERSONER = sum(PERSONER)) %>% 
  left_join(befolkning_per_HF_tot, by = "NAVN_HF") %>% 
  mutate(ANDEL = (PERSONER/Befolkning)*100, 
         ANDEL_AVRUNDDET = round(ANDEL, digits = 1)) %>% 
  pivot_wider(id_cols = c("NAVN_RHF", "NAVN_HF"),
              names_from = "ALDERSGRUPPERING",
              values_from = "ANDEL_AVRUNDDET") %>% 
  dplyr::left_join(befolkning_per_HF_tot) %>% 
  select(NAVN_RHF, NAVN_HF, Befolkning, '0-5 år', '6-15 år', '16-66 år', '67 år eller eldre')

openxlsx::write.xlsx(befolkning_per_HF_aldersgrupper, file = paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/analyse/befolkning_per_HF_aldersgrupper_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes

befolkning_per_HF_aldersgrupper

# ## Aldersfordeling  per HF (ettårig alder)

befolkning_per_HF_alder <- befolkning_per_opptaksomrade_masterfil %>% 
  filter(NAVN_HF %in% c("SYKEHUSET INNLANDET HF", "LOVISENBERG DIAKONALE SYKEHUS AS")) %>% 
  mutate(ALDER = case_when(ALDER_KODE == 105 ~ "105 år eller eldre", TRUE ~ paste0(ALDER_KODE, " år"))) %>% 
  select(NAVN_HF, ALDER, PERSONER) %>% 
  arrange(desc(NAVN_HF)) %>% 
  pivot_wider(id_cols = c("ALDER"),
              names_from = "NAVN_HF",
              values_from = "PERSONER") %>% 
  rename(Alder = ALDER, 
         'Lovisenberg diakonale sykehus AS' = 'LOVISENBERG DIAKONALE SYKEHUS AS', 
         'Sykehuset Innlandet HF' = 'SYKEHUSET INNLANDET HF')

openxlsx::write.xlsx(befolkning_per_HF_alder, file = paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/analyse/befolkning_per_HF_alder_", aargang, ".xlsx"),
                     rowNames = FALSE,
                     showNA = FALSE,
                     overwrite=T) # T = overskriver dersom filen allerede finnes, F = gir feilmelding dersom filen finnes

befolkning_per_HF_alder         

# ## Lager kart

opptaksomrade_HF_filsti <- paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/", aargang, "/opptaksomrader_SOM_HF_utenhav_", aargang, ".parquet")
opptaksomrade_HF_filsti

opptaksomrader_HF <- sfarrow::st_read_parquet(opptaksomrade_HF_filsti)

# +
opptaksomrader_HF_oslo <- opptaksomrader_HF %>%
filter(NAVN_HF %in% c("OSLO UNIVERSITETSSYKEHUS HF", 
                      "LOVISENBERG DIAKONALE SYKEHUS AS", 
                       "AKERSHUS UNIVERSITETSSYKEHUS HF",
                      "DIAKONHJEMMET SYKEHUS AS")) %>%
sf::st_cast("MULTIPOLYGON") %>%
sf::st_cast("POLYGON") %>%
select(ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF)

sf::st_write(opptaksomrader_HF_oslo, paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/analyse/opptaksomrader_SOM_HF_utenhav_", aargang, "_polygon_oslo.geojson"), append=FALSE)

ggplot() + 
geom_sf(data = opptaksomrader_HF_oslo)

# +
opptaksomrader_HF_simpl <- opptaksomrader_HF %>%
sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000) %>%
sf::st_cast("MULTIPOLYGON") %>%
sf::st_cast("POLYGON") %>%
select(ORGNR_HF, NAVN_HF, ORGNR_RHF, NAVN_RHF)

sf::st_write(opptaksomrader_HF_simpl, paste0("/ssb/stamme01/fylkhels/speshelse/felles/opptaksomrader/analyse/opptaksomrader_SOM_HF_utenhav_", aargang, "_polygon_simpl.geojson"), append=FALSE)

nrow(nrow(opptaksomrader_HF_simpl))

ggplot() + 
geom_sf(data = opptaksomrader_HF_simpl)
