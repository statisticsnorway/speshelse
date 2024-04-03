# # Kartfiler til .parquet
#
# OBS: grunnkrets og fylke mangler for 2020!

source("/home/jovyan/fellesr/R/DAPLA_funcs.R")
library(tidyverse)

# +
aargang <- 2023

# Logg på for å få tilgang til data fra X-disken
# system("kinit", input = getPass::getPass("Skriv inn Windows-passord"))
# -

arbeidsmappe <- "/ssb/stamme01/fylkhels/speshelse/felles/"

# +
kart_mappe <- paste0(arbeidsmappe, "kart/")

if (file.exists(kart_mappe)==FALSE) {
  dir.create(kart_mappe)
}

# +
kart_aargang_mappe <- paste0(kart_mappe, aargang, "/")

if (file.exists(kart_aargang_mappe)==FALSE) {
  dir.create(kart_aargang_mappe)
}
# -

# ## Laster inn filer

sf::st_layers(paste0("/ssb/x_disk/Felles/GIS-ressurssenter/KartdataQ/BasisKart/kart", aargang, ".gdb"))

# +
# ABAS_grunnkrets_utenhav <- sf::read_sf(paste0("/ssb/x_disk/Felles/GIS-ressurssenter/KartdataQ/BasisKart/kart", aargang, ".gdb"), layer = paste0("ABAS_grunnkrets_utenhav_", aargang))
# -

# ABAS_grunnkrets_flate <- sf::read_sf(paste0("/ssb/x_disk/Felles/GIS-ressurssenter/KartdataQ/BasisKart/kart", aargang, ".gdb"), layer = paste0("ABAS_grunnkrets_flate_", aargang))
ABAS_grunnkrets_flate <- sfarrow::st_read_parquet("/home/jovyan/ABAS_grunnkrets_flate_2023.parquet")

# +
ABAS_grunnkrets_flate <- ABAS_grunnkrets_flate %>%
dplyr::rename(OBJTYPE = objtype, 
             VERSJONID = versjonid, 
             DATAUTTAKS = datauttaksdato, 
             DATAFANGST = datafangstdato, 
             OPPDATERIN = oppdateringsdato, 
             OPPHAV = opphav, 
             GRUNNKRE_1 = grunnkretsnavn, 
             GRUNNKRETS = grunnkretsnummer, 
             KOMMUNENR = kommunenummer, 
             Shape_Length = SHAPE_Length, 
             Shape_Area = SHAPE_Area) %>%
dplyr::select(-lokalid, -navnerom)

head(ABAS_grunnkrets_flate)
# -

ABAS_kommune_utenhav <- sf::read_sf(paste0("/ssb/x_disk/Felles/GIS-ressurssenter/KartdataQ/BasisKart/kart", aargang, ".gdb"), layer = paste0("ABAS_kommune_utenhav_", aargang))

ABAS_kommune_flate <- sf::read_sf(paste0("/ssb/x_disk/Felles/GIS-ressurssenter/KartdataQ/BasisKart/kart", aargang, ".gdb"), layer = paste0("ABAS_kommune_flate_", aargang))

# +
# ABAS_fylke_flate <- sf::read_sf(paste0("/ssb/x_disk/Felles/GIS-ressurssenter/KartdataQ/BasisKart/kart", aargang, ".gdb"), layer = paste0("ABAS_fylke_flate_", aargang))
# -

# ## Lagrer filer

# +
# sfarrow::st_write_parquet(obj=ABAS_grunnkrets_utenhav, dsn=paste0(kart_aargang_mappe, "ABAS_grunnkrets_utenhav_", aargang, ".parquet"))
# -

# sfarrow::st_write_parquet(obj=ABAS_grunnkrets_flate, dsn=paste0(kart_aargang_mappe, "ABAS_grunnkrets_flate_", aargang, ".parquet"))
arbeidsmappe_kart <- paste0("ssb-prod-dapla-felles-data-delt/GIS/Kart/", aargang, "/")
grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_flate_", aargang, "/ABAS_grunnkrets_flate_", aargang, ".parquet")
write_SSB(ABAS_grunnkrets_flate, file = grunnkrets_kart_filsti, sf = TRUE)

sfarrow::st_write_parquet(obj=ABAS_kommune_utenhav, dsn=paste0(kart_aargang_mappe, "ABAS_kommune_utenhav_", aargang, ".parquet"))

sfarrow::st_write_parquet(obj=ABAS_kommune_flate, dsn=paste0(kart_aargang_mappe, "ABAS_kommune_flate_", aargang, ".parquet"))

# +
# sfarrow::st_write_parquet(obj=ABAS_fylke_flate, dsn=paste0(kart_aargang_mappe, "ABAS_fylke_flate_", aargang, ".parquet"))
# -


