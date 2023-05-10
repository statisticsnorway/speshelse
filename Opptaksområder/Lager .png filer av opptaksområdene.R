# -*- coding: utf-8 -*-
# # Lager .png filer av opptaksområdene

aargang <- 2023

# +
utenhav <- TRUE

if (utenhav == TRUE) {
filsti_med_uten_hav <- "utenhav"
    } else if (utenhav == FALSE) {
  filsti_med_uten_hav <- "flate"
}

filsti_med_uten_hav

sf::sf_use_s2(FALSE)
CRS <- 4326
# -

# ### Laster inn pakker

# +
renv::load()
# renv::autoload()

suppressPackageStartupMessages({ 
library(arrow)
library(sf)
library(sfarrow)
library(tidyverse)
library(readxl)
library(klassR)
library(leaflet)
library(sfarrow)  
library(cowplot)
library(fellesr)
        })

# OBS: legg til open_dataset under NAMESPACE + export i fellesr
# source("/home/jovyan/fellesr/R/DAPLA_funcs.R")

  rename_geometry <- function(g, name){
    current = attr(g, "sf_column")
    names(g)[names(g)==current] = name
    sf::st_geometry(g)=name
    g
  }


# +
arbeidsmappe_kart <- paste0("ssb-prod-dapla-felles-data-delt/GIS/Kart/", aargang, "/")

# grunnkrets_kart_filsti <- paste0(arbeidsmappe_kart, "ABAS_grunnkrets_utenhav_", aargang, "/")

opptaksomrader_SOM_RHF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_RHF_", filsti_med_uten_hav, "_", aargang, ".parquet")
opptaksomrader_SOM_RHF_filsti

opptaksomrader_SOM_HF_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_HF_", filsti_med_uten_hav, "_", aargang, ".parquet")
opptaksomrader_SOM_HF_filsti

opptaksomrader_SOM_lokasjon_filsti <- paste0("ssb-prod-helse-speshelse-data-kilde/felles/Kart/", aargang, "/Opptaksområder/opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", aargang, "/opptaksomrader_SOM_lokasjon_", filsti_med_uten_hav, "_", aargang, ".parquet")
opptaksomrader_SOM_lokasjon_filsti
# -

# ## Laster inn opptaksområder

opptaksomrader_SOM_RHF_filsti

# +
opptaksomrader_SOM_RHF <- fellesr::read_SSB(opptaksomrader_SOM_RHF_filsti, sf = TRUE) 
# sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

opptaksomrader_SOM_HF <- fellesr::read_SSB(opptaksomrader_SOM_HF_filsti, sf = TRUE) 
# sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)

opptaksomrader_SOM_lokasjon <- fellesr::read_SSB(opptaksomrader_SOM_lokasjon_filsti, sf = TRUE) 
# sf::st_simplify(preserveTopology = FALSE, dTolerance = 1000)
# -

# ### Laster inn kommunekart

# kommunekart <- fellesr::read_SSB("ssb-prod-dapla-felles-data-delt/GIS/2022/ABAS_kommune_flate_2022.parquet", sf = TRUE) 
kommunekart <- fellesr::read_SSB(paste0("ssb-prod-kart-data-delt/kartdata_analyse/klargjorte-data/2023/ABAS_kommune_flate_p", aargang, "_v1.parquet"), sf = TRUE) 

kommunekart_oslo <- kommunekart %>%
filter(KOMMUNENR == "0301") %>%
sf::st_set_crs(25833) %>%
sf::st_transform(crs = 4326)

# ### SSB fargepalett

# +
Sys.setenv(no_proxy = "nexus.ssb.no, git-adm.ssb.no, i.test.ssb.no, i.ssb.no, data.ssb.no, api.github.com, github.com") # OBS: denne burde ikke være nødvendig?

ssb_farger <- klassR::GetKlass(614, output_style = "wide") %>%
  dplyr::rename(farge_nummer = code3, 
                HEX = name3, 
                farge = name2, 
                type = name1) %>%
  dplyr::select(-code1, -code2) %>%
  dplyr::filter(farge != "Hvit")

ssb_farger_blaa <- ssb_farger %>%
dplyr::filter(farge_nummer == "SSB Blå 2")

# ssb_farger

ssb_farger_oslo <- ssb_farger %>%
dplyr::filter(farge_nummer %in% c("SSB Grønn 1", "SSB Blå 1", "SSB Gul 1", "SSB Rød 1"))
# -

# ## HF (alle regioner)
#
# TO DO
# + Legg til navn på HF (en versjon med og uten) + inset map for Oslo

# +
# ggplot() + 
# geom_sf(data = opptaksomrader_SOM_RHF)

# ggplot() + 
# geom_sf(data = opptaksomrader_SOM_HF)

# ggplot() + 
# geom_sf(data = opptaksomrader_SOM_lokasjon)

# +
sf_use_s2(FALSE)
options(repr.plot.width=20, repr.plot.height=20)

opptaksomrader_SOM_HF_test <- opptaksomrader_SOM_HF %>%
# filter(!NAVN_RHF %in% c("HELSE VEST RHF")) %>%
dplyr::mutate(farge_nummer = case_when(NAVN_HF == "FINNMARKSSYKEHUSET HF" ~ "SSB Blå 2", 
                                       NAVN_HF == "UNIVERSITETSSYKEHUSET NORD-NORGE HF" ~ "SSB Blå 3",
                                       NAVN_HF == "NORDLANDSSYKEHUSET HF" ~ "SSB Blå 4", 
                                       NAVN_HF == "HELGELANDSSYKEHUSET HF" ~ "SSB Blå 5", 
                                       NAVN_HF == "HELSE NORD-TRØNDELAG HF" ~ "SSB Grønn 2", 
                                       NAVN_HF == "ST OLAVS HOSPITAL HF" ~ "SSB Grønn 3", 
                                       NAVN_HF == "HELSE MØRE OG ROMSDAL HF" ~ "SSB Grønn 4", 
                                       NAVN_HF == "SØRLANDET SYKEHUS HF" ~ "SSB Lilla 1", 
                                       NAVN_HF == "SYKEHUSET TELEMARK HF" ~ "SSB Lilla 2", 
                                       NAVN_HF == "VESTRE VIKEN HF" ~ "SSB Lilla 3", 
                                       NAVN_HF == "SYKEHUSET INNLANDET HF" ~ "SSB Lilla 4", 
                                       NAVN_HF == "AKERSHUS UNIVERSITETSSYKEHUS HF" ~ "SSB Lilla 5",
                                       NAVN_HF == "OSLO UNIVERSITETSSYKEHUS HF" ~ "SSB Rød 1", 
                                       NAVN_HF == "LOVISENBERG DIAKONALE SYKEHUS AS" ~ "SSB Rød 2", 
                                       NAVN_HF == "DIAKONHJEMMET SYKEHUS AS" ~ "SSB Rød 3", 
                                       NAVN_HF == "SYKEHUSET I VESTFOLD HF" ~ "SSB Rød 4", 
                                       NAVN_HF == "SYKEHUSET ØSTFOLD HF" ~ "SSB Rød 5", 
                                       NAVN_HF == "HELSE FØRDE HF" ~ "SSB Gul 1", 
                                        NAVN_HF == "HELSE BERGEN HF" ~ "SSB Gul 2", 
                                       NAVN_HF == "HELSE FONNA HF" ~ "SSB Gul 3", 
                                       NAVN_HF == "HELSE STAVANGER HF" ~ "SSB Gul 4",
                                        TRUE ~ "")) %>%
dplyr::left_join(ssb_farger, by = "farge_nummer")

farger_HF <- as.character(opptaksomrader_SOM_HF_test$HEX)
names(farger_HF) <- opptaksomrader_SOM_HF_test$NAVN_HF

HF_kart <- ggplot() + 
geom_sf(data = opptaksomrader_SOM_HF_test, aes(fill = NAVN_HF), color = "white", lwd = 1) +
scale_fill_manual(values = farger_HF) + 
theme(legend.position = "none") +
theme_void()

# geom_sf(data = opptaksomrader_SOM_HF_test, 
#         # fill = ssb_farger_blaa$HEX, 
#         color = "white", 
#         lwd = 1) +
# geom_sf(data = point_both, color = "black", lwd = 0.1) +
# coord_sf(expand = FALSE) +
# geom_point(data = opptaksomrader_SOM_HF_H12_coords, aes(x = X, y = Y), colour = "white", size = 2) +
# geom_text(data = opptaksomrader_SOM_HF_H12_coords_right, aes(X, Y, label = NAVN_HF), colour = "black", hjust  = 0, check_overlap = F, size = 5) +
# geom_text(data = opptaksomrader_SOM_HF_H12_coords_left, aes(X, Y, label = NAVN_HF), colour = "black", 
#           hjust  = 1, 
#           # hjust  = 0, 
#           check_overlap = F, size = 5) +
# coord_sf(xlim=c(2, 17), ylim=c(57.5, 63), expand = TRUE) +

# if (utenhav == TRUE){
# Lagrer kartet som .png
png(filename = paste0("/home/jovyan/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_", aargang, ".png"), width = 2000, height = 2000)
HF_kart
dev.off()
#     }

HF_kart
# -

paste0("/home/jovyan/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_", aargang, ".png")

# # HF (per RHF)
#
# ## Helse Sør-Øst

# +
# opptaksomrader_SOM_RHF_H12 <- opptaksomrader_SOM_RHF %>%
# filter(NAVN_RHF == "HELSE SØR-ØST RHF")

# ggplot() + 
# geom_sf(data = opptaksomrader_SOM_RHF_H12)

# OBS:
opptaksomrader_SOM_HF_H12 <- opptaksomrader_SOM_HF %>%
# filter(NAVN_HF %in% c("VESTRE VIKEN HF", "DIAKONHJEMMET SYKEHUS AS", "SYKEHUSET TELEMARK HF", "SØRLANDET SYKEHUS HF", "SYKEHUSET INNLANDET HF", 
# "AKERSHUS UNIVERSITETSSYKEHUS HF", "LOVISENBERG DIAKONALE SYKEHUS AS", "OSLO UNIVERSITETSSYKEHUS HF", "SYKEHUSET I VESTFOLD HF", "SYKEHUSET ØSTFOLD HF")) %>%
filter(NAVN_RHF == "HELSE SØR-ØST RHF") %>%
# dplyr::mutate(NAVN_HF = case_when(NAVN_HF == "DIAKONHJEMMET SYKEHUS AS" ~ "DIAKONHJEMMET SYKEHUS", 
#                                  NAVN_HF == "LOVISENBERG DIAKONALE SYKEHUS AS" ~ "LOVISENBERG DIAKONALE SYKEHUS", 
#                                  NAVN_HF == "DIAKONHJEMMET SYKEHUS AS" ~ "DIAKONHJEMMET SYKEHUS", TRUE ~ NAVN_HF)) %>%
dplyr::mutate(NAVN_HF = gsub(" AS", "", NAVN_HF)) %>%
dplyr::mutate(NAVN_HF = gsub(" HF", "", NAVN_HF)) %>%
sf::st_transform(crs = 4326)

# -


# ### HF uten navn

# +
HSØ_uten_navn <- ggplot() + 
geom_sf(data = opptaksomrader_SOM_HF_H12, fill = ssb_farger_blaa$HEX, color = "white", lwd = 1) +
theme_void()

# if (utenhav == TRUE){
# Lagrer kartet som .png
png(filename = paste0("/home/jovyan/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_HSØ_", aargang, "_uten_navn.png"), width = 2000, height = 2000)
HSØ_uten_navn
dev.off()
   #  }
# -

# ### HF med navn

# +
# X_left <- 4.3
X_left <- 6.3
X_right <- 13

opptaksomrader_SOM_HF_H12_points <- sf::st_point_on_surface(opptaksomrader_SOM_HF_H12) %>%
sf::st_coordinates(NAVN_HF = case_when()) %>%
data.frame()

opptaksomrader_SOM_HF_H12_df <- opptaksomrader_SOM_HF_H12 %>% data.frame() %>% select(NAVN_HF)
opptaksomrader_SOM_HF_H12_points <- cbind(opptaksomrader_SOM_HF_H12_df, opptaksomrader_SOM_HF_H12_points) %>%
mutate(Y = case_when(NAVN_HF == "AKERSHUS UNIVERSITETSSYKEHUS" ~ Y+0.2, 
      NAVN_HF == "SYKEHUSET I VESTFOLD" ~ Y-0.2, TRUE ~ Y))

# opptaksomrader_SOM_HF_H12_coords <- as.data.frame(sf::st_coordinates(opptaksomrader_SOM_HF_H12_points))
opptaksomrader_SOM_HF_H12_coords <- opptaksomrader_SOM_HF_H12_points
opptaksomrader_SOM_HF_H12_coords$NAVN_HF <- opptaksomrader_SOM_HF_H12$NAVN_HF

opptaksomrader_SOM_HF_H12_coords <- opptaksomrader_SOM_HF_H12_coords # %>%
# mutate(Y = case_when(NAVN_HF == "AKERSHUS UNIVERSITETSSYKEHUS HF" ~ Y+0.2, TRUE ~ Y))

# opptaksomrader_SOM_HF_H12_coords <- opptaksomrader_SOM_HF_H12_coords %>%
# dplyr::mutate(X = 13) %>%
# dplyr::mutate(X = case_when(NAME %in% c("VESTRE VIKEN HF", "DIAKONHJEMMET SYKEHUS AS", "SYKEHUSET TELEMARK HF", "SØRLANDET SYKEHUS HF", "SYKEHUSET INNLANDET HF") ~ 4.5, TRUE ~ X))

opptaksomrader_SOM_HF_H12_coords_left <- opptaksomrader_SOM_HF_H12_coords %>%
filter(NAVN_HF %in% c("VESTRE VIKEN", "DIAKONHJEMMET SYKEHUS", "SYKEHUSET TELEMARK", "SØRLANDET SYKEHUS", "SYKEHUSET INNLANDET")) %>%
# dplyr::mutate(X = 6.3)
dplyr::mutate(X = X_left)

opptaksomrader_SOM_HF_H12_coords_right <- opptaksomrader_SOM_HF_H12_coords %>%
filter(!NAVN_HF %in% c("VESTRE VIKEN", "DIAKONHJEMMET SYKEHUS", "SYKEHUSET TELEMARK", "SØRLANDET SYKEHUS", "SYKEHUSET INNLANDET")) %>%
dplyr::mutate(X = X_right)

opptaksomrader_SOM_HF_H12_coords_both <- rbind(opptaksomrader_SOM_HF_H12_coords_left, opptaksomrader_SOM_HF_H12_coords_right) # %>%

point_map <- opptaksomrader_SOM_HF_H12_points %>%
sf::st_as_sf(coords = c("X", "Y")) %>%
select(NAVN_HF, geometry) %>%
st_set_crs(4326)
# data.frame()

# ggplot() + 
# geom_sf(data = point_map)

point_name <- opptaksomrader_SOM_HF_H12_coords_both %>%
mutate(X = case_when(X == X_left ~ X_left+0.1, 
                     X == X_right ~ X_right-0.1, 
                     TRUE ~ X)) %>%
# dplyr::rename(NAVN_HF = NAME) %>%
sf::st_as_sf(coords = c("X", "Y")) %>%
select(NAVN_HF, geometry) %>%
st_set_crs(4326)
# data.frame()

# ggplot() + 
# geom_sf(data = point_map, color = "red") +
# geom_sf(data = point_name)


point_both <- rbind(point_map, point_name) %>%
group_by(NAVN_HF) %>%
summarise(do_union=F) %>% st_cast("LINESTRING")

sf_use_s2(FALSE)
options(repr.plot.width=20, repr.plot.height=20)

main_map <- ggplot() + 
geom_sf(data = opptaksomrader_SOM_HF_H12, fill = ssb_farger_blaa$HEX, color = "white", lwd = 1) +
geom_sf(data = point_both, color = "black", lwd = 0.1) +
# coord_sf(expand = FALSE) +
geom_point(data = opptaksomrader_SOM_HF_H12_coords, aes(x = X, y = Y), colour = "white", size = 2) +
geom_text(data = opptaksomrader_SOM_HF_H12_coords_right, aes(X, Y, label = NAVN_HF), colour = "black", hjust  = 0, check_overlap = F, size = 5) +
geom_text(data = opptaksomrader_SOM_HF_H12_coords_left, aes(X, Y, label = NAVN_HF), colour = "black", 
          hjust  = 1, 
          # hjust  = 0, 
          check_overlap = F, size = 5) +
coord_sf(xlim=c(2, 17), ylim=c(57.5, 63), expand = TRUE) +
theme_void()

main_map

# if (utenhav == TRUE){
# Lagrer kartet som .png
png(filename = paste0("/home/jovyan/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_HSØ_", aargang, "_med_navn.png"), width = 2000, height = 2000)
main_map
dev.off()
   #  }
# -

# ### Legger til inset plot med Oslo
#
# https://upgo.lab.mcgill.ca/2019/12/13/making-beautiful-maps/

# +
# library(cowplot)

opptaksomrader_SOM_HF_H12_oslo <- opptaksomrader_SOM_HF %>%
dplyr::mutate(NAVN_HF = gsub(" AS", "", NAVN_HF)) %>%
dplyr::mutate(NAVN_HF = gsub(" HF", "", NAVN_HF)) %>%
filter(NAVN_HF %in% c("DIAKONHJEMMET SYKEHUS", "OSLO UNIVERSITETSSYKEHUS", "LOVISENBERG DIAKONALE SYKEHUS", "AKERSHUS UNIVERSITETSSYKEHUS")) %>%
# filter(NAVN_RHF == "HELSE SØR-ØST RHF") %>%
sf::st_transform(crs = 4326) %>%
sf::st_intersection(kommunekart_oslo)

# # kommunekart_oslo

# test <- sf::st_intersection(opptaksomrader_SOM_HF_H12_oslo, kommunekart_oslo)

# # test
# ggplot() + 
# geom_sf(data = test)

oslo_map <- ggplot() + 
geom_sf(data = opptaksomrader_SOM_HF_H12_oslo, aes(fill = NAVN_HF)) +
scale_fill_manual(values = c(ssb_farger_oslo$HEX[1],  # Ahus
                                ssb_farger_oslo$HEX[2], # B
                                ssb_farger_oslo$HEX[3], 
                                ssb_farger_oslo$HEX[4])) +
# coord_sf(xlim=c(2, 17), ylim=c(57.5, 63), expand = TRUE) +
theme_void() +

# oslo_map <- oslo_map +
# coord_sf(xlim=oslo_map$coordinates$limits$x, ylim=oslo_map$coordinates$limits$y, expand = TRUE) +

coord_sf(xlim=c(10.45, 11), ylim=c(59.8, 60.15), expand = TRUE) +
theme(panel.border = element_rect(colour = "red", fill=NA, size=3), 
     legend.title=element_blank()) # , 
     # legend.key.size = unit(3, 'cm'))

# oslo_map

options(repr.plot.width=30, repr.plot.height=20)

main_map_2 <-  main_map +
geom_rect(aes(xmin = min(oslo_map$coordinates$limits$x), xmax = max(oslo_map$coordinates$limits$x), ymin = min(oslo_map$coordinates$limits$y), ymax = max(oslo_map$coordinates$limits$y)), color = "red", fill = NA, size = 1) +
coord_sf(xlim=c(2, 25), ylim=c(57.5, 63), expand = TRUE)

helse_sor_ost_med_oslo <- main_map_2 %>% 
  ggdraw() +
  draw_plot(
    {
      oslo_map + # main_map_2 +
        coord_sf(
        # xlim = sf::st_bbox(opptaksomrader_SOM_HF_H12_oslo)[c(1,3)],
        # ylim = sf::st_bbox(opptaksomrader_SOM_HF_H12_oslo)[c(2,4)],
            # xlim = oslo_map$coordinates$limits$x,
            # ylim = c(min(oslo_map$coordinates$limits$y), max(oslo_map$coordinates$limits$y)),
            xlim = c(min(oslo_map$coordinates$limits$x),  max(oslo_map$coordinates$limits$x)),
            ylim = c(min(oslo_map$coordinates$limits$y),  max(oslo_map$coordinates$limits$y)),
          expand = FALSE) +
        # theme(legend.position = "left")
    theme(legend.position = "right")

      },
    # x = 0.72, 
          x = 0.5, 
    y = 0.63,
    width = 0.3, 
    height = 0.3)

helse_sor_ost_med_oslo

# if (utenhav == TRUE){
# Lagrer kartet som .png
png(filename = paste0("/home/jovyan/speshelse/Opptaksområder/images/opptaksområde_SOM_HF_HSØ_", aargang, "_med_Oslo.png"), width = 2000, height = 2000*0.7)
helse_sor_ost_med_oslo
dev.off()
#     }
