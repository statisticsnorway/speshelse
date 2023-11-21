# -*- coding: utf-8 -*-
# # Nøkkeltall og hovedtallstabell for Spesialisthelsetjenesten

# ### Laster inn pakker og funksjoner

# +
# OBS: slett når funksjonene finnes i fellesr
source("/ssb/bruker/rdn/hack4ssb-tabellbygger/R/funksjoner_hack.R") 
source("/ssb/bruker/rdn/hack4ssb-tabellbygger/R/tabellbygger_funksjon.R")

suppressPackageStartupMessages({
  suppressWarnings(library(tidyverse))
    library(fellesr)
    })
# -


# ### Oppretter TBML-ID

# +
# tbml_create_id(kortnavn = "speshelse", 
#               tittel = list("Spesialisthelsetjenesten, nøkkeltall" = "Specialist health service, key figure"))
# -

# ## Nøkkeltall

tabellbygger(id = "299389", 
             sporring = "ID:14022,HelseReg:H00,HelseTjenomr:TOT,InntektKostnad:K00,ContentsCode:LopendePriser", 
             rad_navn = list("Driftskostnader" = "Expenses"), 
             kategori = "N")

# ## Hovedtallstabell

tabellbygger(id = "299365",
             tittel = list("Spesialisthelsetjenesten, hovedtall" = "Specialist health service, main figures"),
             sporring = c("",
                           "ID:14022,HelseReg:H00,HelseTjenomr:TOT,InntektKostnad:K00,ContentsCode:LopendePriser",
                           "ID:13942,HelseReg:H00,HelseTjenomr:TOT,ContentsCode:Dognplass",
                           "ID:13942,HelseReg:H00,HelseTjenomr:TOT,ContentsCode:Utskriv",
                           "ID:13942,HelseReg:H00,HelseTjenomr:TOT,ContentsCode:Liggedag",
                           "ID:13942,HelseReg:H00,HelseTjenomr:TOT,ContentsCode:Polikliniske",
                           "ID:13942,HelseReg:H00,HelseTjenomr:TOT,ContentsCode:Dag",
                           "ID:13954,HelseReg:H00,HelseTjenomr:TOT,Utdanning:TOT,ContentsCode:Arsverk"),
             tid = c("1",
                     "0",
                     "P(T2-T1)",
                     "P(T1-T0)"),
             rad_navn = list("Spesialisthelsetjenesten" = "Specialist health service",
                             "Driftskostnader (mill.kr)" = "Expenses (NOK million)",
                             "Senger/Døgnplasser" = "Beds",
                             NULL,
                             NULL,
                             NULL,
                             NULL,
                             "Årsverk" = "Man-years"),
             rad_fet_skrift = c(1), 
             rad_tittel = c(1),
             kolonne_overskrift = list(NULL = NULL,
                                       NULL = NULL,
                                       "Endring i prosent" = "Change in percent",
                                       "Endring i prosent" = "Change in percent"),
             fotnote = list(NULL,
                            "Tallene som oppgis er i løpende priser. Driftskostnader for 2022 ble rettet 23. juni 2023 kl. 08:00." = "The figures are in current prices. Expenses for 2022 was corrected on 23 June 2023 at 08:00.",
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL))
