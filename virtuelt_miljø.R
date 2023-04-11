
getwd()

renv::init(project = "/ssb/bruker/rdn/speshelse")

# renv::install("tidyverse")
library(tidyverse)

# devtools::install_github("statisticsnorway/fellesr", auth_token = getPass::getPass("PAT: "))
library(fellesr)

Sys.time()

renv::install("arrow")
library(arrow)



renv::install("png")
library(png)


renv::snapshot()
