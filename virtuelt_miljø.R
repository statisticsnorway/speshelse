
getwd()

renv::init(project = "/ssb/bruker/rdn/speshelse")

renv::install("devtools")
library(devtools)

devtools::install_github("statisticsnorway/fellesr", auth_token = getPass::getPass("PAT: "))
library(fellesr)

renv::install("arrow")
library(arrow)

renv::install("png")
library(png)


renv::snapshot()
