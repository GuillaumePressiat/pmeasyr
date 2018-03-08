## ----eval = F------------------------------------------------------------
#  library(pmeasyr)
#  library(dplyr, warn.conflicts = F)
#  library(dbplyr)
#  library(DBI)

## ----eval = F------------------------------------------------------------
#  dbdir <- "~/Documents/data/monetdb"
#  con <- src_monetdblite(dbdir)

## ----eval = F------------------------------------------------------------
#  dbdir <- "~/Documents/data/sqlite/pmsi.sqlite"
#  con <- src_sqlite(dbdir)

## ----eval = F------------------------------------------------------------
#  con <- src_postgres(user = "gui", password = "gui", dbname = "aphp",
#                      host = "localhost", port = 5432)

## ---- eval = F-----------------------------------------------------------
#  # noyau_skeleton()
#  p <- noyau_pmeasyr(
#    finess   = '750712184',
#    annee = 2016,
#    mois     = 12,
#    path     = '~/Documents/data/mco',
#    progress = F,
#    tolower_names = T, # choix de noms de colonnes minuscules : T / F
#    n_max = 1e4, # on limite la lecture a un petit nombre de lignes pour tester d'abord
#    lib = F)

## ---- eval = F-----------------------------------------------------------
#  # Tables mco in 2011
#  purrr::quietly(db_mco_in)(con,  p, annee = 2011) -> ok # on analyse l'objet ok ensuite : ok ?
#  # ... 2012 -- 2015
#  purrr::quietly(db_mco_in)(con,  p, annee = 2016) -> ok # on analyse l'objet ok ensuite : ..
#  
#  # Tables mco out 2016
#  purrr::quietly(db_mco_out)(con,  p, annee = 2016) -> ok # on analyse l'objet ok ensuite : ..
#  # ...

## ---- eval = F-----------------------------------------------------------
#  
#  
#  p$path <- "~/Documents/data/rsf"
#  
#  # Tables rsf out 2016
#  purrr::quietly(db_rsf_out)(con,  p, annee = 2016) -> ok # on analyse l'objet ok ensuite : ..

## ---- eval = F-----------------------------------------------------------
#  # Exemple en had
#  p$path <- "~/Documents/data/had"
#  
#  irapss(p, annee = 2015) -> tables_had
#  
#  # Table had rapss 2015
#  purrr::quietly(db_generique)(con, an = 15, table = tables_had$rapss, prefix = 'had', suffix =  'rapss_rapss') -> ok
#  # had_15_rapss_rapss

## ---- eval = F-----------------------------------------------------------
#  con <- DBI::dbConnect(odbc::odbc(), "PostgreSQL")
#  con <- dbplyr::src_dbi(con)

