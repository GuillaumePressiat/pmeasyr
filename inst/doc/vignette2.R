## ----eval = F------------------------------------------------------------
#  library(pmeasyr)
#  library(dplyr, warn.conflicts = F)

## ----eval = F------------------------------------------------------------
#  p <- noyau_pmeasyr(
#          finess   = '750100042',
#          annee    = 2015,
#          mois     = 12,
#          path     = '~/Documents/data/mco',
#          progress = F)

## ----eval = F------------------------------------------------------------
#  # Tout dezipper
#  # out
#  p %>% adezip(type = "out")
#  # in
#  p %>% adezip(type = "in")

## ----eval = F------------------------------------------------------------
#  
#  # out
#  p %>% irsa()     -> rsa
#  p %>% iano_mco() -> ano_out
#  p %>% iium()     -> ium
#  p %>% idiap()    -> diap_out
#  p %>% imed_mco() -> med_out
#  p %>% idmi_mco() -> dmi_out
#  p %>% ipo()      -> po_out
#  p %>% ileg_mco() -> leg
#  p %>% itra()     -> tra
#  
#  # rsa type 6 :
#  p %>% irsa(typi = 6) -> rsa
#  # rsa d'une autre annee :
#  p %>% irsa(annee = 2016) -> rsa
#  # rsa d'une autre annee, lire les dix premiers rsa :
#  p %>% irsa(annee = 2016, n_max = 10) -> rsa
#  
#  # in
#  p %>% irum()                    -> rum
#  p %>% iano_mco(typano  = "in")  -> ano_in
#  p %>% imed_mco(typmed  = "in")  -> med_in
#  p %>% idmi_mco(typdmi  = "in")  -> dmi_in
#  p %>% idiap(typdiap = "in")     -> diap_in
#  p %>% ipo(typpo   = "in")       -> po_in

## ----eval = F------------------------------------------------------------
#  rsa_2011  rsa_2012  rsa_2013  rsa_2014  rsa_2015

## ----eval = F------------------------------------------------------------
#  p <- noyau_pmeasyr(
#    finess = '750100042',
#    mois   = 12,
#    path = "~/Documents/data/mco",
#    progress = F
#  )
#  
#  for (i in 2011:2015){
#    p %>% adezip(annee = i, type = "out", liste = "rsa")
#    p %>% irsa(annee = i) -> temp
#    assign(paste("rsa", i, sep = "_"), temp)
#  }
#  

## ----eval = F------------------------------------------------------------
#  # On liste les fonctions MCO du package :
#  fout <- c('irsa', 'iano_mco', 'iium', 'idiap', 'imed_mco', 'idmi_mco', 'ipo', 'ileg_mco', 'itra')
#  
#  sapply(fout, function(x)get(x)(p)) -> liste_tables_mco_out
#  names(liste_tables_mco_out)
#  # enlever les i des noms des tables
#  names(liste_tables_mco_out) <- substr(names(liste_tables_mco_out),2, nchar(names(liste_tables_mco_out)))

## ----eval = F------------------------------------------------------------
#  rsa ano_mco ium diap med_mco dmi_mco po leg_mco tra

## ----eval = F------------------------------------------------------------
#  # Coller des chaines de caracteres facon pipe
#  `%+%` <- function(x,y){paste0(x,y)}
#  
#  dir.create(p$path %+% '/tables')

## ----eval = F------------------------------------------------------------
#  '~/Documents/data/mco/tables/'

## ----eval = F------------------------------------------------------------
#  nom <- p$finess %+% '.' %+% p$annee %+% '.' %+% p$mois %+% '.' %+% 'out' %+% '.' %+% 'rds'
#  saveRDS(liste_tables_mco_out, p$path %+% '/tables/' %+% nom)

## ----eval = F------------------------------------------------------------
#  750100042.2015.12.out.rds

## ----eval = F------------------------------------------------------------
#  # Tout effacer sauf les zip :
#  p %>%  adelete()

## ----eval = F------------------------------------------------------------
#  # Coller des chaines de caracteres faon pipe
#  `%+%` <- function(x,y){paste0(x,y)}
#  
#  # Le fichier se nomme : 750100042.2015.12.out.Rds
#  nom <- p$finess %+% '.' %+% p$annee %+% '.' %+% p$mois %+% '.' %+% 'out' %+% '.' %+% 'Rds'
#  readRDS(p$path %+% '/tables/' %+% nom) -> mydata
#  
#  View(mydata$rsa$rsa)
#  View(mydata$rsa$actes)
#  
#  View(mydata$leg_mco)

