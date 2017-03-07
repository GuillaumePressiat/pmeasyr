## ----eval = F------------------------------------------------------------
#  library(pmeasyr)
#  library(dplyr)

## ----eval = F------------------------------------------------------------
#  p <- noyau_pmeasyr(
#          finess   = '750100042',
#          annee    = 2015,
#          mois     = 12,
#          path     = '~/Documents/data/mco',
#          progress = F)

## ----eval = F------------------------------------------------------------
#  # Ajouter des paramètres au noyau de parametres façon pipe
#  `%P%` <- function(p, x){c(p,x)}
#  
#  # Tout dezipper
#  # out
#  p %P% c(type = "out") %>% adezip()
#  # in
#  p %P% c(type = "in" ) %>% adezip()

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
#  # in
#  p %>% irum()                            -> rum
#  p %P% c(typano  = "in") %>% iano_mco()  -> ano_in
#  p %P% c(typmed  = "in") %>% imed_mco()  -> med_in
#  p %P% c(typdmi  = "in") %>% idmi_mco()  -> dmi_in
#  p %P% c(typdiap = "in") %>% idiap()     -> diap_in
#  p %P% c(typpo   = "in") %>% ipo()       -> po_in

## ----eval = F------------------------------------------------------------
#  # En plus dense :
#  # On liste les fonctions MCO du package :
#  fout <- c('irsa', 'iano_mco', 'iium', 'idiap', 'imed_mco', 'idmi_mco', 'ipo', 'ileg_mco', 'itra')
#  
#  sapply(fout, function(x)get(x)(p)) -> liste_tables_mco_out
#  names(liste_tables_mco_out)
#  # enlever les i des noms des tables
#  names(liste_tables_mco_out) <- substr(names(liste_tables_mco_out),2, nchar(names(liste_tables_mco_out)))

## ----eval = F------------------------------------------------------------
#  # Coller des chaines de caracteres faon pipe
#  `%+%` <- function(x,y){paste0(x,y)}
#  
#  dir.create('~/Documents/data/mco/tables')
#  nom <- p$finess %+% '.' %+% p$annee %+% '.' %+% p$mois %+% '.' %+% 'out' %+% '.' %+% 'rds'
#  saveRDS(liste_tables_mco_out, '~/Documents/data/mco/tables/' %+% nom)
#  

## ----eval = F------------------------------------------------------------
#  # Tout effacer sauf les zip :
#  p %>%  adelete()

## ----eval = F------------------------------------------------------------
#  # Coller des chaines de caracteres faon pipe
#  `%+%` <- function(x,y){paste0(x,y)}
#  
#  # Le fichier se nomme : 750100042.2015.12.out.rds
#  nom <- p$finess %+% '.' %+% p$annee %+% '.' %+% p$mois %+% '.' %+% 'out' %+% '.' %+% 'rds'
#  readRDS('~/Documents/data/mco/tables/' %+% nom) -> mydata
#  
#  View(mydata$rsa$rsa)
#  View(mydata$rsa$actes)
#  
#  View(mydata$leg_mco)

