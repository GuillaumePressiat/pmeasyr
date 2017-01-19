## ----eval = F------------------------------------------------------------
#  library(pmeasyr)
#  # Dezippage de tous les fichiers du in 2015
#  # Ex : 750100042.2015.12.20160130.153012.in.zip
#  adezip(finess = 750100042,
#         annee = 2015,
#         mois = 12,
#         path = '~/Documents/data/mco',
#         liste = "",
#         type = "in")
#  
#  # Dezippage uniquement des fichiers rsa, ano et tra du out 2015
#  # Ex: 750100042.2015.12.20160130.153012.out.zip
#  adezip(finess = 750100042,
#         annee = 2015,
#         mois = 12,
#         path = '~/Documents/data/mco',
#         liste = c("rsa", "ano", "tra"),
#         type = "out")

## ----eval = F------------------------------------------------------------
#  # Effacer les fichiers
#  adelete(finess = 750100042,
#          annee = 2015,
#          mois = 12,
#          path = '~/Documents/data/mco',
#          liste = c("rsa", "ano"),
#          type = "out")

## ----eval = F------------------------------------------------------------
#  # Informations sur les fichiers : Date de creation, Taille
#  astat(path = '~/Documents/data/mco/',
#        file = '750100042.2015.12.29012016174032.out.zip',
#        view = F)

## ----eval = F------------------------------------------------------------
#  # Import des rsa 2015
#  irsa(finess = 750100042,
#       annee = 2015,
#       mois = 12,
#       path = '~/Documents/data/mco')

## ----eval = F------------------------------------------------------------
#  # Import des rsa 2015 type 6
#  irsa(finess = 750100042,
#       annee = 2015,
#       mois = 12,
#       path = '~/Documents/data/mco',
#       typi = 6) -> rsa15
#  View(rsa15$rsa)
#  View(rsa15$rsa_um)
#  View(rsa15$actes)
#  View(rsa15$das)

## ----eval = F------------------------------------------------------------
#  # Import des rsa 2015
#  irum(finess = 750100042, annee = 2015, mois = 12, path = '~/Documents/data/mco')

## ----eval = F------------------------------------------------------------
#  # lecture du fichier tra et jointure aux rsa
#  itra(750100042, 2015, 12, '~/Documents/data/mco') -> tra
#  # Ajout du tra aux rsa :
#  inner_tra(rsa15$rsa, tra) -> rsa15$rsa

## ----eval = F------------------------------------------------------------
#  # Obtenir les noms, labels et type de variable (character, numeric, integer, date, ...)
#  dico(rsa15$rsa)

## ----eval = F------------------------------------------------------------
#  # Pour les objets rsa et rum du MCO
#  # Transbahuter tous les diagnostics dans une seule table
#  tdiag(rsa15, "rsa") -> rsa15
#  View(rsa15$diags)
#  # Tous les diagnostics sont dans une table, avec un numero selon leur position
#  # 1:DP, 2:DR, 3:DPUM, 4:DRUM, 5:DAS

## ----eval = F------------------------------------------------------------
#  # En utilisant le package dplyr
#  library(dplyr)
#  # acte EBLA003
#  rsa15$rsa %>% filter(grepl('EBLA003', actes)) %>% nrow()
#  
#  # actes EBLA001 ou EBLA002 ou EBLA003
#  rsa15$rsa %>% filter(grepl('EBLA', actes)) %>% nrow()
#  
#  # actes EBLA003 ou EPLF002
#  rsa15$rsa %>% filter(grepl('EBLA003|EPLF002', actes)) %>% nrow()
#  
#  # directement dans la table actes
#  rsa15$actes %>% filter(CDCCAM %in% c('EBLA003', 'EPLF002'))  %>%
#    .$CLE_RSA %>% unique() %>% length()

## ----eval = F------------------------------------------------------------
#  # Diagnostic categorie cancer du colon en toutes positions
#  rsa15$rsa %>% filter(grepl('C18', dpdrum)|grepl('C18', das)) %>% nrow()
#  
#  # Autrement apres avoir utilise tdiag() :
#  rsa15$diags %>% filter(grepl('C18', diag)) %>%
#    .$CLE_RSA %>% unique() %>% length()
#  
#  # En restreignant au dp dr du sejour (avec la variable position)
#  rsa15$diags %>% filter(grepl('C18', diag), position < 3) %>%
#    .$CLE_RSA %>% unique() %>% length()

## ----eval = F------------------------------------------------------------
#  library(pmeasyr)
#  ?irsa
#  ?irum
#  
#  ?itra
#  ?inner_tra

## ----eval = F------------------------------------------------------------
#  # Charger les formats des donnees
#  pmeasyr::formats

