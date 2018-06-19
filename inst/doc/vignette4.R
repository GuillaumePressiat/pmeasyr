## ----eval = F------------------------------------------------------------
#  library(pmeasyr)
#  library(dplyr, warn.conflicts = F)

## ----eval = F------------------------------------------------------------
#  library(pmeasyr)
#  
#  noyau_pmeasyr(
#    finess = '750100042',
#    annee  = 2016,
#    mois   = 12,
#    path   = '~/Documents/data/mco',
#    progress = FALSE,
#    lib = FALSE,
#    tolower_names = TRUE
#    ) -> p
#  
#  adezip(p, type = "out")
#  
#  vrsa <- vvr_rsa(p)
#  vano <- vvr_ano_mco(p)
#  
#  

## ----eval = F------------------------------------------------------------
#  
#  library(MonetDBLite)
#  dbdir <- "~/Documents/data/monetdb"
#  con <- src_monetdblite(dbdir)
#  
#  vrsa <- vvr_rsa(con, 16)
#  vano <- vvr_ano_mco(con,  16)
#  

## ----eval = F------------------------------------------------------------
#  # devtools::install_github('GuillaumePressiat/nomensland')
#  library(nomensland)

## ----eval = F------------------------------------------------------------
#  # Importer les tarifs GHS
#  tarifs_ghs <- dplyr::distinct(get_table('tarifs_mco_ghs'), ghs, anseqta, .keep_all = TRUE)
#  
#  resu <- vvr_mco(
#    vvr_ghs_supp(rsa = vrsa, ano =  vano, tarifs = tarifs_ghs),
#    vvr_mco_sv(vrsa, vano)
#  )

## ----eval = F------------------------------------------------------------
#  # Importer les tarifs des suppléments
#  tarifs_supp <- get_table('tarifs_mco_supplements') %>% mutate_if(is.numeric, tidyr::replace_na, 0)
#  
#  resu <- vvr_mco(
#  vvr_ghs_supp(rsa = vrsa,
#               tarifs = tarifs_ghs,
#               supplements =  tarifs_supp,
#               ano = vano,
#               porg = ipo(p),
#               diap = idiap(p),
#               pie = ipie(p),
#               full = FALSE,
#               cgeo = 1.07,
#               prudent = NULL,
#               bee = FALSE),
#  vvr_mco_sv(vrsa, vano, ipo(p))
#  )

## ----eval = TRUE---------------------------------------------------------
knitr::kable(pmeasyr::vvr_libelles_valo('lib_type_sej'))

## ----eval = TRUE---------------------------------------------------------
knitr::kable(pmeasyr::vvr_libelles_valo('lib_valo'))

## ----eval = TRUE---------------------------------------------------------
knitr::kable(pmeasyr::vvr_libelles_valo('lib_vidhosp'))

## ----eval = F------------------------------------------------------------
#  epmsi_mco_sv(resu)

## ----eval = F------------------------------------------------------------
#  epmsi_mco_rav(resu)

## ----eval = F------------------------------------------------------------
#  resu <- vvr_mco(
#  vvr_ghs_supp(rsa = vrsa,
#               tarifs = tarifs_ghs,
#               supplements =  tarifs_supp,
#               ano = vano,
#               porg = ipo(p),
#               diap = idiap(p),
#               pie = ipie(p),
#               full = TRUE,
#               cgeo = 1.07,
#               prudent = NULL,
#               bee = FALSE),
#  vvr_mco_sv(vrsa, vano, ipo(p))
#  )

## ----eval = F------------------------------------------------------------
#  resu %>%
#    summarise_at(vars(starts_with('rec_')), sum)  %>%
#    names() %>%
#    cat(sep = "\n")

