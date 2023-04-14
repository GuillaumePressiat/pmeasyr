


#' ~ VVR - preparer les rsa pour la valorisation
#'
#' Importer ou collecter les variables des rsa nécessaires à leur valorisation GHS + suppléments
#'
#' 
#' Deux méthodes sont disponibles : une utilisant l'import avec un noyau pmeasyr (p), l'autre utilisant les rsa stockés
#' dans une base de données (con)
#' 
#' 
#' @param p Un noyau de paramètres \code{\link{noyau_pmeasyr}}
#' @param con Une connexion vers une db contenant les données PMSI
#' @param annee Dans le cas d'une con db, préciser l'année en integer sur deux caractères
#' 
#' @return Un tibble contenant les variables des rsa nécessaires pour calculer les recettes ghs et suppléments
#'
#' @examples
#' \dontrun{
#' # avec un noyau pmeasyr (importer les données)
#' annee <- 18
#' p <- noyau_pmeasyr(
#'   finess   = '750712184',
#'   annee    = 2000 + annee,
#'   mois     = 4,
#'   path     = '~/Documents/data/mco',
#'   progress = FALSE,
#'   n_max    = Inf,
#'   lib      = FALSE,
#'   tolower_names = TRUE)
#' 
#' vrsa <- vvr_rsa(p)
#' 
#' # depuis une base de données (collecter les données)
#' dbdir <- "~/Documents/data/monetdb"
#' con <- src_monetdblite(dbdir)
#' 
#' vrsa <- vvr_rsa(con, annee)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{epmsi_mco_sv}}, \code{\link{vvr_ano_mco}}, \code{\link{vvr_mco}}
#' @export vvr_rsa
#' @export
vvr_rsa <- function(...){
  UseMethod('vvr_rsa')
}

#' @export
vvr_rsa.pm_param <- function(p, ...){
  
  new_par <- list(..., typi = 4)
  p2 <- utils::modifyList(p, new_par)
  
  rsa <- pmeasyr::irsa(p2)
  rsa$rsa %>% 
    dplyr::select_all(tolower) %>% 
    dplyr::select(cle_rsa, duree, rsacmd, ghm, typesej, noghs, moissor, ansor, sexe, agean, agejr,
                  anseqta, nbjrbs, nbjrexb, sejinfbi, agean, agejr, nbseance,
                  dplyr::starts_with('nbsup'), dplyr::starts_with('sup'),
                  nb_rdth, nbacte9615,
                  echpmsi, prov, dest, schpmsi,
                  rdth, nb_rdth, nbrum) %>%
    dplyr::mutate(nbseance = dplyr::case_when(
      rsacmd == '28' & ! ghm %in% c('28Z19Z', '28Z20Z', '28Z21Z', '28Z22Z') ~ nbseance, 
      ghm %in% c('28Z19Z', '28Z20Z', '28Z21Z', '28Z22Z') ~ 1L, 
      TRUE ~ 1L
    )) %>% 
    dplyr::left_join(rsa$rsa_um %>% 
                       dplyr::select_all(tolower) %>% 
                       dplyr::filter(substr(typaut1, 1, 2) == '07') %>% 
                       dplyr::distinct(cle_rsa) %>%
                       dplyr::mutate(uhcd = 1), by = 'cle_rsa') %>%
    dplyr::mutate(monorum_uhcd = (uhcd == 1 & nbrum == 1)) %>%
    dplyr::select(-uhcd)
  
}

#' @export
vvr_rsa.src <- function(con, an,  ...){
  pmeasyr::tbl_mco(con, an, 'rsa_rsa')  %>% 
    dplyr::select_all(tolower) %>% 
    dplyr::select(cle_rsa, duree, rsacmd, ghm, typesej, noghs, moissor, ansor, sexe, agean, agejr,
                  anseqta, nbjrbs, nbjrexb, sejinfbi, agean, agejr, nbseance,
                  dplyr::starts_with('nbsup'), dplyr::starts_with('sup'),
                  nb_rdth, nbacte9615,
                  echpmsi, prov, dest, schpmsi,
                  rdth, nb_rdth, nbrum) %>% 
    dplyr::mutate(nbseance = dplyr::case_when(
      rsacmd == '28' & ! ghm %in% c('28Z19Z', '28Z20Z', '28Z21Z', '28Z22Z') ~ nbseance, 
      ghm %in% c('28Z19Z', '28Z20Z', '28Z21Z', '28Z22Z') ~ 1L, 
      TRUE ~ 1L
    )) %>% 
    dplyr::left_join(pmeasyr::tbl_mco(con, an, 'rsa_um') %>% 
                       dplyr::select_all(tolower) %>% 
                       dplyr::filter(substr(typaut1, 1, 2) == '07') %>% 
                       dplyr::distinct(cle_rsa) %>%
                       dplyr::mutate(uhcd = 1), by = 'cle_rsa') %>%
    dplyr::mutate(monorum_uhcd = (uhcd == 1 & nbrum == 1)) %>%
    dplyr::select(-uhcd) %>%
    dplyr::collect()
  
}

#' ~ VVR - Forcer le groupage des RSA hors période en erreur
#'
#' Pour les RSA transmis hors période, on modifie CMD, GHM, et GHS en 90, 90Z99Z, et 9999 pour ne pas les valoriser
#' @examples
#' \dontrun{
#' noyau_pmeasyr(
#'   finess = '290000017',
#'   annee  = 2019,
#'   mois   = 11,
#'   path   = '~/Documents/data/mco',
#'   progress = FALSE,
#'   lib = FALSE, 
#'   tolower_names = TRUE
#' ) -> p
#' 
#' vrsa <- vvr_rsa(p)
#' vrsa <- vrsa %>% 
#'   vvr_rsa_hors_periode(as.character(p$annee), stringr::str_pad(p$mois, 2, "left", '0'))
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{epmsi_mco_sv}}, \code{\link{vvr_rsa}}, \code{\link{vvr_mco}}
#' @export vvr_rsa_hors_periode
#' @export
vvr_rsa_hors_periode <- function(vrsa, an_v, mois_v){
  vrsa_ <- vrsa %>% 
    mutate(rsa_hors_periode = (ansor != an_v) | (moissor > mois_v),
           ghm = ifelse(rsa_hors_periode,    "90Z99Z", ghm),
           noghs = ifelse(rsa_hors_periode,  "9999",   noghs),
           rsacmd = ifelse(rsa_hors_periode, "90",     rsacmd))
  vrsa_
}




#' ~ VVR - preparer les ano pour valoriser les rsa
#'
#' Importer ou collecter les variables des anohosp out nécessaires pour attribuer le caractère facturable d'un séjour
#' 
#' Deux méthodes sont disponibles : une utilisant l'import avec un noyau pmeasyr (p), l'autre utilisant les rsa stockés
#' dans une base de données (con)
#' 
#' @param p Un noyau de paramètres \code{\link{noyau_pmeasyr}}
#' @param con Une connexion vers une db contenant les données PMSI
#' @param annee Dans le cas d'une con db, préciser l'année en integer sur deux caractères
#' @return Un tibble contenant les variables ano
#'
#' @examples
#' \dontrun{
#' # avec un noyau pmeasyr (importer les données)
#' p <- noyau_pmeasyr(
#'   finess   = '750712184',
#'   annee    = 2000 + 18,
#'   mois     = 4,
#'   path     = '~/Documents/data/mco',
#'   progress = FALSE,
#'   n_max    = Inf,
#'   lib      = FALSE,
#'   tolower_names = TRUE)
#' 
#' vano <- vvr_ano_mco(p)
#' 
#' # depuis une base de données (collecter les données)
#' dbdir <- "~/Documents/data/monetdb"
#' con <- src_monetdblite(dbdir)
#' 
#' vano <- vvr_ano_mco(con, 18)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{epmsi_mco_sv}}, \code{\link{vvr_rsa}}, \code{\link{vvr_mco}}
#' @export vvr_ano_mco
#' @export
vvr_ano_mco <- function(...){
  UseMethod('vvr_ano_mco')
}

#' @export
vvr_ano_mco.pm_param <- function(p, ...){
  new_par <- list(...)
  p2 <- utils::modifyList(p, new_par)
  iano_mco(p) %>% 
    dplyr::select_all(tolower)
}

#' @export
vvr_ano_mco.src <- function(con, an, ...){
  pmeasyr::tbl_mco(con, an, 'rsa_ano') %>% 
    dplyr::collect() %>% 
    dplyr::select_all(tolower)
}

#' ~ VVR - Attribuer les recettes GHS et suppléments sur des rsa
#'
#' Reproduire la valorisation BR et coefficients géo/prudentiels du tableau RAV d'epmsi, à partir des tables résultant des fonctions
#' \code{\link{vvr_rsa}}, \code{\link{vvr_ano_mco}}, et de tables contenant les fichcomp PO et DIAP, PIE et MO
#' 
#' Cette fonction ne tient pas compte de la rubrique "Minoration forfaitaire liste en sus, car elle a été supprimée en 2018
#' 
#' 
#' @param rsa Un tibble rsa partie fixe (créé avec \code{\link{vvr_rsa}})
#' @param tarifs Un tibble contenant une ligne par tarif GHS - année séquentielle des tarifs
#' @param supplements Un tibble contenant une ligne par année et une colonne par tarif de supplément
#' @param ano Un tibble, facultatif si `r bee = TRUE`, créé avec \code{\link{vvr_ano_mco}}
#' @param porg Un tibble contenant les prélévements d'organes du out (importés avec \code{\link{ipo}})
#' @param diap Un tibble contenant les dialyses péritonéales du out (importés avec \code{\link{idiap}})
#' @param pie Un tibble contenant les prestations inter-établissements du out (importés avec \code{\link{ipie}})
#' @param mo Un tibble contenant les molécules onéreuses du out (importés avec \code{\link{imed_mco}})
#' @param full Booléen, à `r TRUE` toutes les variables intermédiaires de valo sont gardées
#' @param cgeo Coefficient géographique, par défaut celui de l'Île-de-France (1.07)
#' @param prudent coefficient prudentiel, par défaut à `r NULL`, le coefficient prudentiel est appliqué par année séquentielles des tarifs
#' @param bee par défaut à `r TRUE`, seule la valorisation de GHS de base + extrême haut - extrême bas est calculée
#' 
#' @return Un tibble contenant les différentes rubriques de valorisation, une ligne par clé rsa
#'
#' @examples
#' \dontrun{
#' # Récupérer les tarifs GHS et des suppléments (ex-DGF) : 
#' tarifs      <- nomensland::get_table('tarifs_mco_ghs') %>% distinct(ghs, anseqta, .keep_all = TRUE)
#' supplements <- nomensland::get_table('tarifs_mco_supplements') %>% mutate_if(is.numeric, tidyr::replace_na, 0)
#' 
#' # Recette GHS de base et suppléments EXB, EXH
#' vvr_ghs_supp(rsa = vrsa, tarifs = tarifs)
#' 
#' # Recette GHS de base et suppléments EXB, EXH, et des suppléments
#' vvr_ghs_supp(vrsa, tarifs, supplements, vano, ipo(p), idiap(p), ipie(p), imed_mco(p), bee = FALSE)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{vvr_ano_mco}}, \code{\link{vvr_rsa}}, \code{\link{vvr_mco}}
#' @export vvr_ghs_supp
#' @export
vvr_ghs_supp <- function(rsa, 
                         tarifs, 
                         supplements = NULL, 
                         ano = NULL, 
                         porg = dplyr::tibble(), 
                         diap = dplyr::tibble(),  
                         pie  = dplyr::tibble(),
                         mo   = dplyr::tibble(),
                         full = FALSE, cgeo = 1.07, prudent = NULL,
                         bee = TRUE, csegur = 1.0019) {
  
  # Ajout de tarifs nuls pour le GHS 9999
  n_anseqta <- length(unique(rsa$anseqta))
  tarifs <- dplyr::bind_rows(tarifs,
                             dplyr::tibble(anseqta      = unique(rsa$anseqta),
                                           ghs          = rep('9999', n_anseqta),
                                           tarif_base   = rep(0, n_anseqta),
                                           tarif_exb    = rep(0, n_anseqta),
                                           tarif_exh    = rep(0, n_anseqta),
                                           forfait_exb  = rep(0, n_anseqta)))
  
  # Gestion des tibbles complémentaires (diap, po, pie, mo)
  
  if(nrow(pie) == 0) {
    pie = dplyr::tibble(cle_rsa = "", code_pie = "REP", nbsuppie = 0)
  }
  if(nrow(diap) == 0) {
    diap = dplyr::tibble(cle_rsa = "", nbsup = 0)
  }
  if(nrow(porg) == 0) {
    porg = dplyr::tibble(cle_rsa = "", cdpo = "")
  }
  if(nrow(mo) == 0) {
    mo = dplyr::tibble(cle_rsa = "", cducd = "")
  }
  
  # Correction GHS 5907 suite erreur dans tarifs atih entrainant une valo négative en cas de borne basse
  if ("2018" %in% unique(rsa$anseqta)){
    rsa <- rsa %>%
      dplyr::mutate(
        nbjrexb = ifelse(ghm == '15M06A' & noghs == '5907' & nbjrexb ==  30 & duree == 0 & anseqta == "2018", nbjrexb - 10, nbjrexb)
      ) %>%
      dplyr::mutate(
        nbjrexb = ifelse(ghm == '15M06A' & noghs == '5907' & nbjrexb ==  30 & duree == 1 & monorum_uhcd == 1 & anseqta == "2018", nbjrexb - 10, nbjrexb)
      )
  }
  
  # Switch de GHS si molécule Yescarta ou Kymriah (car-T cells)
  cart_cells <- mo %>%
    dplyr::filter(substr(cducd, 6, 12) %in% c('9439938', '9439921')) %>%
    dplyr::distinct(cle_rsa) %>%
    dplyr::mutate(switch_ghs = 1)
  
  rsa <- rsa %>%
    dplyr::left_join(cart_cells, by = 'cle_rsa') %>%
    dplyr::mutate(
      old_noghs = ifelse(!is.na(switch_ghs) & (moissor < "03" | anseqta == "2018"), noghs, ''),
      noghs = ifelse(!is.na(switch_ghs) & (moissor < "03" | anseqta == "2018"), '8973', noghs))
  
  
  if (is.null(prudent)) {
    rsa_2 <- rsa %>%
      dplyr::filter(substr(noghs,1,1) != 'I') %>%
      dplyr::mutate(cprudent = dplyr::case_when(anseqta == "2023" ~ 0.993 * 1.0023,
                                                anseqta == "2022" ~ 0.993 * 1.0013,
                                                anseqta == "2021" ~ 0.993 * 1.0019, 
                                                anseqta == "2020" ~ 0.993,
                                                anseqta == "2019" ~ 0.993, anseqta == "2018" ~ 0.993,
                                                anseqta == "2017" ~ 0.993, anseqta == "2016" ~ 0.995,
                                                anseqta == "2015" ~ 0.9965, anseqta == "2014" ~
                                                  0.9965, anseqta == "2013" ~ 0.9965, TRUE ~ 1)) %>%
      dplyr::left_join(tarifs %>% dplyr::select(-ghm),
                       by = c(noghs = "ghs", anseqta = "anseqta")) %>%
      dplyr::mutate(t_base = nbseance * tarif_base * cgeo *
                      cprudent, t_haut = nbjrbs * tarif_exh * cgeo *
                      cprudent, t_bas = dplyr::case_when(sejinfbi ==
                                                           "2" ~ (nbjrexb/10) * tarif_exb * cgeo * cprudent,
                                                         sejinfbi == "1" ~ forfait_exb * cgeo * cprudent,
                                                         sejinfbi == "0" ~ 0), rec_bee = (t_base + t_haut -
                                                                                            t_bas), rec_totale = rec_bee) %>%
      bind_rows(rsa %>%
                  dplyr::filter(substr(noghs,1,1) == 'I') %>%
                  dplyr::mutate(cprudent = dplyr::case_when(anseqta == "2023" ~ 0.993, anseqta == "2022" ~ 0.993, 
                                                            anseqta == "2021" ~ 0.993, anseqta == "2020" ~ 0.993,
                                                            anseqta == "2019" ~ 0.993, anseqta == "2018" ~ 0.993,
                                                            anseqta == "2017" ~ 0.993, anseqta == "2016" ~ 0.995,
                                                            anseqta == "2015" ~ 0.9965, anseqta == "2014" ~
                                                              0.9965, anseqta == "2013" ~ 0.9965, TRUE ~ 1)) %>%
                  dplyr::mutate(t_base = dplyr::case_when(noghs == 'I08' ~ 3119L * cprudent, TRUE ~ NA_real_),
                rec_bee = t_base, rec_totale = rec_bee, t_haut = 0, t_bas = 0))
  }
  else {
    rsa_2 <- rsa %>%
      dplyr::filter(substr(noghs,1,1) != 'I') %>%
      
      dplyr::mutate(cprudent = dplyr::case_when(
        anseqta == "2023" ~ prudent * 1.0023,
        anseqta == "2022" ~ prudent * 1.0013,
        anseqta == "2021" ~ prudent * 1.0019,
        TRUE ~ prudent)) %>% 
      dplyr::left_join(tarifs %>%
                         dplyr::select(-ghm), by = c(noghs = "ghs", anseqta = "anseqta")) %>%
      dplyr::mutate(t_base = nbseance * tarif_base * cgeo * cprudent, 
                    t_haut = nbjrbs * tarif_exh * cgeo * cprudent, 
                    t_bas = dplyr::case_when(sejinfbi == "2" ~ (nbjrexb/10) * tarif_exb * cgeo * cprudent,
                                             sejinfbi == "1" ~ forfait_exb * cgeo * cprudent,
                                             sejinfbi == "0" ~ 0), 
                    rec_bee = (t_base + t_haut - t_bas), 
                    rec_totale = rec_bee) %>%
      bind_rows(rsa %>%
                  dplyr::filter(substr(noghs,1,1) == 'I') %>%
                  mutate(cprudent = prudent) %>%
                  dplyr::mutate(t_base = dplyr::case_when(noghs == 'I08' ~ 3119L * cprudent * cgeo, TRUE ~ NA_real_),
                rec_bee = t_base, rec_totale = rec_bee, t_haut = 0, t_bas = 0))
  }
  
  if (bee == TRUE){
    return(rsa_2 %>% dplyr::select(cle_rsa, nbseance, rec_totale, rec_base = t_base, rec_exb =  t_bas, rec_exh = t_haut, rec_bee))
  }
  
  
  
  # Info suppléments ghs radiothérapie
  rsa_i <- rsa %>% dplyr::select(cle_rsa, rdth, nb_rdth) %>%
    dplyr::mutate(lrdh = stringr::str_extract_all(rdth, '.{7}'))
  
  rdth <- purrr::flatten_chr(rsa_i$lrdh) %>% stringr::str_trim()
  df <- rsa_i %>% dplyr::select(cle_rsa, nb_rdth)
  df <- as.data.frame(lapply(df, rep, df$nb_rdth), stringsAsFactors = F) %>% tibble::as_tibble()
  rdth <- dplyr::bind_cols(df,data.frame(r = rdth, stringsAsFactors = F) ) %>% tibble::as_tibble()
  
  rdth <- rdth %>%
    dplyr::mutate(codsupra = stringr::str_sub(r, 1,4),
                  nbsupra = stringr::str_sub(r, 5,7) %>% as.integer()) %>%
    dplyr::select(-nb_rdth, -r)
  
  
  rdth <- rdth %>%
    dplyr::mutate(
      nbacte9610 = nbsupra * (codsupra == '9610'),
      nbacte9619 = nbsupra * (codsupra == '9619'),
      nbacte9620 = nbsupra * (codsupra == '9620'),
      nbacte9621 = nbsupra * (codsupra == '9621'),
      nbacte9622 = nbsupra * (codsupra == '9622'),
      nbacte9625 = nbsupra * (codsupra == '9625'),
      nbacte9631 = nbsupra * (codsupra == '9631'),
      nbacte9632 = nbsupra * (codsupra == '9632'),
      nbacte9633 = nbsupra * (codsupra == '9633'),
      nbacte6523 = nbsupra * (codsupra == '6523'),
      nbacte9623 = nbsupra * (codsupra == '9623')
    ) %>%
    dplyr::group_by(cle_rsa) %>%
    dplyr::summarise_at(vars(starts_with('nbacte')), sum) %>%
    dplyr::right_join(distinct(rsa, cle_rsa), by = "cle_rsa") %>%
    dplyr::mutate_if(is.numeric, function(x)ifelse(is.na(x), 0, x))
  
  
  rsa_2 <- rsa_2 %>%
    dplyr::left_join(rdth, by = 'cle_rsa')
  
  
  # pie
  
  trans_pie <- pie %>%
    dplyr::group_by(cle_rsa, code_pie) %>%
    dplyr::summarise(nbsuppie = sum(nbsuppie)) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(tibble(liste_pie = c('STF', 'SRC', 'REA', 'REP', 'NN1', 'NN2', 'NN3')), by = c('code_pie' = 'liste_pie')) %>%
    tidyr::complete(cle_rsa, code_pie, fill = list(nbsuppie = 0)) %>%
    dplyr::mutate(code_pie = paste0('pie_', tolower(code_pie))) %>%
    tidyr::spread(code_pie, nbsuppie, fill = 0) %>%
    dplyr::filter(!is.na(cle_rsa))
  trans_pie <- trans_pie %>%
    dplyr::right_join(distinct(rsa, cle_rsa), by = "cle_rsa") %>%
    dplyr::mutate_if(is.numeric, function(x)dplyr::if_else(is.na(x), 0, x))
  
  
  # rehosp
  if (is.null(ano)){
    stop('La table ano doit être non vide et contenir les colonnes :\nnoanon, cok, cle_rsa, moissort, dtent, dtsort')
  }
  
  ano <- ano %>%
    dplyr::select(noanon, cok, cle_rsa, moissort, dtent, dtsort, nosej)
  
  
  reh <- rsa %>%
    dplyr::inner_join(ano, by = 'cle_rsa') %>%
    dplyr::filter(cok, moissor == moissort, rsacmd != '28', noghs != '9999') %>%
    dplyr::mutate(mdentr = paste0(echpmsi, prov),
                  mdsort = paste0(schpmsi, dest)) %>%
    dplyr::arrange(noanon, nosej) %>% # , ghm (suivant version de vvs ?)
    dplyr::group_by(noanon) %>%
    dplyr::mutate(r = row_number()) %>%
    dplyr::ungroup()
  
  reh <- reh %>%
    dplyr::mutate(lag_mdsort = dplyr::lag(mdsort),
                  lag_noanon = dplyr::lag(noanon),
                  lag_r      = dplyr::lag(r),
                  lag_noghs  = dplyr::lag(noghs),
                  lag_nosej  = dplyr::lag(nosej),
                  lag_duree  = dplyr::lag(duree),
                  lag_sexe   = dplyr::lag(sexe),
                  lag_agean  = dplyr::lag(agean)) %>%
    dplyr::mutate(delai = nosej - lag_nosej - lag_duree) %>%
    dplyr::filter(mdentr == '71',
                  mdentr == lag_mdsort, 
                  r == lag_r + 1,
                  noanon == lag_noanon, 
                  (agean == lag_agean | agean == lag_agean + 1 | is.na(agean)),
                  lag_noghs == noghs,
                  lag_sexe == sexe,
                  delai <= 10 ,
                  delai > 2) %>%
    dplyr::select(cle_rsa) %>%
    dplyr::mutate(rehosp_ghm = 1)
  
  
  
  # Import dip
  dip <- diap %>%
    dplyr::group_by(cle_rsa) %>%
    dplyr::summarise(nbdip = sum(nbsup))
  
  # Importer po
  po <- porg %>%
    dplyr::mutate(
      nb_poi    = (cdpo == "PO1"),  
      nb_poii   = (cdpo == "PO2"), 
      nb_poiii  = (cdpo == "PO3"),
      nb_poiv   = (cdpo == "PO4"), 
      nb_pov    = (cdpo == "PO5"),
      nb_povi   = (cdpo == "PO6"),
      nb_povii  = (cdpo == "PO7"), 
      nb_poviii = (cdpo == "PO8"), 
      nb_poix   = (cdpo == "PO9"),
      nb_poa    = (cdpo == "POA")
    )
  po_synthese <- po %>%
    dplyr::group_by(cle_rsa) %>%
    dplyr::summarise_at(dplyr::vars(dplyr::starts_with('nb_')), sum)
  
  
  for (i in 1:nrow(po_synthese)){
    if (po_synthese[i,]$nb_poiv > 0){
      po_synthese[i,]$nb_poi   <- 0
      po_synthese[i,]$nb_poii  <- 0
      po_synthese[i,]$nb_poiii <- 0
    }
  }
  
  po_synthese <- dplyr::distinct(rsa, cle_rsa) %>%
    dplyr::left_join(po_synthese, by = 'cle_rsa') %>%
    dplyr::mutate_if(is.numeric, function(x){ifelse(is.na(x), 0, x)})
  
  
  if (max(unique(rsa_2$anseqta)) < '2017'){
    rsa_2 <- rsa_2 %>%
      dplyr::mutate(suppdefcard = '0')
  }
  
  # Partie suppléments
  rsa_3 <- rsa_2 %>%
    dplyr::left_join(dip, by = 'cle_rsa') %>%
    dplyr::left_join(reh, by = 'cle_rsa') %>%
    dplyr::left_join(trans_pie, by = 'cle_rsa') %>%
    dplyr::mutate(nbdip = ifelse(is.na(nbdip), 0, nbdip),
                  rehosp_ghm = ifelse(is.na(rehosp_ghm), 0, rehosp_ghm)) %>%
    dplyr::left_join(po_synthese, by = 'cle_rsa') %>%
    dplyr::left_join(supplements, by = c('anseqta' = 'anseqta')) %>%
    # suppléments structures
    dplyr::mutate(rec_rep = pmax(trep * nbsuprep, 0) * cgeo * cprudent,
                  rec_rea = pmax(trea * nbsuprea, 0) * cgeo * cprudent,
                  rec_stf = pmax(tsi  * nbsupstf, 0) * cgeo * cprudent,
                  rec_src = pmax(tsc  * nbsupsrc, 0) * cgeo * cprudent,
                  rec_nn1 = pmax(tnn1 * nbsupnn1, 0) * cgeo * cprudent,
                  rec_nn2 = pmax(tnn2 * nbsupnn2, 0) * cgeo * cprudent,
                  rec_nn3 = pmax(tnn3 * nbsupnn3, 0) * cgeo * cprudent) %>%
    # suppléments dialyse hors séances
    dplyr::mutate(rec_hhs      = pmax(thhs     * nbsuphs,  0) * cgeo * cprudent,
                  rec_edpahs   = pmax(tedpahs  * nbsupahs, 0) * cgeo * cprudent,
                  rec_edpcahs  = pmax(tedpcahs * nbsupchs, 0) * cgeo * cprudent,
                  rec_ehhs     = pmax(tehhs    * nbsupehs, 0) * cgeo * cprudent,
                  rec_dip      = pmax(tdip     * nbdip,    0) * cgeo * cprudent,
                  rec_dialhosp = rec_hhs + rec_edpahs + rec_edpcahs + rec_ehhs + rec_dip) %>%
    #  autres suppléments
    dplyr::mutate(rec_caishyp = pmax(tcaishyp  * nbsupcaisson, 0) * cgeo * cprudent,
                  rec_aph     = pmax(taph_9615 * nbacte9615,   0) * cgeo * cprudent,
                  rec_ant     = pmax(tant      * nbsupatpart,  0) * cgeo * cprudent,
                  rec_rap     = pmax(trap      * nbsupreaped,  0) * cgeo * cprudent,
                  rec_sdc     = pmax(sdc       * (suppdefcard == '1'), 0) * cgeo * cprudent) %>%
    # supplements irradiation hors séances
    dplyr::mutate(
      rec_rdt5    = pmax(trdt5_9610  * nbacte9610, 0) * cgeo * cprudent,
      rec_prot    = pmax(tprot_9619  * nbacte9619, 0) * cgeo * cprudent,
      rec_ict     = pmax(tict_9620   * nbacte9620, 0) * cgeo * cprudent,
      rec_cyb     = pmax(tcyb_9621   * nbacte9621, 0) * cgeo * cprudent,
      rec_gam     = pmax(tgam_6523   * nbacte6523, 0) * cgeo * cprudent,
      rec_rcon1   = pmax(trconf_9622 * nbacte9622, 0) * cgeo * cprudent,
      rec_rcon2   = pmax(trconf_9625 * nbacte9625, 0) * cgeo * cprudent,
      rec_tciea   = pmax(ttciea_9631 * nbacte9631, 0) * cgeo * cprudent,
      rec_tcies   = pmax(ttcies_9632 * nbacte9632, 0) * cgeo * cprudent,
      rec_aie     = pmax(taie_9633   * nbacte9633, 0) * cgeo * cprudent,
      rec_rcon3   = pmax(trconf_9623 * nbacte9623, 0) * cgeo * cprudent,
      rec_rdt_tot = rec_rdt5 + rec_prot + rec_ict + rec_cyb + rec_gam +
        rec_rcon1 + rec_rcon2 + rec_tciea +
        rec_tcies + rec_aie + rec_rcon3) %>%
    # po
    dplyr::mutate(
      rec_poi    = pmax(tpoi    * nb_poi,    0) * cgeo * cprudent,
      rec_poii   = pmax(tpoii   * nb_poii,   0) * cgeo * cprudent,
      rec_poiii  = pmax(tpoiii  * nb_poiii,  0) * cgeo * cprudent,
      rec_poiv   = pmax(tpoiv   * nb_poiv,   0) * cgeo * cprudent,
      rec_pov    = pmax(tpov    * nb_pov,    0) * cgeo * cprudent,
      rec_povi   = pmax(tpovi   * nb_povi,   0) * cgeo * cprudent,
      rec_povii  = pmax(tpovii  * nb_povii,  0) * cgeo * cprudent,
      rec_poviii = pmax(tpoviii * nb_poviii, 0) * cgeo * cprudent,
      rec_poix   = pmax(tpoix   * nb_poix,   0) * cgeo * cprudent,
      rec_poa    = pmax(tpoa    * nb_poa,    0) * cgeo * cprudent,
      rec_po_tot = rec_poi + rec_poii + rec_poiii + rec_poiv +
        rec_pov + rec_povi + rec_povii + rec_poviii + rec_poix + rec_poa) %>%
    
    # rehosp
    dplyr::mutate(rec_rehosp_ghm = - pmax(rehosp_ghm *  (t_base + t_bas) / 2, 0) ) %>%
    # suppléments pie
    dplyr::mutate(rec_pie_src = pie_src * tsc  * cgeo * cprudent,
                  rec_pie_stf = pie_stf * tsi  * cgeo * cprudent,
                  rec_pie_rea = pie_rea * trea * cgeo * cprudent,
                  rec_pie_rep = pie_rep * trep * cgeo * cprudent,
                  rec_pie_nn1 = pie_nn1 * tnn1 * cgeo * cprudent,
                  rec_pie_nn2 = pie_nn2 * tnn2 * cgeo * cprudent,
                  rec_pie_nn3 = pie_nn3 * tnn3 * cgeo * cprudent) %>%
    # ajout des pie aux supp structures classiques
    dplyr::mutate(rec_rep = rec_rep + rec_pie_rep,
                  rec_rea = rec_rea + rec_pie_rea,
                  rec_stf = rec_stf + rec_pie_stf,
                  rec_src = rec_src + rec_pie_src,
                  rec_nn1 = rec_nn1 + rec_pie_nn1,
                  rec_nn2 = rec_nn2 + rec_pie_nn2,
                  rec_nn3 = rec_nn3 + rec_pie_nn3)
  
  # calcul recette totale
  if (min(rsa_3$anseqta) > '2015'){
    rsa_3 <- rsa_3 %>%
      dplyr::mutate(rec_totale = rec_bee + rec_rep + rec_rea + rec_stf + rec_src + rec_nn1 + rec_nn2 + rec_nn3 +
                      rec_dialhosp + rec_caishyp + rec_aph + rec_ant + rec_rap + rec_rehosp_ghm +
                      rec_rdt_tot + rec_sdc + rec_po_tot)
  } else {
    rsa_3 <- rsa_3 %>%
      dplyr::mutate(rec_sdc = 0) %>%
      dplyr::mutate(rec_totale = rec_bee + rec_rep + rec_rea + rec_stf + rec_src + rec_nn1 + rec_nn2 + rec_nn3 +
                      rec_dialhosp + rec_caishyp + rec_aph + rec_ant + rec_rap + rec_rehosp_ghm +
                      rec_rdt_tot + rec_po_tot)
  }
  
  
  if (full){
    return(rsa_3 %>% dplyr::rename(rec_base = t_base, rec_exb =  t_bas, rec_exh = t_haut))
  } else
    if (!full){
      return(rsa_3 %>% dplyr::select(cle_rsa, nbseance, moissor, anseqta, rec_totale, rec_bee, rec_base = t_base, rec_exb =  t_bas, rec_exh = t_haut,
                                     rec_rep, rec_rea, rec_stf, rec_src, rec_nn1, rec_nn2, rec_nn3,
                                     rec_dialhosp, rec_caishyp, rec_aph, rec_ant, rec_rap, rec_rehosp_ghm,
                                     rec_rdt_tot, rec_sdc, rec_po_tot))
    }
  
}


#' ~ VVR - Attribuer le caractere facturable par cle_rsa
#'
#' Reproduire les catégories du tableau SV d'epmsi, à partir des tables résultant des fonctions
#' \code{\link{vvr_rsa}}, \code{\link{vvr_ano_mco}} et éventuellement d'une table contenant le fichcomp PO
#' 
#' 
#' @param rsa un tibble rsa contenant les variables nécessaires (créé avec \code{\link{vvr_rsa}})
#' @param ano un tibble ano contenant les variables nécessaires (créé avec \code{\link{vvr_mco_ano}})
#' @param porg un tibble porg contenant les prélevements d'organes du out (créé avec \code{\link{ipo}})
#' 
#' @return Un tibble contenant la catégorie du tableau SV epmsi, une ligne par clé rsa
#'
#' @examples
#' \dontrun{
#' # Tenir compte des porg
#' vvr_mco_sv(vrsa, vano, porg = ipo(p))
#' 
#' # ne pas tenir compte des porg
#' vvr_mco_sv(vrsa, vano)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{vvr_ano_mco}}, \code{\link{vvr_rsa}}, \code{\link{vvr_mco}}
#' @export vvr_mco_sv
#' @export
vvr_mco_sv <- function(rsa, ano, porg = dplyr::tibble(cle_rsa = "")){
  
  # CM 90
  # Séjours en CM 90 : nombre de RSA groupés dans la CM 90 (groupage en erreur)
  un <- rsa %>% dplyr::filter(rsacmd == '90') %>% 
    dplyr::select(cle_rsa)
  
  # PIE
  # Séjours de prestation inter-établissement (PIE)
  # : nombre de RSA avec type de séjour=’B’ (hors séances de RDTH, dialyse et chimiothérapie, car les PIE sont valorisés pour ces RSA). . Sont 
  # ainsi dénombrés les séjours effectués dans l’établissement de santé prestataire (type de séjour = ‘B’) à la demande d’un établissement (type de séjour = ‘A’) pour la réalisation d’un acte
  # médicotechnique ou d’une autre prestation.
  deux <- rsa %>% dplyr::filter(typesej == 'B', !(ghm %in% c("28Z03Z","28Z04Z","28Z07Z","28Z08Z","28Z09Z","28Z10Z","28Z11Z","28Z12Z","28Z13Z","28Z17Z","28Z18Z",
                                                             "28Z19Z","28Z20Z","28Z21Z","28Z22Z","28Z23Z","28Z23Z","28Z24Z","28Z25Z")))%>% 
    dplyr::select(cle_rsa)
  
  # RSA avec GHS 9999 : nombre de RSA avec GHS = 9999
  trois <- rsa %>% dplyr::filter(noghs == '9999') %>% 
    dplyr::anti_join(un, by = 'cle_rsa') %>% 
    dplyr::select(cle_rsa)
  
  # Table intermédiaire
  autres1 <- rsa %>% 
    dplyr::filter(! substr(ghm,1,5) %in% c('28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25') |
                    !((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))) %>% 
    dplyr::select(cle_rsa) %>% 
    dplyr::anti_join(un, by = 'cle_rsa') %>% 
    dplyr::anti_join(deux, by = 'cle_rsa') %>% 
    dplyr::anti_join(porg, by = 'cle_rsa') %>% 
    dplyr::anti_join(trois, by = 'cle_rsa') %>% 
    dplyr::select(cle_rsa)
  
  # Table intermédiaire 2
  autres2 <- rsa %>% 
    dplyr::anti_join(un, by = 'cle_rsa') %>% 
    dplyr::anti_join(deux, by = 'cle_rsa') %>% 
    dplyr::anti_join(porg, by = 'cle_rsa') %>% 
    dplyr::anti_join(trois, by = 'cle_rsa') %>% 
    dplyr::select(cle_rsa)
  
  # Forfait journalier non applicable
  fjnap <- rsa %>% 
    dplyr::filter(
      (rsacmd == '28')| 
        (duree == 0) |
        ghm == '14Z08Z' | ghm == '23K02Z' |
        ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))) %>% 
    dplyr::select(cle_rsa) %>% 
    dplyr::mutate(fjnap = 1)
  if("cdgestion" %in% colnames(ano)) {
    # séjours avec pb de chaînage (hors NN, rdth et PO)
    quatre <- ano %>% dplyr::filter((crfushosp != '0' | crfuspmsi != '0') & cdgestion != '65') %>% 
      dplyr::select(cle_rsa) %>% 
      dplyr::inner_join(autres1, by = "cle_rsa") %>% 
      dplyr::anti_join(rsa %>% 
                         dplyr::filter( substr(ghm,1,5) %in% c('28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25') | 
                                          ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))), by = "cle_rsa") %>%
      dplyr::select(cle_rsa)
  } else {
    quatre <- ano %>% dplyr::filter((crfushosp != '0' | crfuspmsi != '0')) %>% 
      dplyr::select(cle_rsa) %>% 
      dplyr::inner_join(autres1, by = "cle_rsa") %>% 
      dplyr::anti_join(rsa %>% 
                         dplyr::filter( substr(ghm,1,5) %in% c('28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25') | 
                                          ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))), by = "cle_rsa") %>%
      dplyr::select(cle_rsa)
  }
  # attente de décision sur les droits du patient (hors NN, rdth et PO)
  six <- ano %>% 
    dplyr::filter(factam == '3') %>% 
    dplyr::inner_join(autres1, by = "cle_rsa") %>% 
    dplyr::anti_join(rsa %>% 
                       dplyr::filter( substr(ghm,1,5) %in% c('28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25') | 
                                        ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))), by = "cle_rsa") %>% 
    dplyr::select(cle_rsa)
  
  # séjours non facturables à l AM (sejours sans PO)
  sept <- ano %>% 
    dplyr::filter(factam == '0')  %>% 
    dplyr::anti_join(six, by = "cle_rsa") %>% 
    dplyr::anti_join(deux, by = "cle_rsa") %>% 
    dplyr::anti_join(trois, by = "cle_rsa") %>% 
    dplyr::anti_join(un, by = "cle_rsa") %>% 
    dplyr::anti_join(porg, by = "cle_rsa") %>% 
    dplyr::select(cle_rsa)
  
  # séjours avec PO sur patient arrivé décédé ou avec PO non facturables à l AM
  huit <- ano %>% 
    dplyr::filter(factam == '0')  %>% 
    dplyr::anti_join(six, by = "cle_rsa") %>% 
    dplyr::anti_join(deux, by = "cle_rsa") %>% 
    dplyr::anti_join(trois, by = "cle_rsa") %>% 
    dplyr::anti_join(un, by = "cle_rsa") %>% 
    dplyr::semi_join(porg, by = "cle_rsa") %>% 
    dplyr::select(cle_rsa)
  
  test <- ano %>% 
    dplyr::left_join(fjnap, by = 'cle_rsa')
  test <- test %>% dplyr::inner_join(rsa %>% dplyr::select(cle_rsa, agean, agejr, ghm, rsacmd, duree), by = "cle_rsa")
  
  
  caractere_bloquant <- 
    dplyr::bind_rows(
      test %>% 
        dplyr::filter(factam == '0') %>% 
        dplyr::mutate(typvidhosp = 1, 
                      bextic = (cdexticm == 'X'), 
                      bfojo = (cdprfojo == 'X'), 
                      bnatass = (natass == 'XX'),
                      beven = is.integer(nbeven)),
      test %>% 
        dplyr::filter(factam == '3') %>% 
        dplyr::mutate(typvidhosp = 2,
                      bextic = (cdexticm == 'X'), 
                      bfojo = (cdprfojo == 'X'), 
                      bnatass = (natass == 'XX'),
                      beven = is.integer(nbeven)),
      test %>% 
        dplyr::filter(factam == '2') %>% 
        dplyr::mutate(typvidhosp = 3,
                      bextic = (cdexticm %in% c('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C', 'X')),
                      bfojo =  (cdprfojo %in% c('A', 'L', 'R', 'X')),
                      bnatass = (natass %in% c('10', '13', '30', '41', '90', 'XX')),
                      beven = is.integer(nbeven)),
      test %>% 
        dplyr::filter(factam == '1', # nv-né
                      ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))
                      #duree == 0 & ! rsacmd == '28' & ! ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))
        ) %>% 
        dplyr::mutate(typvidhosp = 4,
                      bextic = (cdexticm == 'X'),
                      bfojo = (cdprfojo == 'X'),
                      bnatass = (natass == 'XX'),
                      beven = (nbeven == 1)),
      test %>% 
        dplyr::filter(factam == '1', # radiotherapie
                      substr(ghm,1,5) %in% c('28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25'),
                      !((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))) %>% 
        dplyr::mutate(typvidhosp = 5,
                      bextic = (cdexticm %in% c('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C', 'X')),
                      bfojo = (cdprfojo == 'X'),
                      bnatass = (natass %in% c('10', '13', '30', '41', '90', 'XX')),
                      beven = is.integer(nbeven)),
      test %>% 
        dplyr::filter(factam == '1', # séances hors radiot
                      rsacmd == '28' & ! substr(ghm,1,5) %in% c('28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25') & ! ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))) %>% 
        dplyr::mutate(typvidhosp = 6, # séances hors radiot
                      bextic = (cdexticm %in% c('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C')),
                      bfojo = (cdprfojo == 'X'),
                      bnatass = (natass %in% c('10', '13', '30', '41', '90')),
                      beven = is.integer(nbeven)),
      test %>% 
        dplyr::filter(factam == '1', # durée = 0 jour
                      duree == 0 & rsacmd != '28' & ! substr(ghm,1,5) %in% c('28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25') & ! ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))) %>% 
        dplyr::mutate(typvidhosp = 7,
                      bextic = (cdexticm %in% c('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C')),
                      bfojo = (cdprfojo == 'X'),
                      bnatass = (natass %in% c('10', '13', '30', '41', '90')),
                      beven = is.integer(nbeven)),
      test %>% 
        dplyr::filter(factam == '1') %>% 
        dplyr::filter(! (rsacmd == '28')) %>% 
        dplyr::filter(! ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))) %>% 
        dplyr::filter(! duree == 0) %>% 
        dplyr::mutate(typvidhosp = 8,
                      bextic = (cdexticm %in% c('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C')),
                      bfojo = (cdprfojo %in% c('A', 'L', 'R')),
                      bnatass = (natass %in% c('10', '13', '30', '41', '90')),
                      beven = (nbeven == 1))) %>% 
    dplyr::select(cle_rsa, typvidhosp, bextic, bfojo, bnatass, beven)
  
  test %>% 
    dplyr::inner_join(caractere_bloquant, by = "cle_rsa") -> resu
  
  
  bloq <- resu %>% 
    dplyr::filter(tauxrm %in% c(80, 90, 100), 
                  (is.na(fjnap) & bfojo == FALSE)) %>% #  | beven == FALSE
    dplyr::bind_rows(
      resu %>% 
        filter(! tauxrm %in% c(80, 90, 100), 
               (is.na(fjnap) & bfojo == FALSE)  | bnatass == FALSE | bextic == FALSE )) #| beven == FALSE
  
  
  ok <- dplyr::bind_rows(un %>% dplyr::mutate(type = 1),
                         deux %>% dplyr::mutate(type = 2),
                         trois %>% dplyr::mutate(type = 3),
                         quatre %>% dplyr::mutate(type = 4),
                         six %>% dplyr::mutate(type = 6),
                         sept %>% dplyr::mutate(type = 7),
                         huit %>% dplyr::mutate(type = 8))
  
  if("cdgestion" %in% colnames(bloq)) {
    bloq %>% 
      dplyr::anti_join(ok, by = "cle_rsa") %>% 
      dplyr::anti_join(rsa %>% 
                         dplyr::filter(
                           ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))), by = 'cle_rsa') %>% 
      dplyr::anti_join(porg, by = 'cle_rsa') %>% 
      dplyr::anti_join(ano %>% filter(cdgestion == '65'), by = 'cle_rsa') -> bloq1
  } else {
    bloq %>% 
      dplyr::anti_join(ok, by = "cle_rsa") %>% 
      dplyr::anti_join(rsa %>% 
                         dplyr::filter(
                           ((agejr <= 30 & agejr >= 0) & (agean == 0 | is.na(agean)))), by = 'cle_rsa') %>% 
      dplyr::anti_join(porg, by = 'cle_rsa')
  }
  fin <- dplyr::select(test, cle_rsa) %>% 
    dplyr::left_join(dplyr::select(ok, cle_rsa, type), by = 'cle_rsa') %>% 
    dplyr::left_join(dplyr::select(bloq1, cle_rsa) %>% dplyr::mutate(bloq = 1), by = 'cle_rsa') %>% 
    dplyr::left_join(dplyr::select(caractere_bloquant, cle_rsa, typvidhosp), by = 'cle_rsa') 
  
  fin <- fin %>% 
    dplyr::mutate(
      type_fin = dplyr::case_when(
        is.na(type)  & !is.na(bloq) ~ 5,
        !is.na(type) & !is.na(bloq) ~ type,
        !is.na(type) & is.na(bloq) ~ type,
        TRUE ~ 0
      )
    )
  
  return(dplyr::select(fin, cle_rsa, type_fin, typvidhosp))
}


#' ~ VVR - Croiser parties GHS / supplements et ano
#'
#' On ajoute les données facturation (vvr_mco_sv) aux données de valorisation 100% T2A (vvr_ghs_supp)
#' 
#' C'est un left join
#' 
#' @param rsa_v tibble résultant de la fonction \code{\link{vvr_rsa}}
#' @param ano_sv tibble résultant de la fonction \code{\link{vvr_ano_mco}}
#' 
#' @return Un tibble final contenant la catégorie du tableau SV epmsi, et les variables rec_ de recette par séjour
#'
#' @examples
#' \dontrun{
# resu_valo <- vvr_mco(
#   vvr_ghs_supp(vrsa, tarifs, supplements, vano, ipo(p), idiap(p), bee = FALSE),
#   vvr_mco_sv(vrsa, vano, porg = ipo(p))
#   )
#' }
#'
#' @author G. Pressiat
#'
#' @export vvr_mco
#' @export
vvr_mco <- function(rsa_v, ano_sv){
  dplyr::left_join(rsa_v, ano_sv, by = 'cle_rsa')
}

#' ~ VVR - Libelles des rubriques de valorisation et de types de rsa
#'
#' Fonction pour obtenir les tables de libellés du tableau SV, RAV, et le type de caractère bloquant des données VIDHOSP.
#' 
#' @param wich Chaine de caractères parmi :  'lib_type_sej', 'lib_vidhosp', 'lib_valo'
#' 
#' @return Un tibble avec les codes et libellés
#'
#' @examples
#' \dontrun{
#' # Libellés des types de séjours (tableau SV)
#' vvr_libelles_valo('lib_type_sej')
#' 
#' # Libellés des types de vidhosp
#' vvr_libelles_valo('lib_vidhosp')
#'
#' # Libellés des types de valo (tableau RAV)
#' vvr_libelles_valo('lib_valo')
#   )
#' }
#'
#' @author G. Pressiat
#'
#' @export vvr_libelles_valo
#' @export
vvr_libelles_valo <- function(wich){
  
  lib_vidhosp <- tibble::tribble(
    ~typvidhosp , ~lib_typvidhosp,
    1L,"0 : non pris en charge",
    2L,"3 : En attente des droits du patient",
    3L,"2 : En attente sur le taux de prise en charge",
    4L,"1 : Nouveau-né",
    5L,"1 : Radiothérapie",
    6L,"1 : Séances hors radiothérapie",
    7L,"1 : Durée de séjour = 0",
    8L,"1 : Autre type de séjour"
  )
  
  lib_type_sej <- tibble::tribble(
    ~type_fin , ~lib_type,
    0L,"Séjours valorisés",
    1L,"Séjours en CM 90",
    2L,"Séjours en prestation inter-établissement",
    3L,"Séjours en GHS 9999",
    4L,"Séjours avec pb de chainage (hors NN, rdth et PO)",
    5L,"Séjours avec pb de codage des variables bloquantes",
    6L,"Séjours en attente de décision sur les droits du patient",
    7L,"Séjours non facturable à l'AM hors PO",
    8L,"Séjours avec PO sur patient arrivé décédé ou avec PO non facturables à l'AM"
  )
  
  lib_valo <- 
    tibble::tribble(
      ~ordre_epmsi,~var,                                                        ~lib_valo,
      22L,"rec_totale",                                  "Valorisation 100% T2A globale",
      1L,"rec_base",                                   "Valorisation des GHS de base",
      0L,"rec_bee",                                   "Valorisation base + exb + exh",
      2L,"rec_exb",                           "Valorisation extrême bas (à déduire)",
      3L,"rec_rehosp_ghm",                 "Valorisation séjours avec rehosp dans même GHM",
      4L,"rec_mino_sus",  "Valorisation séjours avec minoration forfaitaire liste en sus",
      5L,"rec_exh",                             "Valorisation journées extrême haut",
      6L,"rec_aph",                         "Valorisation actes GHS 9615 en Hospit.",
      7L,"rec_rap",             "Valorisation suppléments radiothérapie pédiatrique",
      8L,"rec_ant",                            "Valorisation suppléments antepartum",
      9L,"rec_rdt_tot",                             "Valorisation actes RDTH en Hospit.",
      10L,"rec_rea",                        "Valorisation suppléments de réanimation",
      11L,"rec_rep",                    "Valorisation suppléments de réa pédiatrique",
      12L,"rec_nn1",                     "Valorisation suppléments de néonat sans SI",
      13L,"rec_nn2",                     "Valorisation suppléments de néonat avec SI",
      14L,"rec_nn3",                 "Valorisation suppléments de réanimation néonat",
      15L,"rec_po_tot",                             "Valorisation prélévements d organe",
      16L,"rec_caishyp",           "Valorisation des actes de caissons hyperbares en sus",
      17L,"rec_dialhosp",                            "Valorisation suppléments de dialyse",
      18L,"rec_sdc",                "Valorisation supplément défibrilateur cardiaque",
      19L, "rec_i04",                "Valorisation suppléments Forfait Innovation I04",
      20L,"rec_src",              "Valorisation suppléments de surveillance continue",
      21L,"rec_stf",                    "Valorisation suppléments de soins intensifs",
      
    )
  
  
  lib_detail_valo <- tibble::tribble(
    ~var,             ~libelle_detail_valo,
    "nb_poa",     "Supplément prélèvement d'organe",
    "nb_poi",      "Supplément prélèvement d'organe",
    "nb_poii",      "Supplément prélèvement d'organe",
    "nb_poiii",      "Supplément prélèvement d'organe",
    "nb_poiv",      "Supplément prélèvement d'organe",
    "nb_poix",      "Supplément prélèvement d'organe",
    "nb_pov",      "Supplément prélèvement d'organe",
    "nb_povi",      "Supplément prélèvement d'organe",
    "nb_povii",      "Supplément prélèvement d'organe",
    "nb_poviii",      "Supplément prélèvement d'organe",
    "nbacte6523",            "Supplément RDTH en hopsit",
    "nbacte9610",            "Supplément RDTH en hopsit",
    "nbacte9615",                  "Supplément GHS 9615",
    "nbacte9619",            "Supplément RDTH en hopsit",
    "nbacte9620",            "Supplément RDTH en hopsit",
    "nbacte9621",            "Supplément RDTH en hopsit",
    "nbacte9622",            "Supplément RDTH en hopsit",
    "nbacte9623",            "Supplément RDTH en hopsit",
    "nbacte9625",            "Supplément RDTH en hopsit",
    "nbacte9631",            "Supplément RDTH en hopsit",
    "nbacte9632",            "Supplément RDTH en hopsit",
    "nbacte9633",            "Supplément RDTH en hopsit",
    "nbdip",                 "Supplément dialyses",
    "nbjrbs",                        "Extrêmes haut",
    "nbjrexb",                         "Extrêmes bas",
    "nbrum",                               "Nb RUM",
    "nbseance",                 "Nb séjours / séances",
    "nbsupahs",                  "Supplément dialyses",
    "nbsupatpart",               "Supplément Ante partum",
    "nbsupcaisson",         "Supplément caisson hyperbare",
    "nbsupchs",                  "Supplément dialyses",
    "nbsupehs",                  "Supplément dialyses",
    "nbsuphs",                  "Supplément dialyses",
    "nbsupnn1",            "Supplément néonat sans SI",
    "nbsupnn2",            "Supplément néonat avec SI",
    "nbsupnn3",        "Supplément réanimation néonat",
    "nbsuprea",               "Supplément réanimation",
    "nbsupreaped", "Supplément radiothérapie pédiatrique",
    "nbsuprep",   "Supplément réanimation pédiatrique",
    "nbsupsrc",     "Supplément surveillance continue",
    "nbsupstf",           "Supplément soins intensifs",
    "pie_nn1",     "Supplément néonat sans SI",
    "pie_nn2",     "Supplément néonat avec SI",
    "pie_nn3",     "Supplément réanimation néonat",
    "pie_rea",     "Supplément réanimation",
    "pie_rep",     "Supplément réanimation pédiatrique", 
    "pie_src",     "Supplément surveillance continue",
    "pie_stf",     "Supplément soins intensifs",
    "rehosp_ghm", "Réhospitalisation dans le même GHM"
  )
  
  return(list(lib_valo = lib_valo, lib_type_sej = lib_type_sej, lib_vidhosp = lib_vidhosp, lib_detail_valo = lib_detail_valo)[[wich]])
}

#' ~ VVR - Reproduire le tableau SV
#'
#' Il s'agit d'un tableau similaire au tableau "Séjours Valorisés"
#' 
#' @param valo Un tibble résultant de \code{\link{vvr_mco}}
#' @param knit à TRUE, appliquer une sortie `knitr::kable` au résultat
#' 
#' @return Un tibble similaire au tableau RAV epmsi
#'
#' Il s'agit d'un tableau similaire au tableau "Récapitulation Activité - Valorisation"
#' @examples
#' \dontrun{
#' epmsi_mco_sv(valo)
#  
#' }
#'
#' @author G. Pressiat
#'
#' @export epmsi_mco_sv
#' @export
epmsi_mco_sv <- function(valo, knit = FALSE){
  rr <- valo %>% 
    dplyr::group_by(type_fin) %>% 
    dplyr::summarise(n = sum(nbseance),
                     rec = sum(rec_totale)) %>%
    dplyr::mutate(`%` = scales::percent(n / sum(n))) %>% 
    dplyr::left_join(vvr_libelles_valo('lib_type_sej'), by = c('type_fin' = 'type_fin'))
  
  if (knit){
    return(knitr::kable(rr))
  } else {
    return(rr)
  }
}

#' ~ VVR - Reproduire le tableau RAV
#'
#' 
#' @param valo Un tibble résultant de \code{\link{vvr_mco}}
#' @param knit à TRUE, appliquer une sortie `knitr::kable` au résultat
#' 
#' @return Un tibble similaire au tableau RAV epmsi (montant BR* ou BR si coeff prud = 1)
#'
#' @examples
#' \dontrun{
#' epmsi_mco_rav(valo)
# 
#' }
#'
#' @author G. Pressiat, fbrcdnj 
#'
#' @export epmsi_mco_rav
#' @export
epmsi_mco_rav <- function(valo, knit = FALSE){
  rr <- valo %>% 
    dplyr::select(cle_rsa, dplyr::starts_with('rec_'), type_fin, -rec_totale, -rec_bee) %>% 
    dplyr::mutate(rec_exb = -rec_exb) %>% 
    tidyr::gather(var, val, - cle_rsa, - type_fin) %>%
    dplyr::filter(abs(val) > 0) %>%
    # on créé fictivement le type 8 pour être générique
    dplyr::bind_rows(tibble(cle_rsa = "0000000000", type_fin = 8L)) %>% 
    dplyr::mutate(type_fin = paste0('t_', type_fin)) %>% 
    tidyr::spread(type_fin, val, fill = 0) %>% 
    dplyr::mutate(t_total = rowSums(.[grep("t_", names(.))])) %>% 
    dplyr::mutate(t_total = ifelse(var == "rec_po_tot", t_total, t_0)) %>% 
    dplyr::select(cle_rsa, var, val = t_total) %>% 
    dplyr::filter(cle_rsa != "0000000000") %>% 
    tidyr::gather(type_fin, val, - cle_rsa, - var) %>% 
    dplyr::inner_join(vvr_libelles_valo('lib_valo'), by = "var") %>%
    dplyr::group_by(ordre_epmsi, lib_valo, var) %>%
    dplyr::summarise(v = sum(val)) %>% 
    tidyr::replace_na(list(v = 0)) %>% 
    dplyr::ungroup() %>% 
    # arrange(lib_valo) %>% 
    dplyr::arrange(ordre_epmsi) %>% 
    dplyr::bind_rows(tibble(lib_valo = "Total valorisation 100% T2A", 
                            var = "rec_totale",
                            v = sum(.$v))) %>% 
    dplyr::mutate(v = formatC(v, format = "f", big.mark = " ", decimal.mark = ",", digits = 3) %>% paste0('€'))
  
  if (knit){
    return(knitr::kable(rr))
  } else {
    return(rr)
  }
}




#' ~ VVR - Reproduire le tableau RAE
#'
#' 
#' @param valo Un tibble résultant de \code{\link{vvr_mco}}
#' @param knit à TRUE, appliquer une sortie `knitr::kable` au résultat
#' 
#' @return Un tibble similaire au tableau RAE epmsi
#'
#' @examples
#' \dontrun{
#' epmsi_mco_rae(valo)
# 
#' }
#'
#' @author G. Pressiat
#'
#' @export epmsi_mco_rae
#' @export
epmsi_mco_rae <- function(valo, knit = FALSE){
  rr <- valo %>% 
    dplyr::select(cle_rsa, dplyr::starts_with('nb'), dplyr::starts_with('pie'), 
                  type_fin, rehosp_ghm, -nbsupsi, - nb_rdth) %>% 
    tidyr::gather(var, val, - cle_rsa, - type_fin) %>%
    dplyr::filter(abs(val) > 0) %>%
    dplyr::inner_join(vvr_libelles_valo('lib_detail_valo'), by = 'var') %>% 
    dplyr::group_by(cle_rsa) %>% 
    dplyr::mutate(flag_po = sum(libelle_detail_valo == "Supplément prélèvement d'organe", na.rm = TRUE) > 0) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(val = case_when(
      type_fin == 0L ~ val,
      flag_po & substr(var,1,5) == "nb_po" ~ val,
      TRUE ~ 0)) %>%
    # on créé fictivement le type 8 pour être générique
    dplyr::bind_rows(tibble(cle_rsa = "0000000000", type_fin = 8L)) %>% 
    dplyr::mutate(type_fin = paste0('t_', type_fin)) %>% 
    dplyr::filter(val > 0) %>% 
    tidyr::spread(type_fin, val, fill = 0) %>% 
    dplyr::mutate(t_total = rowSums(.[grep("t_", names(.))])) %>% 
    dplyr::mutate(t_total = ifelse(substr(var,1,5) == "nb_po", t_total, t_0)) %>% 
    dplyr::select(cle_rsa, var, val = t_total, libelle_detail_valo) %>% 
    dplyr::filter(cle_rsa != "0000000000") %>% 
    dplyr::group_by(libelle_detail_valo) %>%
    dplyr::summarise(n_sej = dplyr::n_distinct(cle_rsa),
                     n_sup = sum(val))
  
  if (knit){
    return(knitr::kable(rr))
  } else {
    return(rr)
  }
}

#' ~ VVR - Distribuer la valorisation des rsa au niveau des rum ou des passages UM
#'
#' 
#' @param valo Un tibble résultant de \code{\link{vvr_mco}}
#' @param p Un noyau de paramètres
#' @param repartition_multi pour renseigner les paramètres de la clef de répartition (entre durée de passage et PMCT des UM fréquentées)
#' @param pmct_mono Calcul du PMCT par UM sur les mono-RUM (TRUE), ou sur tous les séjours par l'UM fournissant le DP (FALSE)
#' @param seuil_pmct En dessous de quel nombre on considère le PMCT non robuste, dans ce cas, on passe à une distribution uniquement sur les durées de passages
#' @param type_passage La table résultat est soit au niveau RUM, soit au niveau passage unique (pas de UM A, UM B, UM A, juste UM A, UM B)
#' 
#' 
#' @return Un tibble 
#'
#' @examples
#' \dontrun{
#' vvr_rum(p, valo, type_passage = "RUM", pmct_mono = FALSE)
# 
#' }
#'
#' @author G. Pressiat
#' @author fbrcdnj 
#'
#' @seealso epmsi_mco_rav_rum, vvr_mco
#' @export vvr_rum
#' @export
vvr_rum <- function(p, valo, 
                    repartition_multi = '{prop_pmct_um}*0.5+{prop_pass}*0.5', 
                    pmct_mono = c(FALSE, TRUE), 
                    seuil_pmct = 10,
                    type_passage = c('RUM', 'Passage unique')){
  
  message('- Valorisation des RUM')
  message('-- Clef de répartition : ', repartition_multi)
  
  p1 <- stringr::str_replace(repartition_multi, '(\\{prop_pmct_um\\})\\*(.*)\\+(\\{prop_pass\\})\\*(.*)', '\\2') %>% as.numeric()
  p2 <- stringr::str_replace(repartition_multi, '(\\{prop_pmct_um\\})\\*(.*)\\+(\\{prop_pass\\})\\*(.*)', '\\4') %>% as.numeric()
  
  message('-- Poids PMCT             : ', p1)
  message('-- Poids durée de passage : ', p2)
  
  if ((p1 + p2) != 1L){
    stop(paste0('Pb dans la formule : la somme des deux paramètres doit faire 1 : ', repartition_multi))
    
  }
  
  if (p1 > 0){
  message('-- PMCT utilisé : \n-- -- ', 
          ifelse(!is.na(pmct_mono), 'PMCT des MONO-RUM', 'PMCT via le RUM principal du RSA'))
  }
  rsa <- pmeasyr::irsa(p, typi = 4)
  
  message('-- Nb rsa ', nrow(rsa$rsa))
  message('-- Nb rsa_um ', nrow(rsa$rsa_um))
  
  rsa_um <- rsa$rsa_um %>% 
    dplyr::select(cle_rsa, typaut1, dureesejpart, 
                  dpum, drum, 
                  natsupp1, nbsupp1, 
                  nbsupp2, typaut2, 
                  natsupp2, nseqrum) %>% 
    dplyr::mutate(nbsupp1 = as.integer(nbsupp1)) %>% 
    dplyr::mutate(nbsupp2 = as.integer(nbsupp2)) %>% 
    dplyr::mutate(typaut1 = substr(typaut1,1,3)) %>% 
    mutate_if(is.numeric, tidyr::replace_na, 0) %>% 
    inner_tra(itra(p))
  
  rum <- irum(p, typi = 1)$rum %>% 
    dplyr::select(norss, cdurm, d8eeue, d8soue, dp, dr, norum) %>% 
    dplyr::mutate(dsrum = as.integer(difftime(d8soue, d8eeue, units = "days"))) %>% 
    dplyr::mutate(norum = stringr::str_pad(
      stringr::str_trim(substr(norum, nchar(norum) - 1, nchar(norum))), 2, 'left', '0'))
  
  rsa_rum <- dplyr::inner_join(rsa_um, rum, by = c('norss', 'nseqrum' = 'norum'))
  
  message('-- Nb rum ', nrow(rum))
  
  rsa_rum_dp <- dplyr::inner_join(rsa$rsa %>%  select(cle_rsa, nbrum, noseqrum) %>% 
                                    inner_tra(itra(p)), rum, by = c('norss', 'noseqrum' = 'norum')) %>% 
    inner_join(valo %>% select(cle_rsa, rec_bee, rec_totale), by = 'cle_rsa') %>% 
    left_join(rsa_rum %>% select(cle_rsa, nseqrum, typaut1), by = c('cle_rsa', 'noseqrum' = 'nseqrum')) %>% 
    distinct(cle_rsa, cdurm, .keep_all = TRUE)
  
  pmct_um_dp <- rsa_rum_dp %>% 
    filter(nbrum == 1L) %>% 
    group_by(cdurm, typaut1, mono  = TRUE) %>% 
    summarise(n = n(),
              pmct_um_dp = mean(rec_bee, na.rm = TRUE),
              pmedct_um_dp = median(rec_bee, na.rm = TRUE),
              psdct_um_dp = sd(rec_bee),
              iqr = IQR(rec_bee)) %>% 
    bind_rows(rsa_rum_dp %>% 
                group_by(cdurm, typaut1, mono  = FALSE) %>% 
                summarise(n = n(),
                          pmct_um_dp = mean(rec_bee),
                          pmedct_um_dp = median(rec_bee),
                          psdct_um_dp = sd(rec_bee),
                          iqr = IQR(rec_bee)))
  
  
  rsa_rum <- rsa_rum %>% 
    dplyr::mutate(
      suprea = pmax( nbsupp1 * (natsupp1 == '01'),0) + pmax(nbsupp2 * (natsupp2 == '01'),0),
      flag_rea = (natsupp1 == '01' | natsupp2 == '01'),
      supstf = pmax(nbsupp1 * (natsupp1 == '02'),0) + pmax(nbsupp2 * (natsupp2 == '02'),0),
      flag_stf = (natsupp1 == '02' | natsupp2 == '02'),
      supsrc = pmax(nbsupp1 * (natsupp1 == '03'),0) + pmax(nbsupp2 * (natsupp2 == '03'),0), 
      flag_src= (natsupp1 == '03' | natsupp2 == '03'),
      suprep= pmax(nbsupp1 * (natsupp1 == '13'),0)+ pmax(nbsupp2 * (natsupp2 == '13'),0), 
      flag_rep= (natsupp1 == '13' | natsupp2 == '13'),
      supnn1= pmax(nbsupp1 * (natsupp1 == '04'),0) + pmax(nbsupp2 * (natsupp2 == '04'),0), 
      flag_nn1=(natsupp1 == '04' | natsupp2 == '04'),
      supnn2= pmax(nbsupp1 * (natsupp1 == '05'),0) + pmax(nbsupp2 * (natsupp2 == '05'),0), 
      flag_nn2= (natsupp1 == '05' | natsupp2 == '05'),
      supnn3= pmax(nbsupp1 * (natsupp1 == '06'),0) + pmax(nbsupp2 * (natsupp2 == '06'),0), 
      flag_nn3=(natsupp1 == '06' | natsupp2 == '06')
    )
    
  checks_coincide <- rsa_rum %>% 
    group_by(cle_rsa, norss) %>% 
    summarise_at(vars(starts_with('sup')), sum) %>% 
    ungroup() %>% 
    left_join(rsa$rsa %>% select(cle_rsa, ghm, nbsuprea, nbsupstf, 
                                 nbsupsrc, nbsuprep, nbsupnn1, nbsupnn2, nbsupnn3), by = 'cle_rsa') %>% 
    mutate(check_rea = suprea == nbsuprea,
           check_src = supsrc == nbsupsrc,
           check_stf = supstf == nbsupstf,
           check_rep = suprep == nbsuprep,
           check_nn1 = supnn1 == nbsupnn1,
           check_nn2 = supnn2 == nbsupnn2,
           check_nn3 = supnn3 == nbsupnn3)
  
  
  
  checks <- group_by_at(checks_coincide, vars(starts_with('check'))) %>% count()
  
  message('-- Check correspondance des suppléments RUM <-> RSA : ', ifelse(nrow(checks) == 1L, "ok", "nok !"))
  if (nrow(checks) > 1){
    checks
  }

  # passage unique par UM
  if (type_passage == "RUM") {
    rum_uma <- rsa_rum %>% group_by(cle_rsa, norss, cdurm, 
                                    typaut1, nseqrum)
  } else {
    rum_uma <- rsa_rum %>% 
      group_by(cle_rsa, norss, cdurm, typaut1)
  }
  
  rum_uma <- rum_uma %>% 
    summarise(dsrum = sum(dsrum),
              suprea = sum(suprea),
              supstf = sum(supstf),
              supsrc = sum(supsrc),
              supnn1 = sum(supnn1),
              supnn2 = sum(supnn2),
              supnn3 = sum(supnn3),
              suprep = sum(suprep),
              flag_rea = sum(flag_rea),
              flag_stf = sum(flag_stf),
              flag_src = sum(flag_src),
              flag_nn1 = sum(flag_nn1),
              flag_nn2 = sum(flag_nn2),
              flag_nn3 = sum(flag_nn3),
              flag_rep = sum(flag_rep)) %>% 
    ungroup() %>% 
    mutate(
      suprea = suprea + (flag_rea > 0),
      supstf = supstf + (flag_stf > 0),
      supsrc = supsrc + (flag_src > 0),
      suprep = suprep + (flag_rep > 0),
      supnn1 = supnn1 + (flag_nn1 > 0),
      supnn2 = supnn2 + (flag_nn2 > 0),
      supnn3 = supnn3 + (flag_nn3 > 0)) %>% 
    select(- starts_with('flag_')) %>% 
    group_by(cle_rsa, norss) %>% 
    mutate(suprea_prop = suprea / sum(suprea),
           supstf_prop = supstf / sum(supstf),
           supsrc_prop = supsrc / sum(supsrc),
           suprep_prop = suprep / sum(suprep),
           supnn1_prop = supnn1 / sum(supnn1),
           supnn2_prop = supnn2 / sum(supnn2),
           supnn3_prop = supnn3 / sum(supnn3)) %>% 
    ungroup() %>% 
    mutate_if(is.numeric, tidyr::replace_na, 0)
  
  
  rum_uma_valo <- rum_uma %>% 
    left_join(valo %>% select(cle_rsa, type_fin, ghm, nbrum, duree, rec_base, rec_exb, rec_exh, rec_bee, rec_po_tot, rec_rea, 
                              rec_stf, rec_src, rec_rep, rec_nn1, rec_rehosp_ghm, rec_sdc,
                              rec_nn2, rec_nn3, rec_ant, rec_dialhosp, rec_rdt_tot, rec_caishyp, rec_aph), by = 'cle_rsa')
  
  
  valo_mono <- rum_uma_valo %>% 
    filter(nbrum == 1L) %>% 
    mutate(nb_uma = 1L) %>% 
    #select(-rec_po_tot) %>% 
    mutate(rec_rea_rum         = rec_rea,
           rec_stf_rum         = rec_stf,
           rec_src_rum         = rec_src,
           rec_rep_rum         = rec_rep,
           rec_nn1_rum         = rec_nn1,
           rec_nn2_rum         = rec_nn2,
           rec_nn3_rum         = rec_nn3, 
           rec_bee_rum         = rec_bee, 
           rec_po_tot_rum      = rec_po_tot,
           rec_base_rum        = rec_base, 
           rec_exb_rum         = rec_exb, 
           rec_exh_rum         = rec_exh, 
           rec_ant_rum         = rec_ant, 
           rec_sdc_rum         = rec_sdc, 
           rec_caishyp_rum     = rec_caishyp,
           rec_rehosp_ghm_rum  = rec_rehosp_ghm,
           rec_aph_rum         = rec_aph) %>% 
  mutate(rec_totale_rum = rec_bee + rec_po_tot + rec_rea + rec_stf + 
           rec_src + rec_rep + rec_nn1 + rec_sdc +
           rec_nn2 + rec_nn3 + rec_ant + rec_aph + rec_caishyp + rec_rehosp_ghm) %>% 
    mutate(type_repartition = "mono-rum")
  
  message('-- Nb mono-rum ', nrow(valo_mono))
  
  if (is.na(pmct_mono)){
    pmct_um_dp_use <- pmct_um_dp %>% 
      filter(is.na(mono))
  } else {
    pmct_um_dp_use <- pmct_um_dp %>%
      filter(mono == TRUE)
  }
  
  message('-- Nb UM dans réf. PMCT : ', nrow(pmct_um_dp_use))
  message('-- Nb UM dans réf. PMCT seuillé : ', nrow(pmct_um_dp_use %>% filter(n >= seuil_pmct)))
  
  pmct_um_dp_use <- pmct_um_dp_use %>% filter(n >= seuil_pmct)
  
  valo_multi <- rum_uma_valo %>% 
    left_join(pmct_um_dp_use, by = c("cdurm", "typaut1")) %>% 
    filter(nbrum > 1L) %>% 
    group_by(norss) %>% 
    mutate(nb_uma = n(),
           check_pmct_ok = (sum(!is.na(pmct_um_dp)) == nb_uma),
           pmct_tot_parcours = sum(pmct_um_dp, na.rm = TRUE)) %>% 
    ungroup() %>% 
      mutate(p1 = p1,
             p2 = p2) %>%
    # si un des RUM n'a pas de pmct, on bascule sur le calcul au pro rata durée de séjour seul
    mutate(p1 = ifelse(!check_pmct_ok, 0, p1),
           p2 = ifelse(! check_pmct_ok, 1, p2),
           prop_pass = (dsrum + 1) / (duree + nb_uma),
           prop_pmct_um   = ifelse(check_pmct_ok, pmct_um_dp  / pmct_tot_parcours, 0)) %>% 
    mutate(rec_rea_rum = rec_rea * suprea_prop,
           rec_stf_rum = rec_stf * supstf_prop,
           rec_src_rum = rec_src * supsrc_prop,
           rec_rep_rum = rec_rep * suprep_prop,
           rec_nn1_rum = rec_nn1 * supnn1_prop,
           rec_nn2_rum = rec_nn2 * supnn2_prop,
           rec_nn3_rum = rec_nn3 * supnn3_prop, 
           rec_base_rum = rec_base * (prop_pass * p2 + prop_pmct_um * p1), 
           rec_exb_rum = rec_exb   * (prop_pass * p2 + prop_pmct_um * p1), 
           rec_exh_rum = rec_exh   * (prop_pass * p2 + prop_pmct_um * p1), 
           rec_bee_rum = rec_bee   * (prop_pass * p2 + prop_pmct_um * p1),
           rec_po_tot_rum = rec_po_tot * (prop_pass * p2 + prop_pmct_um * p1),
           rec_ant_rum = rec_ant * (prop_pass * p2 + prop_pmct_um * p1),
           rec_sdc_rum = rec_sdc * (prop_pass * p2 + prop_pmct_um * p1),
           rec_caishyp_rum = rec_caishyp * (prop_pass * p2 + prop_pmct_um * p1),
           rec_aph_rum = rec_aph * (prop_pass * p2 + prop_pmct_um * p1),
           rec_rehosp_ghm_rum = rec_rehosp_ghm * (prop_pass * p2 + prop_pmct_um * p1)) %>% 
    mutate(rec_totale_rum = rec_bee_rum + rec_po_tot_rum + rec_rea_rum + rec_stf_rum + 
                            rec_src_rum + rec_rep_rum + rec_nn1_rum + rec_sdc_rum + 
              rec_nn2_rum + rec_nn3_rum + rec_ant_rum + rec_aph_rum + rec_caishyp_rum + rec_rehosp_ghm_rum) %>% 
    mutate(type_repartition = "multi-rum")
  
  message('-- Nb multi-rum ', nrow(valo_multi %>% distinct(cle_rsa)))
  
  valo_rum <- bind_rows(valo_mono, valo_multi)
  
  message('-- Nb lignes : rum valo ', nrow(valo_rum))
  
  # Ici, on ventile les suppléments dialyses et rdth fictivement sur chaque passage dans la période
  # pour les dialyses, on splitte enfants / adultes
  # pour ventiler l'ensemble des recettes sur ces unitées dans certains reportings (recette annexes)
  # l'idée pourrait être de faire pareil pour le caisson hyperbare et les aphérèses sanguines.
  
  # cas des dialyses
  valo_unite_dial <- rum_uma_valo %>% 
    distinct(cle_rsa, norss, cdurm, dsrum, typaut1) %>% 
    filter(substr(typaut1,1,2) %in% c('21', '22', '23')) %>% 
    left_join(rsa$rsa %>% select(cle_rsa, agean), by = "cle_rsa") %>% 
    #bind_cols(tibble(rec_dialhosp_tot_hop = rep(sum(valo$rec_dialhosp), nrow(.)))) %>% 
    bind_cols(tibble(rec_dialhosp_tot_hop_enf = rep(sum(valo[is.na(valo$agean) | valo$agean < 16,]$rec_dialhosp), nrow(.)))) %>% 
    bind_cols(tibble(rec_dialhosp_tot_hop_adu = rep(sum(valo[! is.na(valo$agean) & valo$agean > 15,]$rec_dialhosp), nrow(.)))) %>% 
    group_by(ped = (agean < 16 | is.na(agean))) %>% 
    mutate(typaut1 = case_when(
      substr(typaut1,1,2) %in% c('21', '23') ~ '21',
      substr(typaut1,1,2)== '22' ~ '22')) %>% 
    add_count(typaut1, name = "nb_passage") %>% 
    mutate(rec_dialhosp_rum = case_when(
      typaut1 == '21' & !ped ~ rec_dialhosp_tot_hop_adu / nb_passage,
      typaut1 == '22' &  ped ~ rec_dialhosp_tot_hop_enf / nb_passage,
      TRUE ~ 0)) %>% 
    ungroup()
  

  # radiothérapie
  valo_unite_rdt <- rum_uma_valo %>% 
    distinct(cle_rsa, norss, cdurm, dsrum, typaut1) %>% 
    filter(substr(typaut1,1,2) == '42') %>% 
    bind_cols(tibble(rec_rdt_tot_hop = rep(sum(valo$rec_rdt_tot), nrow(.)))) %>% 
    add_count(typaut1, name = "nb_passage") %>% 
    ungroup() %>% 
    mutate(rec_rdt_tot_rum = ifelse(substr(typaut1,1,2) == '42', rec_rdt_tot_hop / nb_passage, 0))
  
  # radiothérapie pédiatrique
  valo_unite_rap <- rum_uma_valo %>% 
    distinct(cle_rsa, norss, cdurm, dsrum, typaut1) %>% 
    filter(substr(typaut1,1,2) == '42') %>% 
    bind_cols(tibble(rec_rap_tot_hop = rep(sum(valo$rec_rap), nrow(.)))) %>% 
    add_count(typaut1, name = "nb_passage") %>% 
    ungroup() %>% 
    mutate(rec_rap_rum = ifelse(substr(typaut1,1,2) == '42', rec_rap_tot_hop / nb_passage, 0))
  
    
  valo_rum <- valo_rum %>% 
    left_join(valo_unite_dial %>% select(cle_rsa, cdurm, rec_dialhosp_rum), by = c("cle_rsa", "cdurm")) %>% 
    left_join(valo_unite_rdt %>% select(cle_rsa, cdurm, rec_rdt_tot_rum), by = c("cle_rsa", "cdurm")) %>% 
    left_join(valo_unite_rap %>% select(cle_rsa, cdurm, rec_rap_rum), by = c("cle_rsa", "cdurm")) %>% 
    tidyr::replace_na(list(rec_dialhosp_rum = 0, rec_rdt_tot_rum = 0, rec_rap_rum = 0)) %>% 
    mutate(rec_annexes_rum = rec_dialhosp_rum + rec_rdt_tot_rum + rec_rap_rum,
           rec_totale_rum = rec_totale_rum + rec_annexes_rum + rec_rap_rum)
  
  message('-- Ventilation suppléments dialyses sur UM typaut 21, 22 et 23 : ', 
          round(mean(valo_rum$rec_dialhosp_rum[valo_rum$rec_dialhosp_rum > 0], na.rm = TRUE)), '€ en moyenne sur chaque RUM typaut 21, 22, ou 23 (recette annexes)')
  message('-- Ventilation suppléments radioth sur UM typaut 42 : ', 
          round(mean(valo_rum$rec_rdt_tot_rum[valo_rum$rec_rdt_tot_rum > 0], na.rm = TRUE)), '€ sur chaque RUM typaut 42 (recette annexes)')
  
  #  vvr_rum_check_rubriques_rav(valo, valo_rum)
  

  # valo_rum %>% 
  #   select(cle_rsa, nbrum, p1, p2, n, pmct_um_dp, nb_uma, norss, cdurm, typaut1, ends_with('_rum')) %>% 
  #   mutate_at(vars(starts_with('rec_')), tidyr::replace_na, 0)

  if (type_passage == "RUM"){
    return(valo_rum %>% select(cle_rsa, nseqrum, nbrum, p1, p2, n, pmct_um_dp,
                               nb_uma, norss, cdurm, typaut1, ends_with("_rum"), starts_with('sup'), starts_with("flag_")) %>%
             mutate_at(vars(starts_with("rec_")), tidyr::replace_na,
                       0))} else {
                         return(valo_rum %>% select(cle_rsa, nbrum, p1, p2, n, pmct_um_dp,
                                                    nb_uma, norss, cdurm, typaut1, ends_with("_rum"), starts_with('sup'), starts_with("flag_")) %>%
                                  mutate_at(vars(starts_with("rec_")), tidyr::replace_na,
                                            0))
                       }  
}


#' ~ VVR - Reproduire le tableau RAV (works)
#'
#' 
#' @param valo Un tibble résultant de \code{\link{vvr_mco}}
#' @param knit à TRUE, appliquer une sortie `knitr::kable` au résultat
#' 
#' @return Un tibble similaire au tableau RAV epmsi (montant BR* ou BR si coeff prud = 1)
#'
#' @examples
#' \dontrun{
#' epmsi_mco_rav2(valo)
# 
#' }
#'
#' @author G. Pressiat
#'
#' @export
#' @export epmsi_mco_rav2
epmsi_mco_rav2 <- function(valo, theorique = TRUE){
  if (theorique){
  rr <- valo %>% 
    dplyr::select(dplyr::starts_with('rec_'), -rec_totale, -rec_bee) %>% 
    dplyr::mutate(rec_exb = -rec_exb) %>% 
    summarise_all(sum) %>% 
    tidyr::gather(var, val) %>% 
    filter(abs(val) > 0) %>% 
    inner_join(vvr_libelles_valo('lib_valo'), by = "var") %>% 
    group_by(ordre_epmsi, var, lib_valo) %>% 
    summarise(val = round(sum(val),0)) %>% 
    ungroup()
  } else {
    rr <- valo %>% 
      dplyr::select(cle_rsa, dplyr::starts_with('rec_'), type_fin, -rec_totale, -rec_bee) %>% 
      dplyr::mutate(rec_exb = -rec_exb) %>% 
      group_by(type_fin, cle_rsa) %>% 
      summarise_all(sum) %>% 
      tidyr::gather(var, val, - type_fin, - cle_rsa) %>% 
      filter(abs(val) > 0) %>% 
      mutate(flag_po = sum(("rec_po_tot" == var)) > 0) %>% 
      ungroup() %>% 
      mutate(val_reelle = case_when((type_fin == 0L | flag_po) ~ val,
                                    TRUE ~ 0)) %>% 
      filter(val_reelle != 0L) %>% 
      inner_join(vvr_libelles_valo('lib_valo')) %>% 
      group_by(ordre_epmsi, var, lib_valo) %>% 
      summarise(val = round(sum(val_reelle),0)) %>% 
      ungroup() %>% 
      bind_rows(summarise_if(., is.numeric, sum))
  }
  rr
}


#' ~ VVR - Synthese des rubriques de valo apres valo des RUM
#'
#' 
#' @param valo_rum Un tibble résultant de \code{\link{vvr_rum}}
#' 
#' @return Un tibble similaire au tableau RAV epmsi
#'
#' @examples
#' \dontrun{
#' epmsi_mco_rav_rum(valo)
# 
#' }
#'
#' @author G. Pressiat
#'
#' @seealso  epmsi_mco_rav, epmsi_mco_rav_rum, epmsi_mco_rav2,
#' @export epmsi_mco_rav_rum
epmsi_mco_rav_rum <- function(valo_rum, theorique = TRUE){
  if (theorique){
  rr <- valo_rum %>% 
    dplyr::select(dplyr::ends_with('_rum')) %>% 
      dplyr::mutate(rec_exb_rum = -rec_exb_rum) %>% 
    summarise_all(sum) %>% 
    tidyr::gather(var, val) %>% 
    filter(abs(val) > 0) %>% 
    mutate(var = stringr::str_remove(var, '_rum')) %>% 
    left_join(vvr_libelles_valo('lib_valo'), by = "var") %>% 
    group_by(ordre_epmsi, var, lib_valo) %>% 
    summarise(val = round(sum(val),0)) %>% 
    ungroup()  -> rr_rum
     
  } else {
    rr <- valo_rum %>% 
      dplyr::select(cle_rsa, type_fin, dplyr::ends_with('_rum')) %>% 
      dplyr::mutate(rec_exb_rum = -rec_exb_rum) %>% 
      group_by(type_fin, cle_rsa) %>% 
      summarise_all(sum) %>% 
      tidyr::gather(var, val, - type_fin, - cle_rsa) %>% 
      filter(abs(val) > 0) %>% 
      mutate(flag_po = sum(("rec_po_rum" == var)) > 0) %>% 
      ungroup() %>% 
      mutate(val_reelle = case_when((type_fin == 0L | flag_po) ~ val,
                                    TRUE ~ 0)) %>% 
      filter(val_reelle != 0L) %>% 
      mutate(var = stringr::str_remove(var, '_rum')) %>% 
      left_join(vvr_libelles_valo('lib_valo')) %>% 
      group_by(ordre_epmsi, var, lib_valo) %>% 
      summarise(val = round(sum(val_reelle),0)) %>% 
      ungroup()  -> rr_rum
    
  }
  rr
}

#' ~ VVR - Confronter la valo rum a la valo rsa par rubrique epmsi
#'
#' 
#' @param valo Un tibble résultant de \code{\link{vvr_mco}}
#' @param valo_rum Un tibble résultant de \code{\link{vvr_rum}}
#' 
#' @return Un tibble similaire au tableau RAV epmsi avec deux colonnes (rsa et rum)
#'
#' @examples
#' \dontrun{
#' vvr_rum_check_rubriques_rav(valo, valo_rum)
# 
#' }
#'
#' @author G. Pressiat
#'
#' @seealso  epmsi_mco_rav, epmsi_mco_rav_rum, epmsi_mco_rav2,
#' @export vvr_rum_check_rubriques_rav
vvr_rum_check_rubriques_rav <- function(valo, valo_rum){
  epmsi_mco_rav2(valo) %>% 
    left_join(epmsi_mco_rav_rum(valo_rum), 
              by = c('ordre_epmsi', 'lib_valo', 'var'), 
              suffix = c('_rsa', '_rum')) %>% 
    mutate_if(is.numeric, tidyr::replace_na, 0) %>% 
    mutate(delta = val_rum - val_rsa)
}

