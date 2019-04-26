


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
    dplyr::select(cle_rsa, duree, rsacmd, ghm, typesej, noghs, moissor,sexe, agean, agejr,
           anseqta, nbjrbs, nbjrexb, sejinfbi, agean, agejr, 
           dplyr::starts_with('nbsup'), dplyr::starts_with('sup'),
           nb_rdth, nbacte9615,
           echpmsi, prov, dest, schpmsi,
           rdth, nb_rdth, nbrum) %>%
    dplyr::left_join(rsa$rsa_um %>% 
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
    dplyr::select(cle_rsa, duree, rsacmd, ghm, typesej, noghs, moissor,sexe, agean, agejr,
           anseqta, nbjrbs, nbjrexb, sejinfbi, agean, agejr, 
           dplyr::starts_with('nbsup'), dplyr::starts_with('sup'),
           nb_rdth, nbacte9615,
           echpmsi, prov, dest, schpmsi,
           rdth, nb_rdth, nbrum) %>% 
    dplyr::left_join(pmeasyr::tbl_mco(con, an, 'rsa_um') %>% 
                       dplyr::filter(substr(typaut1, 1, 2) == '07') %>% 
                       dplyr::distinct(cle_rsa) %>%
                       dplyr::mutate(uhcd = 1), by = 'cle_rsa') %>%
    dplyr::mutate(monorum_uhcd = (uhcd == 1 & nbrum == 1)) %>%
    dplyr::select(-uhcd) %>%
    dplyr::collect()
  
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
                         bee = TRUE) {
  
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
  
  if (is.null(prudent)){
    # Partie GHS
    rsa_2 <- rsa %>%
      dplyr::mutate(cprudent = dplyr::case_when(
        anseqta == '2019'    ~ 0.9930,
        anseqta == '2018'    ~ 0.9930,
        anseqta == '2017'    ~ 0.9930,
        anseqta == '2016'    ~ 0.9950,
        anseqta == '2015'    ~ 0.9965,
        anseqta == '2014'    ~ 0.9965,
        anseqta == '2013'    ~ 0.9965,
        TRUE    ~ 1)) %>%
      dplyr::left_join(tarifs %>% dplyr::select(-ghm), by = c('noghs' = 'ghs', 'anseqta' = 'anseqta')) %>%
      dplyr::mutate(
        t_base  = tarif_base                 * cgeo * cprudent,
        t_haut  = nbjrbs * tarif_exh * cgeo  * cprudent,
        t_bas   = dplyr::case_when(
          sejinfbi == '2' ~ (nbjrexb / 10) * tarif_exb   * cgeo * cprudent,
          sejinfbi == '1' ~ forfait_exb                  * cgeo * cprudent,
          sejinfbi == '0' ~ 0),
        rec_bee = (t_base + t_haut - t_bas),
        rec_totale = rec_bee)
  } else {
    # Partie GHS
    rsa_2 <- rsa %>%
      mutate(cprudent = prudent) %>%
      dplyr::left_join(tarifs %>% dplyr::select(-ghm), by = c('noghs' = 'ghs', 'anseqta' = 'anseqta')) %>%
      dplyr::mutate(
        t_base  = tarif_base                 * cgeo * cprudent,
        t_haut  = nbjrbs * tarif_exh * cgeo  * cprudent,
        t_bas   = dplyr::case_when(
          sejinfbi == '2' ~ (nbjrexb / 10) * tarif_exb   * cgeo * cprudent,
          sejinfbi == '1' ~ forfait_exb                  * cgeo * cprudent,
          sejinfbi == '0' ~ 0),
        rec_bee = (t_base + t_haut - t_bas),
        rec_totale = rec_bee)
  }
  
  if (bee == TRUE){
    return(rsa_2 %>% dplyr::select(cle_rsa, rec_totale, rec_base = t_base, rec_exb =  t_bas, rec_exh = t_haut, rec_bee))
  }
  
  
  
  # Info suppléments ghs radiothérapie
  rsa_i <- rsa %>% dplyr::select(cle_rsa, rdth, nb_rdth) %>%
    dplyr::mutate(lrdh = stringr::str_extract_all(rdth, '.{7}'))
  
  rdth <- purrr::flatten_chr(rsa_i$lrdh) %>% stringr::str_trim()
  df <- rsa_i %>% dplyr::select(cle_rsa, nb_rdth)
  df <- as.data.frame(lapply(df, rep, df$nb_rdth), stringsAsFactors = F) %>% dplyr::tbl_df()
  rdth <- dplyr::bind_cols(df,data.frame(r = rdth, stringsAsFactors = F) ) %>% dplyr::tbl_df()
  
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
      return(rsa_3 %>% dplyr::select(cle_rsa, moissor, anseqta, rec_totale, rec_bee, rec_base = t_base, rec_exb =  t_bas, rec_exh = t_haut,
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
    dplyr::left_join(dplyr::select(bloq1, cle_rsa, typvidhosp) %>% dplyr::mutate(bloq = 1), by = 'cle_rsa') 
  
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
      ~var,                                                        ~lib_valo,
      "rec_totale",                                  "Valorisation 100% T2A globale",
      "rec_base",                                   "Valorisation des GHS de base",
      "rec_bee",                                   "Valorisation base + exb + exh",
      "rec_exb",                           "Valorisation extrême bas (à déduire)",
      "rec_rehosp_ghm",                 "Valorisation séjours avec rehosp dans même GHM",
      "rec_mino_sus",  "Valorisation séjours avec minoration forfaitaire liste en sus",
      "rec_exh",                             "Valorisation journées extrême haut",
      "rec_aph",                         "Valorisation actes GHS 9615 en Hospit.",
      "rec_rap",             "Valorisation suppléments radiothérapie pédiatrique",
      "rec_ant",                            "Valorisation suppléments antepartum",
      "rec_rdt_tot",                             "Valorisation actes RDTH en Hospit.",
      "rec_rea",                        "Valorisation suppléments de réanimation",
      "rec_rep",                    "Valorisation suppléments de réa pédiatrique",
      "rec_nn1",                     "Valorisation suppléments de néonat sans SI",
      "rec_nn2",                     "Valorisation suppléments de néonat avec SI",
      "rec_nn3",                 "Valorisation suppléments de réanimation néonat",
      "rec_po_tot",                             "Valorisation prélévements d organe",
      "rec_caishyp",           "Valorisation des actes de caissons hyperbares en sus",
      "rec_dialhosp",                            "Valorisation suppléments de dialyse",
      "rec_src",              "Valorisation suppléments de surveillance continue",
      "rec_stf",                    "Valorisation suppléments de soins intensifs",
      "rec_sdc",                "Valorisation supplément défibrilateur cardiaque"
    )
  
  return(list(lib_valo = lib_valo, lib_type_sej = lib_type_sej, lib_vidhosp = lib_vidhosp)[[wich]])
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
    dplyr::summarise(n = n(),
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
#' @author G. Pressiat
#'
#' @export epmsi_mco_rav
#' @export
epmsi_mco_rav <- function(valo, knit = FALSE){
  rr <- valo %>% dplyr::select(cle_rsa, dplyr::starts_with('rec_'), type_fin, -rec_totale, -rec_bee) %>% 
    dplyr::mutate(rec_exb = -rec_exb) %>% 
    tidyr::gather(var, val, - cle_rsa, - type_fin) %>%
    dplyr::filter(abs(val) > 0) %>%
    tidyr::spread(type_fin, val, fill = 0) %>% 
    dplyr::mutate(`0` = ifelse(var == "rec_po_tot", `0` + `8`, `0`)) %>% 
    select(cle_rsa, var, val = `0`) %>% 
    tidyr::gather(type_fin, val, - cle_rsa, - var) %>% 
    dplyr::inner_join(vvr_libelles_valo('lib_valo'), by = "var") %>%
    dplyr::group_by(lib_valo, var) %>%
    dplyr::summarise(n = sum(val != 0),
                     v = sum(val)) %>% 
    ungroup() %>% 
    arrange(lib_valo) %>% 
    dplyr::bind_rows(tibble(lib_valo = "Total valorisation 100% T2A", 
                            var = "rec_totale", 
                            n = sum(valo$rec_totale != 0),
                            v = sum(.$v))) %>% 
    dplyr::mutate(n = formatC(n, format = "f", big.mark = " ", digits = 0),
                  v = formatC(v, format = "f", big.mark = " ", decimal.mark = ",", digits = 3) %>% paste0('€'))
  
  if (knit){
    return(knitr::kable(rr))
  } else {
    return(rr)
  }
}

# 
# annee = 17
# 
# library(pmeasyr)
# p <- noyau_pmeasyr(
#   finess   = '750712184',
#   annee    = 2000 + annee,
#   mois     = 12,
#   path     = '~/Documents/data/mco',
#   progress = FALSE,
#   n_max    = Inf,
#   lib      = FALSE,
#   tolower_names = TRUE)
# 
# 
# 
# library(MonetDBLite)
# library(dplyr, warn.conflicts = F)
# 
# dbdir <- "~/Documents/data/monetdb"
# con <- src_monetdblite(dbdir)
# 
# 
# vrsa <- vvr_rsa(con, annee)
# vano <- vvr_ano_mco(con, annee)
# 
# # vrsa <- vvr_rsa(p)
# # vano <- vvr_ano_mco(p)
# 
# adezip(p, type = "out", liste = "porg")
# adezip(p, type = "out", liste = c("porg", "diap"))
# 
# resu <- vvr_mco(
#   vvr_ghs_supp(vrsa, tarifs),
#   vvr_mco_sv(vrsa, vano)
# )
# 
# resu %>% 
#   group_by(type_fin) %>% 
#   summarise(n = n(),
#             rec = sum(rec_totale)) %>% 
#   left_join(lib_type_sej, by = c('type_fin' = 'type_fin'))
# 
# p <- noyau_pmeasyr(
#   finess   = '750712184',
#   annee    = 2000 + 18,
#   mois     = 4,
#   path     = '~/Documents/data/mco',
#   progress = FALSE,
#   n_max    = Inf,
#   lib      = FALSE,
#   tolower_names = TRUE)
# 
# 
# vrsa <- vvr_rsa(p)
# vano <- vvr_ano_mco(p)
# 
# 
# adezip(p, type = "out", liste = "porg")
# adezip(p, type = "out", liste = c("porg", "diap"))
# 
# # resu <- vvr_mco(
# #   vvr_ghs_supp(vrsa, tarifs, supplements, vano, ipo(p), idiap(p), bee = FALSE),
# #   vvr_mco_sv(vrsa, vano, porg = ipo(p))
# #   )
# 
# epmsi_mco_sv <- function(valo){
#   valo %>% 
#     group_by(type_fin) %>% 
#     summarise(n = n(),
#               rec = sum(rec_totale)) %>% 
#     left_join(vvr_libelles_valo('lib_type_sej'), by = c('type_fin' = 'type_fin'))
# }
# 
# epmsi_mco_rav <- function(valo){
#   valo %>% select(cle_rsa, starts_with('rec_'), type_fin) %>% 
#     tidyr::gather(var, val, - cle_rsa, - type_fin) %>%
#     filter(abs(val) > 1e-7,!is.na(val)) %>%
#     left_join(vvr_libelles_valo('lib_type_sej'), by = "type_fin") %>%
#     left_join(vvr_libelles_valo('lib_valo'), by = "var") %>%
#     group_by(lib_type, lib_valo, var) %>%
#     summarise(n = n(),
#               v = sum(val))
# }
# 
# 
# epmsi_mco_sv(resu)
# epmsi_mco_rav(resu)
