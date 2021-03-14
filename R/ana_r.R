

#' ~ ANA - Chirurgie ambulatoire : 55 gestes marqueurs
#' 
#' 
#' @param p Noyau de paramètres
#' @param periode paramètres année et mois de l'envoi
#' @param gestes_marqueurs liste de requêtes des gestes marqueurs (nomensland)
#' 
#'
#' @examples
#' \dontrun{
#' 
#' library(dplyr, warn.conflicts = FALSE)
#' library(pmeasyr)
#' 
#' p <- noyau_pmeasyr(finess = '290000017',
#'                    annee  = 2018,
#'                    mois   = 12,
#'                    path   = '~/Documents/data/mco', 
#'                    tolower_names = TRUE,
#'                    n_max = Inf)
#' 
#' 
#' library(nomensland)
#' 
#' dicts <- get_dictionnaire_listes()
#' lgm <- get_all_listes('Chir ambu : 55 GM')
#' 
#' periodes <- list(
#'   list(an = 2013, moi = 12),
#'   list(an = 2014, moi = 12),
#'   list(an = 2015, moi = 12),
#'   list(an = 2016, moi = 12),
#'   list(an = 2017, moi = 12),
#'   list(an = 2018, moi = 12),
#'   list(an = 2019, moi = 11))
#' 
#' result <- periodes %>% purrr::map_dfr(ana_r_ca_gestes_marqueurs, p = p, gestes_marqueurs = lgm)
#' result <- result %>% arrange(`Geste marqueur`, `Période`)
#' 
#' pivot_result <- result %>% 
#'   select(`Geste marqueur`, nofiness, taux_ambu, `Nb total`, `Période`) %>% 
#'   mutate(stat = paste0(scales::percent(taux_ambu), ' (', `Nb total`, ')')) %>% 
#'   select(-taux_ambu, - `Nb total`) %>% 
#'   tidyr::spread(`Période`, stat, '')
#' 
#' }
#' 
#' @return Taux de chirurgie ambulatoire et DMS > 0 nuit des gestes marqueurs par finess géographique
#' @importFrom scales percent
#' @export
ana_r_ca_gestes_marqueurs <- function(p, periode = list(an = 2018, moi = 12), gestes_marqueurs){
  
  # an <- periode$an %>% unlist
  # moi <- periode$moi %>% unlist
  
  message('----- Période : ', periode$an, ' M', periode$moi)
  message('-- Import rsa')
  rsa <- pmeasyr::irsa(p, annee = periode$an, mois = periode$moi, typi = 6)
  rsa <- pmeasyr::prepare_rsa(rsa)
  
  message('-- Requêtes ambu GM')
  # Lancement des requêtes et ajout du site géographique du RUM fourinissant le DP
  gm <- pmeasyr::lancer_requete(rsa, gestes_marqueurs, vars = c('duree', 'ansor', 'noseqrum', 'nofiness')) %>% 
    dplyr::left_join(rsa$rsa_um %>% dplyr::select(nohop1, cle_rsa, nseqrum), by = c('cle_rsa', 'noseqrum' = 'nseqrum'))
  
  
  gm <- gm %>% dplyr::mutate(classe_duree = 
                               dplyr::case_when(duree == 0 ~ "0 nuit",
                                  duree == 1 ~ "1 nuit",
                                  duree == 2 ~ "2 nuits",
                                  duree > 2  ~ "3 nuits et +")) %>% 
    dplyr::mutate(ansor = paste0(ansor, ' M', stringr::str_pad(p$mois, 2, "left", '0')))
  
  stat_ambu <- function(ggmm){
    ggmm %>% 
      dplyr::group_by(nofiness, ansor, Requete, classe_duree) %>% 
      dplyr::summarise(Nb = n(),
                `Nuitées` = sum(duree)) %>% 
      tidyr::gather(var, val, - Requete, - classe_duree, - nofiness, - ansor) %>% 
      tidyr::unite(var, var, classe_duree, sep = " ") %>% 
      tidyr::spread(var, val, fill = 0) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(`Nuitées totales` = rowSums(.[grep("Nuitées", names(.))])) %>% 
      dplyr::select(nofiness, ansor, Requete, starts_with('Nb'), `Nuitées 3 nuits et +`, `Nuitées totales`) %>% 
      dplyr::mutate(`Nb total` = rowSums(.[grep("Nb", names(.))])) %>% 
      dplyr::mutate(taux_ambu = `Nb 0 nuit` / `Nb total`, 
             `DMS hors ambu` = round(`Nuitées totales` / (`Nb total` - `Nb 0 nuit`), 1))
  }
  
  message('-- Stats ambu GM')
  stat_gm <- dplyr::bind_rows(stat_ambu(gm), 
                       stat_ambu(gm %>% 
                                   dplyr::select(-nofiness) %>% 
                                   dplyr::rename(nofiness = nohop1)))
  
  stat_gm %>% 
    dplyr::arrange(Requete) %>% 
    dplyr::rename(`Geste marqueur` = Requete, `Période` = ansor)
  
}



#' ~ ANA - Analyse des taux ambulatoires et DMS sur un périmètre GHM à définir
#' 
#' 
#' @param p Noyau de paramètres
#' @param periode paramètres année et mois de l'envoi
#' @param requete liste de requêtes à définir (nomensland)
#' 
#'
#' @examples
#' \dontrun{
#' 
#' library(dplyr, warn.conflicts = FALSE)
#' library(pmeasyr)
#' 
#' p <- noyau_pmeasyr(finess = '290000017',
#'                    annee  = 2018,
#'                    mois   = 12,
#'                    path   = '~/Documents/data/mco', 
#'                    tolower_names = TRUE,
#'                    n_max = Inf)
#' 
#' 
#' library(nomensland)
#' 
#' ghmc_7r <- get_liste('chir_ambu_ghm_C_7_racines')
#' 
#' periodes <- list(
#'   list(an = 2013, moi = 12),
#'   list(an = 2014, moi = 12),
#'   list(an = 2015, moi = 12),
#'   list(an = 2016, moi = 12),
#'   list(an = 2017, moi = 12),
#'   list(an = 2018, moi = 12),
#'   list(an = 2019, moi = 11))
#' 
#' result <- periodes %>% purrr::map_dfr(ana_r_ghm_ambu_dms, p = p, requete = ghmc_7r)
#' result <- result %>% arrange(niveau, Requete, `Période`)
#' 
#' 
#' pivot_result <- result %>% 
#'   select(niveau, Requete, nofiness, taux_ambu, `Nb total`, `Période`) %>% 
#'   mutate(stat = paste0(scales::percent(taux_ambu), ' (', `Nb total`, ')')) %>% 
#'   select(-taux_ambu, - `Nb total`) %>% 
#'   tidyr::spread(`Période`, stat, '')
#' 
#' 
#' ghmk <- list(nom = 'GHM K', rsatype = 'K', Thematique = 'GHM K', abrege = 'ghmk')
#' 
#' lancer_requete(rsa, ghmk)
#' 
#' 
#' }
#' 
#' @return Taux ambulatoire et DMS > 0 nuit par finess géographique, au globa, par racine et par DA
#' @export
ana_r_ghm_ambu_dms <- function(p, periode = list(an = 2018, moi = 12), requete){
  
  message('----- Période : ', periode$an, ' M', periode$moi)
  message('-- Import rsa')
  
  rsa <- pmeasyr::irsa(p, annee = periode$an, mois = periode$moi, typi = 6)
  rsa <- pmeasyr::prepare_rsa(rsa)
  
  message('-- Requêtes GHM C')
  # Lancement des requêtes et ajout du site géographique du RUM fourinissant le DP
  ghmc <- pmeasyr::lancer_requete(rsa, requete, vars = c('duree', 'ansor', 'noseqrum', 'nofiness', 'ghm', 'anseqta')) %>% 
    dplyr::left_join(rsa$rsa_um %>% dplyr::select(nohop1, cle_rsa, nseqrum), by = c('cle_rsa', 'noseqrum' = 'nseqrum'))
  
  ghmc <- ghmc %>% dplyr::mutate(classe_duree = 
                                   dplyr::case_when(duree == 0 ~ "0 nuit",
                                      duree == 1 ~ "1 nuit",
                                      duree == 2 ~ "2 nuits",
                                      duree == 3 ~ "3 nuits",
                                      duree == 4 ~ "4 nuits",
                                      duree > 4  ~ "5 nuits et +")) %>% 
    dplyr::mutate(ansor = paste0(ansor, ' M', stringr::str_pad(periode$moi, 2, "left", '0')))
  
  stat_ambu <- function(ghmc){
    global_requete <- ghmc %>% 
      dplyr::group_by(nofiness, ansor, Requete, classe_duree) %>% 
      dplyr::summarise(Nb = n(),
                `Nuitées` = sum(duree)) %>% 
      tidyr::gather(var, val, - Requete, - classe_duree, - nofiness, - ansor) %>% 
      tidyr::unite(var, var, classe_duree, sep = " ") %>% 
      tidyr::spread(var, val, fill = 0) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(`Nuitées totales` = rowSums(.[grep("Nuitées", names(.))])) %>% 
      dplyr::select(nofiness, ansor, Requete, starts_with('Nb'), `Nuitées 5 nuits et +`, `Nuitées totales`) %>% 
      dplyr::mutate(`Nb total` = rowSums(.[grep("Nb", names(.))])) %>% 
      dplyr::mutate(taux_ambu = `Nb 0 nuit` / `Nb total`, 
             `DMS hors ambu` = round(`Nuitées totales` / (`Nb total` - `Nb 0 nuit`), 1))
    
    lrghm <- get_table('ghm_rghm_regroupement', version = unique(rsa$rsa$anseqta)) %>% 
      dplyr::distinct(anseqta, racine, da, libelle_da, libelle_racine) %>% 
      tidyr::unite(da, da, libelle_da, sep = " - ")
    
    par_racine_requete <- ghmc %>%
      dplyr::mutate(rghm = substr(ghm,1,5)) %>% 
      dplyr::left_join(lrghm, by = c('anseqta', 'rghm' = 'racine')) %>% 
      dplyr::group_by(nofiness, ansor, Requete = paste0(rghm, ' - ', libelle_racine), classe_duree) %>%
      dplyr::summarise(Nb = n(),
                `Nuitées` = sum(duree)) %>%
      tidyr::gather(var, val, - Requete, - classe_duree, - nofiness, - ansor) %>%
      tidyr::unite(var, var, classe_duree, sep = " ") %>%
      tidyr::spread(var, val, fill = 0) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(`Nuitées totales` = rowSums(.[grep("Nuitées", names(.))])) %>%
      dplyr::select(nofiness, ansor, Requete, starts_with('Nb'), `Nuitées 5 nuits et +`, `Nuitées totales`) %>%
      dplyr::mutate(`Nb total` = rowSums(.[grep("Nb", names(.))])) %>%
      dplyr::mutate(taux_ambu = `Nb 0 nuit` / `Nb total`,
             `DMS hors ambu` = round(`Nuitées totales` / (`Nb total` - `Nb 0 nuit`), 1))
    
    par_da_requete <- ghmc %>%
      dplyr::mutate(rghm = substr(ghm,1,5)) %>% 
      dplyr::left_join(lrghm, by = c('anseqta', 'rghm' = 'racine')) %>% 
      dplyr::group_by(nofiness, ansor, Requete = da, classe_duree) %>%
      dplyr::summarise(Nb = n(),
                `Nuitées` = sum(duree)) %>%
      tidyr::gather(var, val, - Requete, - classe_duree, - nofiness, - ansor) %>%
      tidyr::unite(var, var, classe_duree, sep = " ") %>%
      tidyr::spread(var, val, fill = 0) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(`Nuitées totales` = rowSums(.[grep("Nuitées", names(.))])) %>%
      dplyr::select(nofiness, ansor, Requete, starts_with('Nb'), `Nuitées 5 nuits et +`, `Nuitées totales`) %>%
      dplyr::mutate(`Nb total` = rowSums(.[grep("Nb", names(.))])) %>%
      dplyr::mutate(taux_ambu = `Nb 0 nuit` / `Nb total`,
             `DMS hors ambu` = round(`Nuitées totales` / (`Nb total` - `Nb 0 nuit`), 1))
    
    dplyr::bind_rows(global_requete %>% mutate(niveau = "1 - Global"), 
              par_da_requete %>% mutate(niveau = "2 - Domaine d'activité"),
              par_racine_requete %>% mutate(niveau = "3 - Racine de GHM"))
    
  }
  
  message('-- Stats ambu GHM')
  stat_gm <- dplyr::bind_rows(stat_ambu(ghmc), 
                       stat_ambu(ghmc %>% 
                                   dplyr::select(-nofiness) %>% 
                                   dplyr::rename(nofiness = nohop1)))
  
  stat_gm %>% 
    dplyr::arrange(Requete) %>% 
    dplyr::rename(`Période` = ansor) %>% 
    dplyr::select(niveau, Requete, `Période`, nofiness, everything())
  
}

