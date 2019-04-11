

#' ~ VVR - Attribuer les recettes GHT sur des rapss
#'
#' Reproduire la valorisation BR et coefficients géo/prudentiels du tableau VALR d'epmsi
#' 
#' Cette fonction ne tient pas compte du fichier de conventions ESMS (cas des finess non conventionnés non pec ici)
#' 
#' 
#' @param p Un noyau de paramètres créé avec \code{\link{noyau_pmeasyr}}
#' @param ghts Un tibble contenant une ligne par tarif GHT - année séquentielle des tarifs - type de domicile
#' @param coeff_geo Coefficient géographique, au choix (peut être mis à 1)
#' @param coeff_prudent Coefficient prudentiel, par défaut la fonction créé ce coefficient automatiquement, sinon il peut-être mis à 1 ou autre
#' @return Un tibble contenant les valorisations GHT des sous-séquences de la table rapss$ght PAPRICA (et donc des séjours), les coefficient géo et prudentiels
#' sont présents dans la table, la colonne tarif n'en tient pas compte, et il faut multiplier par le nb journées GHT pour obtenir la valorisation totale.
#' 
#' @examples
#' \dontrun{
#' library(pmeasyr)
#' library(dplyr, warn.conflicts = F)
#' 
#' p <- noyau_pmeasyr(
#'   finess = '750712184',
#'   annee  = 2018,
#'   mois   = 12,
#'   path   = '~/Documents/data/had',
#'   progress = FALSE,
#'   tolower_names = TRUE,
#'   lib = FALSE
#' )
#' 
#' adezip(p, type = "out", liste = c('rapss', 'ano'))
#' 
#' library(nomensland)
#' ghts <- get_table('tarifs_had_ght') %>%
#'   tidyr::gather(type_tarif, tarif, - paprica_numght, - lib_ght) %>%
#'   mutate(anseqta = stringr::str_extract(type_tarif, '[0-9]{4}'),
#'          typdom    = substr(type_tarif, 6, nchar(type_tarif)))
#' 
#' 
#' # Utiliser cette fonction
#' base_ght <- vvr_had_ght(p, ghts)
#' 
#' 
#' 
#' ano <- iano_had(p)
#' 
#' library(stringfix)
#' # calculer le montant Base remboursement et le nb de journées valorisées
#' base_ght %>% 
#'   inner_join(distinct(ano, noseqsej, .keep_all = TRUE), by = 'noseqsej') %>%
#'   # filtre sur séjours facturables
#'   filter(factam %in% c('1', '2')) %>% 
#'   mutate(tarif = tarif * joursght * cgeo * cprudent) %>% 
#'   summarise(
#'     euros =  sum(tarif, na.rm = TRUE) %>% round(.,2) %>% 
#'        formatC(., big.mark = " ", format = "f", digits = 2) %+% "€",
#'     nbj = sum(joursght))
#' 
#' ##| euros      | nbj   |
#' ##|------------|-------|
#' ##| xxx xxx,xx€| x xxx |
#'}
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}}, \code{\link{iano_had}}
#' @export vvr_had_ght
#' @export
vvr_had_ght <- function(p, ghts, coeff_geo = 1.07, coeff_prudent = NULL){
  
  rapss <- irapss(p)
  base_ght <- rapss$ght %>% 
    dplyr::filter(typght == 'PAPRICA') %>%
    dplyr::select(noseqsej, noseq, nosousseq, numght, joursght) %>% 
    dplyr::inner_join(rapss$rapss %>% 
                        dplyr::select(noseqsej, noseq, nosousseq, mois_fin_ss_seq, typdom) %>%
                        dplyr::mutate(typdom = dplyr::case_when(typdom %in% c('1', '2', '5') ~ "",
                                           typdom %in% c('3', '4') ~ "esms",
                                           typdom == "6" ~ "ssiad")), 
               by = c("noseqsej", "noseq", "nosousseq")) %>%
    dplyr::mutate(anseqta = ifelse(mois_fin_ss_seq %in% c('01', '02'), p$annee - 1, p$annee) %>% as.character()) %>%
    dplyr::mutate(cgeo = coeff_geo,
           cprudent = case_when(
             anseqta == '2019'    ~ 0.9930,
             anseqta == '2018'    ~ 0.9930,
             anseqta == '2017'    ~ 0.9930,
             anseqta == '2016'    ~ 0.9950,
             anseqta == '2015'    ~ 0.9965,
             anseqta == '2014'    ~ 0.9965,
             anseqta == '2013'    ~ 0.9965,
             anseqta == "2012"    ~ 0.9965,
             anseqta == "2011"    ~ 0.9965
           )) %>% 
    dplyr::left_join(ghts, by = c('numght' = 'paprica_numght', 'anseqta', "typdom"))
  
  if (!is.null(coeff_prudent)){
    base_ght <- base_ght %>% 
      dplyr::mutate(cprudent = coeff_prudent)
  }
  if (coeff_geo == 1){
    base_ght <- base_ght %>% 
      dplyr::mutate(cgeo = 1)
  }
  base_ght
}

