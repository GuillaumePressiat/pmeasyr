#' ~ MCO - Comprendre le tableau epmsi 1.V.2.VMED - F
#' 
#' 
#' @param p Noyau de paramètres
#' @param ref_indic table des indications ATIH
#' 
#'
#' @examples
#' \dontrun{
#' library(pmeasyr)
#' library(dplyr, warn.conflicts = FALSE)
#' 
#' 
#' p <- noyau_pmeasyr(
#'   finess = '123456789',
#'   annee  = 2019,
#'   mois   = 3,
#'   path   = '~/Documents/data/mco',
#'   tolower_names = TRUE,
#'   lib = FALSE, 
#'   progress = FALSE
#' )
#' 
#' # devtools::install_github('GuillaumePressiat/nomensland')
#' library(nomensland)
#' ref_indic <- get_table('mco_medref_atih_indications') %>% 
#'   mutate(mois = substr(anseqta,1,2), 
#'          annee = substr(anseqta, 3,6)) 
#' vmed_f <- epmsi_mco_vmed_f(p, 
#'                            ref_indic)
#' }
#' 
#' @return Liste de deux tables : tableau ePMSI et détail avec NAS, UCD, Indcations, etc.
#' @export
epmsi_mco_vmed_f <- function(p, ref_indic) {
  m <- imed_mco(p)
  a <- vvr_ano_mco(p)
  type_valo <- vvr_mco_sv(vvr_rsa(p), a, ipo(p))
  
  m <- m %>%
    dplyr::inner_join(a, by = 'cle_rsa') %>%
    dplyr::mutate(date_admin = dtent + delai) %>%
    dplyr::filter(typeprest == '06')
  
  analyse <- m %>%
    dplyr::inner_join(type_valo, by = 'cle_rsa') %>% 
    dplyr::mutate(ucd7 = substr(cducd, 6, 12)) %>% 
    dplyr::filter(indication != "") %>% 
    dplyr::group_by(cle_rsa, ucd13 = cducd, ucd7, indication, annee, mois, dtent, date_admin, factam, motnofact, type_fin) %>% 
    dplyr::summarise(q = sum(nbadm),
              p = sum(prix)) %>% 
    dplyr::ungroup()
  
  analyse_2 <- analyse %>% 
    dplyr::left_join(ref_indic %>% dplyr::select(-ucd13), 
                     by = c('ucd7' = 'ucd7', 'indication' = 'code_les', 'annee', 'mois'))
  
  resul <- analyse_2  %>% 
    dplyr::filter(dtent >= '2019-03-01', indication != "I999999", 
           (type_fin == 0 | motnofact == '1')) %>% 
    dplyr::filter(inscription == 'non' |  is.na(inscription)) %>%
    dplyr::mutate(inscription = dplyr::case_when(
      inscription == "non" ~ "Association UCD indication inscrite non liste en SUS",
      is.na(inscription)   ~ "Association UCD indication incorrecte"
    )) %>% 
    dplyr::group_by(ucd13, ucd7, indication, inscription) %>% 
    dplyr::summarise(q = sum(q),
                     p = sum(p))
  
  tra <- itra(p)
  
  ana <- analyse_2 %>% 
    dplyr::filter(dtent >= '2019-03-01', indication != "I999999") %>% 
    dplyr::mutate(sej_valorise = (type_fin == 0 | motnofact == '1')) %>% 
    dplyr::filter(inscription == 'non' |  is.na(inscription)) %>% 
    dplyr::mutate(lib_epmsi = case_when(
      inscription == "non" ~ "Association UCD indication inscrite non liste en SUS",
      is.na(inscription)   ~ "Association UCD indication incorrecte"
    )) %>% 
    inner_tra(tra) %>% 
    dplyr::select(nas, norss, ucd13, ucd7, date_admin, indication, 
                  annee, mois, lib_epmsi, inscription, q, p, 
                  sej_valorise, factam, motnofact)

  list(vmed_f = resul, detail_vmed_f = ana)
}
