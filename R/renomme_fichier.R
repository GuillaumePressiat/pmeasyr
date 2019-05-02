#' Renommer un fichier au format standardisé
#' 
#' Cette fonction permet de renommer un fichier, 
#' par exemple directement produit à partir du système d'information (hospitalier), sous une forme standardisée
#' afin de pouvoir l'importer avec pmeasyr tout en gardant optionnellement le fichier original.
#' 
#' @param nom_fichier Nom du fichier original à renommer
#' @param type_fichier Extension du fichier. Par exemple 'rss.txt'.
#' @param garder_original Si `TRUE` alors le fichier original n'est pas modifié. Sinon, il est renommé.
#' @param copie_temporaire Si `TRUE` alors le fichier renommé est copié dans un répertoire temporaire. Si `FALSE`, alors le nouveau fichier est  Cet argument est pris en compte que si garder_original == TRUE est valable.
#' @param dossier_cible Dossier où la copie renommée du fichier doit être enregistré. Valable que si garder_original == TRUE.
#' 
#' @examples 
#' \dontrun{
#' library(pmeasyr)
#' # classique
#' renomme_fichier(annee = 2019, 
#'                 mois = 12, 
#'                 path = '~/Documents/data/mco/',
#'                 nom_fichier = 'monrss.txt', 
#'                 type_fichier = 'rss.txt', 
#'                 garder_original = TRUE)
#' 
#' # avec noyau de paramètres
#' p <- noyau_pmeasyr(
#'   finess = '123456789',
#'   annee  = 2019,
#'   mois   = 12,
#'   path   = '~/Documents/data/mco',
#'   tolower_names = TRUE,
#'   lib = FALSE, 
#'   progress = FALSE
#' )
#' 
#' renomme_fichier(p, 
#'                 nom_fichier = 'monrss.txt', 
#'                 type_fichier = 'rss.txt', 
#'                 garder_original = TRUE)
#' 
#' }
#' @return Chemin vers le fichier renommé.
#' @export
#' 
renomme_fichier <- function(finess = '000000000', # Argument optionnel, pas d'intérêt dans l'import
                            annee,
                            mois,
                            path,
                            nom_fichier,
                            type_fichier,
                            garder_original = TRUE,
                            copie_temporaire = TRUE,
                            dossier_cible = path){
  UseMethod("renomme_fichier")
}

#' @export
renomme_fichier.pm_param <- function(params, ...){
  new_par <- list(...)
  params <- params[names(params) %in% c('annee', 'finess', 'mois', 'path')]
  param2 <- utils::modifyList(params, new_par)
  do.call(renomme_fichier.default, param2)
}

#' @export
renomme_fichier.pm_param <- function(params,  ...){
  new_par <- list(...)
  params <- params[names(params) %in% c('annee', 'finess', 'mois', 'path')]
  param2 <- utils::modifyList(params, new_par)
  do.call(renomme_fichier.default, param2)
}

#' @export
renomme_fichier.default <- function(finess = '000000000', # Argument optionnel, pas d'intérêt dans l'import
                            annee,
                            mois,
                            path,
                            nom_fichier,
                            type_fichier,
                            garder_original = TRUE,
                            copie_temporaire = TRUE,
                            dossier_cible = path) {
  
  nouveau_nom <- glue::glue('{finess}.{annee}.{mois}.{type_fichier}')
  chemin_fichier <- file.path(path, nom_fichier)
  
  if (copie_temporaire & garder_original) {
    chemin_nouveau <- file.path(tempdir(), nouveau_nom)
    file.copy(chemin_fichier, chemin_nouveau)
    
  } else {
    chemin_nouveau <- file.path(dossier_cible, nouveau_nom)
    
    if (garder_original) {
      file.copy(chemin_fichier, chemin_nouveau)
    } else {
      file.rename(chemin_fichier, chemin_nouveau)
    }
  }
  
  chemin_nouveau
  
}
