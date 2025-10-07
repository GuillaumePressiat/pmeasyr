#' ~ PSY - Import des SRPSA
#'
#' Imports des fichiers SRPSA Out
#'
#' Formats depuis 2017 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2025)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tibble) contenant les données séjours de psychiatrie du Out.
#'
#' @examples
#' \dontrun{
#'    srpsa <- isrpsa('750712184',2025,4,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irpsa}}, \code{\link{irps}}
#' @usage isrpsa(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export
isrpsa <- function(...){
  UseMethod('isrpsa')
}


#' @export
isrpsa.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(isrpsa.default, param2)
}


#' @export
isrpsa.list <- function(l , ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(isrpsa.default, param2)
}

#' @export
isrpsa.default <- function(finess, annee, mois, path, 
                  lib = T, 
                  tolower_names = F, ...){

  pmsi_file <- file.path(
    path,
    pmsi_glue_fullname(finess, annee, mois, 'psy', 'srpsa')
  )
  

  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'psy', table == 'srpsa', an == substr(as.character(annee),3,4))
  
  af <- format$longueur
  libelles <- format$libelle
  an <- format$nom
  vec <- format$type
  col_types <-  vec
  is_character <- vapply(col_types, is.character, logical(1))
  col_concise <- function(x) {
    switch(x,
           "_" = ,
           "-" = readr::col_skip(),
           "?" = readr::col_guess(),
           c = readr::col_character(),
           D = readr::col_date(),
           d = readr::col_double(),
           i = readr::col_integer(),
           l = readr::col_logical(),
           n = readr::col_number(),
           T = readr::col_datetime(),
           t = readr::col_time(),
           stop("Unknown shortcut: ", x, call. = FALSE)
    )
  }
  col_types[is_character] <- lapply(col_types[is_character], col_concise)
  
  at <- structure(
    list(
      cols = col_types
    ),
    class = "col_spec"
  )
  
  srpsa_i<-readr::read_fwf(pmsi_file,
                         readr::fwf_widths(af,an), col_types =at, na=character(), ...)
  
  readr::problems(srpsa_i) -> synthese_import
  
  srpsa_i <- srpsa_i %>% 
      dplyr::mutate(NBJPRES_SEJ = NBJPRES_SEJ / 10L)

  
  if (lib==T){
    v <- libelles
    srpsa_i <- srpsa_i  %>%  sjlabelled::set_label(v)
  }
  if (tolower_names){
    names(srpsa_i) <- tolower(names(srpsa_i))
  }
  attr(srpsa_i,"problems") <- synthese_import
  return(srpsa_i)
  
  
}