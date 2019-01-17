#' ~ MCO - Import des PIE
#'
#' Imports des fichiers PIE Out
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param typpie Type de donnees In / Out (seulement out pour le moment)
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les prestation inter-établissement Out.
#'
#' @examples
#' \dontrun{
#'    pie <- ipie('750712184',2018,4,"~/Documents/data/mco")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}}, \code{\link{irum}}
#' @usage ipie(finess, annee, mois, path, typpie = c("out", "in"), lib = T, tolower_names = F, ...)
#' @export ipie
#' @export
ipie <- function(...){
  UseMethod('ipie')
}


#' @export
ipie.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(ipie.default, param2)
}


#' @export
ipie.list <- function(l , ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(ipie.default, param2)
}

#' @export
ipie.default <- function(finess, annee, mois, path, 
                          typpie = c("out", "in"), lib = T, 
                          tolower_names = F, ...){
  if (annee<2011|annee > 2019){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  typpie <- match.arg(typpie)
  if (!(typpie %in% c('in', 'out'))){
    stop('Paramètre typdiap incorrect')
  }
  
  
  #op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  if (typpie=="out"){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rsa_pie', an == substr(as.character(annee),3,4))
    
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
    
    pie_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".pie"),
                            readr::fwf_widths(af,an), col_types =at, na=character(), ...)
    readr::problems(pie_i) -> synthese_import
    
    
    if (lib==T){
      v <- libelles
      pie_i <- pie_i  %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(pie_i) <- tolower(names(pie_i))
    }
    attr(pie_i,"problems") <- synthese_import
    return(pie_i)
  }
  if (typpie=="in"){
    
    stop('PIE in non pris en charge, pour le moment.')
  }
  
  #   format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'pie_in', an == substr(as.character(annee),3,4))
  #   
  #   af <- format$longueur
  #   libelles <- format$libelle
  #   an <- format$nom
  #   vec <- format$type
  #   col_types <-  vec
  #   is_character <- vapply(col_types, is.character, logical(1))
  #   col_concise <- function(x) {
  #     switch(x,
  #            "_" = ,
  #            "-" = readr::col_skip(),
  #            "?" = readr::col_guess(),
  #            c = readr::col_character(),
  #            D = readr::col_date(),
  #            d = readr::col_double(),
  #            i = readr::col_integer(),
  #            l = readr::col_logical(),
  #            n = readr::col_number(),
  #            T = readr::col_datetime(),
  #            t = readr::col_time(),
  #            stop("Unknown shortcut: ", x, call. = FALSE)
  #     )
  #   }
  #   col_types[is_character] <- lapply(col_types[is_character], col_concise)
  #   
  #   at <- structure(
  #     list(
  #       cols = col_types
  #     ),
  #     class = "col_spec"
  #   )
  #   
  #   diap_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".diap.txt"),
  #                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
  #   readr::problems(diap_i) -> synthese_import
  #   
  #   diap_i <- diap_i %>%
  #     dplyr::mutate(DTDEBUT = lubridate::dmy(DTDEBUT))
  #   
  #   
  #   if (lib==T){
  #     
  #     v <- libelles
  #     diap_i <- diap_i  %>%  sjlabelled::set_label(v)
  #   }
  #   if (tolower_names){
  #     names(diap_i) <- tolower(names(diap_i))
  #   }
  #   attr(diap_i,"problems") <- synthese_import
  #   return(diap_i)
  # }
  # 
}
