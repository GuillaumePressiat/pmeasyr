
#' ~ PSY - Import des raa
#'
#' Import du fichier raa
#'
#' Formats depuis 2012 pris en charge
#' Structure du nom du fichier attendu (sortie de Pivoine) :
#' \emph{finess.annee.moisc.rpa.txt}
#'
#' \strong{750712184.2016.3.rpa.txt}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des données (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tibble) contenant les données raa.
#'
#' @examples
#' \dontrun{
#'    raa <- iraa('750712184',2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irpsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iraa(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export iraa
#' @export
iraa <- function(...){
  UseMethod('iraa')
}



#' @export
iraa.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iraa.default, param2)
}

#' @export
iraa.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iraa.default, param2)
}

#' @export
iraa.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2012|annee > 2022){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'psy', table == 'raa', an == substr(as.character(annee),3,4))
  format$longueur[nrow(format)] <- NA
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
  extz <- function(x,pat){unlist(lapply(stringr::str_extract_all(x,pat),toString) )}
  
  suppressWarnings(raa_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rpa.txt"),
                                            readr::fwf_widths(af,an), col_types = at , na=character()))#, ...)) 
  
  readr::problems(raa_i) -> synthese_import
  
  raa_i <- raa_i %>%
    dplyr::mutate(DP = stringr::str_trim(DP)) %>% 
    dplyr::mutate_at(dplyr::vars(dplyr::starts_with('DT'), dplyr::starts_with('DATE')), lubridate::dmy, quiet = TRUE) %>% 
    dplyr::mutate(ID_RAA = dplyr::row_number())
  
  zad <- raa_i %>% dplyr::select(ID_RAA, IPP,DATE_ACTE, NBDA, ZAD) %>%  dplyr::mutate(da  = ifelse(NBDA>0,ZAD,""),
                                                                              lda = stringr::str_extract_all(da, '.{1,8}'))
  
  da <- purrr::flatten_chr(zad$lda)
  
  df <- zad %>% dplyr::select(ID_RAA, IPP, DATE_ACTE, NBDA)
  df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
  da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::select(-NBDA)
  
  
  raa_i$ZAD[is.na(raa_i$ZAD)] <- ""
  raa_i <- raa_i %>% dplyr::mutate(das = extz(ZAD, ".{1,8}")) %>% dplyr::select(-ZAD)
  
  if (lib == T){
    raa_i <- raa_i %>% sjlabelled::set_label(c(libelles[-length(libelles)], "Numéro de ligne fichier RAA", "Stream DA ou facteurs associés"))
    
    da <- da %>% sjlabelled::set_label(c("Numéro de ligne fichier RAA", 'N° identifiant permanent du patient',"Date de l'acte", 'Diagnostics et facteurs associés'))
  }
  
  if (tolower_names){
    names(raa_i) <- tolower(names(raa_i))
    names(da) <- tolower(names(da))
  }
  
  raa_1 = list(raa = raa_i, das = da)
  
  attr(raa_1,"problems") <- synthese_import
  return(raa_1)
}

