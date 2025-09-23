#' ~ PSY - Import des RPS
#'
#' Import du fichier rps
#'
#' Formats depuis 2012 pris en charge
#' Structure du nom du fichier attendu (sortie de Druides) :
#' \emph{finess.annee.moisc.rps.txt}
#'
#' \strong{750712184.2025.02.rps.txt}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tibble) contenant les données rps.
#'
#' @examples
#' \dontrun{
#'    rps <- irps('750712184',2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{ir3a}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irps(finess, annee, mois, path, lib = T, tolower_names = F, ...) 
#' @export irps
#' @export
irps <- function(...){
  UseMethod('irps')
}



#' @export
irps.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irps.default, param2)
}

#' @export
irps.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irps.default, param2)
}

#' @export
irps.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2012|annee > 2025){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'psy', table == 'rps', an == substr(as.character(annee),3,4))
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
  
  suppressWarnings(rps_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",stringr::str_pad(mois, 2, 'left', '0'),".rps.txt"),
                                             readr::fwf_widths(af,an), col_types = at , na=character()))#, ...)) 
  
  readr::problems(rps_i) -> synthese_import
  
  rps_i <- rps_i %>%
    dplyr::mutate(DP = stringr::str_trim(DP)) %>% 
    dplyr::mutate_at(dplyr::vars(dplyr::starts_with('DT')), lubridate::dmy, quiet = TRUE)
  
  if (annee < 2017 ){
    rps_i <- rps_i %>%  
      dplyr::mutate(da  = ifelse(NBDA>0,stringr::str_sub(ZAD,1, NBDA*8),""),
                    lda = stringr::str_extract_all(da, '.{1,8}'))
    
    zad <- rps_i
    da <- purrr::flatten_chr(zad$lda) %>% stringr::str_trim()
    
    df <- zad %>% dplyr::select(NAS, DTDEBSEQ, DTFINSEQ, NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
    da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::select(-NBDA)
    
    rps_i$ZAD[is.na(rps_i$ZAD)] <- ""
    rps_i <- rps_i %>% dplyr::mutate(das = extz(da, ".{1,8}")) %>% 
      dplyr::select(-ZAD, -da, -lda)
    libelles[is.na(libelles)] <- ""
    
    if (lib == T){
      rps_i <- rps_i %>% sjlabelled::set_label(c(libelles[-length(libelles)], "Stream DA ou facteurs associés"))
      
      da <- da %>% sjlabelled::set_label(c('N° administratif du séjour', 'Date de début de séquence','Date de fin de séquence',
                                           'Diagnostics et facteurs associés'))
      

    }
    if (tolower_names){
      names(rps_i) <- tolower(names(rps_i))
      names(da) <- tolower(names(da))
    }
    rps_1 = list(rps = rps_i, das = da, actes = tibble::tibble())
  }
  
  if (annee > 2016){
    rps_i <- rps_i %>%  
      dplyr::mutate(da  = ifelse(NBDA>0,stringr::str_sub(ZAD,1, NBDA*8),""),
                    lda = stringr::str_extract_all(da, '.{1,8}'),
                    actes = ifelse(NBZA>0,stringr::str_sub(ZAD,NBDA*8+1,1+ NBDA*8 + NBZA*27),""),
                    lactes = stringr::str_extract_all(actes, '.{1,23}'))
    
    zad <- rps_i
    da <- purrr::flatten_chr(zad$lda) %>% stringr::str_trim()
    
    df <- zad %>% dplyr::select(NAS, DTDEBSEQ, DTFINSEQ, NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
    da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::select(-NBDA)
    
    actes <- purrr::flatten_chr(zad$lactes)
    
    df <- zad %>% dplyr::select(NAS, DTDEBSEQ, DTFINSEQ, NBZA)
    df <- as.data.frame(lapply(df, rep, df$NBZA), stringsAsFactors = F) %>% tibble::as_tibble()
    actes <- dplyr::bind_cols(df,data.frame(ACTES = actes, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::select(-NBZA)
    
    fzacte <- function(actes){
      dplyr::mutate(actes,
                    DATE_ACTE  = stringr::str_sub(ACTES,1,8) %>% lubridate::dmy(quiet = TRUE),
                    CDCCAM = stringr::str_sub(ACTES,9,15),
                    DESCRI = stringr::str_sub(ACTES,16,18) %>% stringr::str_trim(),
                    PHASE  = stringr::str_sub(ACTES,19,19),
                    ACT    = stringr::str_sub(ACTES,20,20),
                    EXTDOC = stringr::str_sub(ACTES,21,21),
                    NBEXEC = stringr::str_sub(ACTES,22,23) %>% as.integer()
      ) %>% dplyr::select(-ACTES)
    }
    
    fzacte(actes) %>% dplyr::mutate_if(is.character, stringr::str_trim) -> actes
    rps_i$ZAD[is.na(rps_i$ZAD)] <- ""
    rps_i <- rps_i %>% dplyr::mutate(das = extz(da, ".{1,8}"), 
                                       actes = extz(actes, "[A-Z]{4}[0-9]{3}")) %>% dplyr::select(-ZAD, -da, -lactes, -lda)
    libelles[is.na(libelles)] <- ""
    
    if (lib == T){
      rps_i <- rps_i %>% sjlabelled::set_label(c(libelles[-length(libelles)], "Stream actes","Stream DA ou facteurs associés"))
      
      da <- da %>% sjlabelled::set_label(c('N° administratif du séjour', 'Date de début de séquence','Date de fin de séquence',
                                           'Diagnostics et facteurs associés'))
      
      
      actes <- actes %>% sjlabelled::set_label(c('N° administratif du séjour', 'Date de début de séquence','Date de fin de séquence',
                                                 "Date de réalisation", "Code CCAM",
                                                 "Extension PMSI", 
                                                 "Code de la phase", "Code de l'activité", "Extension documentaire", "Nombre de réalisations"))
    }
    if (tolower_names){
      names(rps_i) <- tolower(names(rps_i))
      names(da) <- tolower(names(da))
      names(actes) <- tolower(names(actes))
    }
    rps_1 = list(rps = rps_i, das = da, actes = actes)
  }
  
  attr(rps_1,"problems") <- synthese_import
  return(rps_1)
}


