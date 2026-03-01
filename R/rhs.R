##############################################
####################### SSR ##################
##############################################

#' ~ SSR - Import des RHS
#'
#' Import des RHS
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du fichier In de GENRHA a integrer
#' @param annee Annee de la periode (du fichier in)
#' @param mois Mois de la periode (du fichier in)
#' @param path Chemin d'acces au fichier .rhs.rtt
#' @param lib Attribution de libelles aux colonnes
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max=10e3} pour lire les 10000 premieres lignes
#'
#' @examples
#' \dontrun{
#'    irhs('750712184',2015,12,'pathpath/') -> rhs15
#' }
#' @author G. Pressiat
#' @seealso \code{\link{iano_ssr}}, \code{\link{ileg_ssr}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irhs(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export irhs
#' @export
irhs <- function(...){
  UseMethod('irhs')
}


#' @export
irhs.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irhs.default, param2)
}

#' @export
irhs.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irhs.default, param2)
}

#' @export
irhs.default <- function(finess, annee, mois, path, lib=T, tolower_names = F, ...){

  pmsi_file <- file.path(
    path,
    pmsi_glue_fullname(finess, annee, mois, 'ssr', 'rhs.rtt.txt')
  )
  
  #op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'ssr', table == 'rhs', an == substr(as.character(annee),3,4))
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
  
  if (annee == 2022){
    readr::read_lines(pmsi_file) %>% 
      dplyr::tibble(l = .) %>% 
      dplyr::mutate(l = case_when(substr(l,11,13) == "M1B" ~ paste0(substr(l,1, 59), " ", substr(l,60, nchar(l))), TRUE ~ l)) %>% 
      dplyr::pull(l) %>% 
      readr::write_lines(paste0(path,"/",pmsi_glue_fullname(finess, annee, mois, "ssr", "rhs.rtt2.txt")))
    
    joker <- '2'
  } else  if (annee == 2025){
    readr::read_lines(pmsi_file) %>% 
      dplyr::tibble(l = .) %>% 
      dplyr::mutate(l = case_when(substr(l,11,13) == "M1C" ~ paste0(substr(l,1, 181), "000", substr(l,182, nchar(l))), TRUE ~ l)) %>% 
      dplyr::pull(l) %>% 
      readr::write_lines(paste0(path,"/",pmsi_glue_fullname(finess, annee, mois, "ssr", "rhs.rtt2.txt")))
    
    joker <- '2'
  } else {
    joker <- ''
  }
  suppressWarnings(rhs_i <- readr::read_fwf(paste0(path,"/",pmsi_glue_fullname(finess, annee, mois, "ssr", paste0("rhs.rtt", joker, ".txt"))),trim_ws = FALSE,
                                            readr::fwf_widths(af,an), col_types = at , na=character(), ...)) 
  
  readr::problems(rhs_i) -> synthese_import
  
  rhs_i <- rhs_i %>%
    dplyr::mutate(FPPC = stringr::str_trim(FPPC),
                  MMP = stringr::str_trim(MMP),
                  AE = stringr::str_trim(AE), 
                  shift_zad  = dplyr::case_when(
                    NOVRHS == 'M1D' ~ 0L,
                    NOVRHS == 'M1C' ~ 1L,
                    NOVRHS == 'M1A' ~ 1L,
                                         NOVRHS == 'M1B' & annee == 2022 ~ 1L,
                                         NOVRHS == 'M1B' ~ 6L, 
                                         NOVRHS == 'M19' ~ 1L,
                                         NOVRHS == 'M18' ~ 0L,
                                         NOVRHS == 'M17' ~ 0L,
                                         NOVRHS == 'M16' ~ 0L)) %>% 
    dplyr::mutate(dplyr::across(dplyr::starts_with('DT'), \(d)lubridate::dmy(d, quiet = TRUE))) %>% 
    dplyr::mutate(dplyr::across(dplyr::starts_with('D8'), \(d)lubridate::dmy(d, quiet = TRUE)))
  
  # TODO : variables en fin de partie fixe sur 2017 -- 2020 (LISP / unité spécifique)
  
  if (annee >  2014){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DATE_ACTE  = stringr::str_sub(ccam,1,8) %>% lubridate::dmy(quiet = TRUE),
                    CDCCAM = stringr::str_sub(ccam,9,15),
                    DESCRI = stringr::str_sub(ccam, 16,18) %>% stringr::str_trim(),
                    PHASE  = stringr::str_sub(ccam,19,19),
                    ACT    = stringr::str_sub(ccam,20,20),
                    EXTDOC = stringr::str_sub(ccam,21,21),
                    NBEXEC = stringr::str_sub(ccam,22,23) %>% as.integer()
      ) %>% dplyr::select(-ccam)
    }
    
    fzsarr <- function(csarr){
      dplyr::mutate(csarr,
                    CSARR       = stringr::str_sub(csarr,1,7),
                    CDAPP       = stringr::str_sub(csarr,8,10),
                    CDMOD       = stringr::str_sub(csarr,11,12),
                    CDPAT1      = stringr::str_sub(csarr,13,14),
                    CDPAT2      = stringr::str_sub(csarr,15,16),
                    CDINTER     = stringr::str_sub(csarr,17,18),
                    NBEXEC      = stringr::str_sub(csarr,20,21) %>% as.integer(),
                    DATE_ACTE   = stringr::str_sub(csarr,22,29) %>% lubridate::dmy(quiet = TRUE),
                    NBPATREEL   = stringr::str_sub(csarr,30,31),
                    NBINT       = stringr::str_sub(csarr,32,33),
                    EXTDOCcsarr = stringr::str_sub(csarr,34,35)
      ) %>% dplyr::select(-csarr)
    }
    
    zad <- rhs_i %>% dplyr::select(shift_zad, NAS, NOSEJ, NOSEM, ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        ZAD = substr(ZAD, shift_zad, nchar(ZAD)),
        # ZAD = stringr::str_trim(ZAD, side = "left"),
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,8*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,8}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,8*NBDA+1,8*NBDA + 35*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,35}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,8*NBDA+1+35*NBCSARR,8*NBDA + 35*NBCSARR + 23*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,23}")
      )
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='DA') %>% dplyr::select(-NBDA) %>%
      dplyr::select(NAS, NOSEJ, NOSEM, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM, NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% tibble::as_tibble()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='CSARR') %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM, NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% tibble::as_tibble()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    
    if (lib == T){
      labelacdi <- c('N° administratif du séjour', 'N° Séquentiel du séjour SSR', 'N° de semaine',
                     "Type de code (DA / CSARR / CCAM)", "Diagnostic associé",
                     "Code CSARR", "Code supplémentaire appareillage", "Code modulateur de lieu", 
                     "Code modulateur patient n°1",
                     "Code modulateur patient n°2", 
                     "Code de l'intervenant", "Nb de réalisations", "Date de réalisation",
                     "Nb réel de patients", "Nb d'intervenants",
                     "Extension documentaire CSARR", "Code CCAM", "Partie descriptive","Phase CCAM",
                     "Activité CCAM", "Extension documentaire CCAM")

      acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }


  }
  if (annee == 2014){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DATE_ACTE  = stringr::str_sub(ccam,1,8) %>% lubridate::dmy(quiet = TRUE),
                    CDCCAM = stringr::str_sub(ccam,9,15),
                    PHASE  = stringr::str_sub(ccam,16,16),
                    ACT    = stringr::str_sub(ccam,17,17),
                    EXTDOC = stringr::str_sub(ccam,18,18),
                    NBEXEC = stringr::str_sub(ccam,19,20) %>% as.integer()
      ) %>% dplyr::select(-ccam)
    }
    
    fzsarr <- function(csarr){
      dplyr::mutate(csarr,
                    CSARR       = stringr::str_sub(csarr,1,7),
                    CDAPP       = stringr::str_sub(csarr,8,10),
                    CDMOD       = stringr::str_sub(csarr,11,12),
                    CDPAT1      = stringr::str_sub(csarr,13,14),
                    CDPAT2      = stringr::str_sub(csarr,15,16),
                    CDINTER     = stringr::str_sub(csarr,17,18),
                    NBEXEC      = stringr::str_sub(csarr,20,21) %>% as.integer(),
                    DATE_ACTE   = stringr::str_sub(csarr,22,29) %>% lubridate::dmy(quiet = TRUE),
                    NBPATREEL   = stringr::str_sub(csarr,30,31),
                    NBINT       = stringr::str_sub(csarr,32,33),
                    EXTDOCcsarr = stringr::str_sub(csarr,34,35)
      ) %>% dplyr::select(-csarr)
    }
    
    zad <- rhs_i %>% dplyr::select(shift_zad, NAS, NOSEJ, NOSEM,ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,8*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,8}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,8*NBDA+1,8*NBDA + 35*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,35}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,8*NBDA+1+35*NBCSARR,8*NBDA + 35*NBCSARR + 20*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,20}")
      )
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='DA') %>% dplyr::select(-NBDA) %>%
      dplyr::select(NAS, NOSEJ, NOSEM, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% tibble::as_tibble()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='CSARR') %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% tibble::as_tibble()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    if (lib == T){
      labelacdi <- c('N° administratif du séjour', 'N° Séquentiel du séjour SSR', 'N° de semaine',
                     "Type de code (DA / CSARR / CCAM)", "Diagnostic associé",
                     "Code CSARR", "Code supplémentaire appareillage", "Code modulateur de lieu", 
                     "Code modulateur patient n°1",
                     "Code modulateur patient n°2", 
                     "Code de l'intervenant", "Nb de réalisations", "Date de réalisation",
                     "Nb réel de patients", "Nb d'intervenants",
                     "Extension documentaire CSARR", "Code CCAM","Phase CCAM",
                     "Activité CCAM", "Extension documentaire CCAM")

      acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  if (annee == 2013){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DATE_ACTE  = stringr::str_sub(ccam,1,8) %>% lubridate::dmy(quiet = TRUE),
                    CDCCAM = stringr::str_sub(ccam,9,15),
                    PHASE  = stringr::str_sub(ccam,16,16),
                    ACT    = stringr::str_sub(ccam,17,17),
                    EXTDOC = stringr::str_sub(ccam,18,18),
                    NBEXEC = stringr::str_sub(ccam,19,20) %>% as.integer()
      ) %>% dplyr::select(-ccam)
    }
    
    fzsarr <- function(csarr){
      dplyr::mutate(csarr,
                    CSARR       = stringr::str_sub(csarr,1,7),
                    CDARR       = ifelse(nchar(stringr::str_trim(CSARR)) == 4, CSARR, ""),
                    CSARR       = ifelse(nchar(stringr::str_trim(CSARR)) == 7, CSARR, ""),
                    CODE        = ifelse(nchar(stringr::str_trim(CSARR)) == 7, "CSARR", "CDARR"),
                    CDAPP       = stringr::str_sub(csarr,8,10),
                    CDMOD       = stringr::str_sub(csarr,11,12),
                    CDPAT1      = stringr::str_sub(csarr,13,14),
                    CDPAT2      = stringr::str_sub(csarr,15,16),
                    CDINTER     = stringr::str_sub(csarr,17,18),
                    NBEXEC      = stringr::str_sub(csarr,20,21) %>% as.integer(),
                    DATE_ACTE   = stringr::str_sub(csarr,22,29) %>% lubridate::dmy(quiet = TRUE)
      ) %>% dplyr::select(-csarr)
    }

    
    zad <- rhs_i %>% dplyr::select(shift_zad, NAS, NOSEJ, NOSEM,ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,8*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,8}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,8*NBDA+1,8*NBDA + 29*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,29}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,8*NBDA+1+29*NBCSARR,8*NBDA + 29*NBCSARR + 20*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,20}")
      )
    
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='DA')%>% dplyr::select(-NBDA) %>%
      dplyr::select(NAS, NOSEJ, NOSEM, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% tibble::as_tibble()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% tibble::as_tibble()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    
    if (lib == T){
    labelacdi <- c('N° administratif du séjour', 'N° Séquentiel du séjour SSR', 'N° de semaine',
                   "Type de code (DA / CSARR-CDARR / CCAM)", "Diagnostic associé",
                   "Code CSARR", "Code CDARR", "Code supplémentaire appareillage", "Code modulateur de lieu", 
                   "Code modulateur patient n°1",
                   "Code modulateur patient n°2", 
                   "Code de l'intervenant", "Nb de réalisations", "Date de réalisation",
                   "Code CCAM","Phase CCAM",
                   "Activité CCAM", "Extension documentaire CCAM")

      acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  if (annee == 2012){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DATE_ACTE  = stringr::str_sub(ccam,1,8) %>% lubridate::dmy(quiet = TRUE),
                    CDCCAM = stringr::str_sub(ccam,9,15),
                    PHASE  = stringr::str_sub(ccam,16,16),
                    ACT    = stringr::str_sub(ccam,17,17),
                    EXTDOC = stringr::str_sub(ccam,18,18),
                    NBEXEC = stringr::str_sub(ccam,19,20) %>% as.integer()
      ) %>% dplyr::select(-ccam)
    }
    
    fzsarr <- function(csarr){
      dplyr::mutate(csarr,
                    CSARR       = stringr::str_sub(csarr,1,7),
                    CDARR       = ifelse(nchar(stringr::str_trim(CSARR)) == 4, CSARR, ""),
                    CSARR       = ifelse(nchar(stringr::str_trim(CSARR)) == 7, CSARR, ""),
                    CODE        = ifelse(nchar(stringr::str_trim(CSARR)) == 7, "CSARR", "CDARR"),
                    CDAPP       = stringr::str_sub(csarr,8,10),
                    CDMOD       = stringr::str_sub(csarr,11,12),
                    CDPAT1      = stringr::str_sub(csarr,13,14),
                    CDPAT2      = stringr::str_sub(csarr,15,16),
                    CDINTER     = stringr::str_sub(csarr,17,18),
                    NBEXEC      = stringr::str_sub(csarr,20,21) %>% as.integer(),
                    DATE_ACTE   = stringr::str_sub(csarr,22,29) %>% lubridate::dmy(quiet = TRUE)
      ) %>% dplyr::select(-csarr)
    }
    
    
    zad <- rhs_i %>% dplyr::select(shift_zad, NAS, NOSEJ, NOSEM,ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,8*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,8}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,8*NBDA+1,8*NBDA + 29*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,29}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,8*NBDA+1+29*NBCSARR,8*NBDA + 29*NBCSARR + 20*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,20}")
      )
    
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='DA')%>% dplyr::select(-NBDA) %>%
      dplyr::select(NAS, NOSEJ, NOSEM, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% tibble::as_tibble()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% tibble::as_tibble()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    
    if (lib == T){
      labelacdi <- c('N° administratif du séjour', 'N° Séquentiel du séjour SSR', 'N° de semaine',
                     "Type de code (DA / CSARR-CDARR / CCAM)", "Diagnostic associé",
                     "Code CSARR", "Code CDARR", "Code supplémentaire appareillage", "Code modulateur de lieu", 
                     "Code modulateur patient n°1",
                     "Code modulateur patient n°2", 
                     "Code de l'intervenant", "Nb de réalisations", "Date de réalisation",
                     "Code CCAM","Phase CCAM",
                     "Activité CCAM", "Extension documentaire CCAM")
      
      acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  if (annee == 2011){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DATE_ACTE  = stringr::str_sub(ccam,1,8) %>% lubridate::dmy(quiet = TRUE),
                    CDCCAM = stringr::str_sub(ccam,9,15),
                    PHASE  = stringr::str_sub(ccam,16,16),
                    ACT    = stringr::str_sub(ccam,17,17),
                    EXTDOC = stringr::str_sub(ccam,18,18),
                    NBEXEC = stringr::str_sub(ccam,19,20) %>% as.integer()
      ) %>% dplyr::select(-ccam)
    }
    
    fzsarr <- function(csarr){
      dplyr::mutate(csarr,
                    CDINTER     = stringr::str_sub(csarr,1,2),
                    CODE        = "CDARR",
                    CDARR       = stringr::str_sub(csarr,3,6)
      ) %>% dplyr::select(-csarr)
    }
    
    
    zad <- rhs_i %>% dplyr::select(shift_zad, NAS, NOSEJ, NOSEM,ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,8*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,8}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,8*NBDA+1,8*NBDA + 8*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,8}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,8*NBDA+1+8*NBCSARR,8*NBDA + 8*NBCSARR + 20*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,20}")
      )
    
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% tibble::as_tibble()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='DA')%>% dplyr::select(-NBDA) %>%
      dplyr::select(NAS, NOSEJ, NOSEM, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% tibble::as_tibble()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rhs_i %>% dplyr::select(NAS, NOSEJ, NOSEM,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% tibble::as_tibble()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% tibble::as_tibble() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    
    if (lib == T){
      labelacdi <- c('N° administratif du séjour', 'N° Séquentiel du séjour SSR', 'N° de semaine',
                     "Type de code (DA / CDARR / CCAM)", "Diagnostic associé",
                     "Code intervenant",
                     "Code CDARR",  "Date de réalisation",
                     
                     "Code CCAM","Phase CCAM","Activité CCAM", "Extension documentaire CCAM",
                     "Nb de réalisations")
      
      acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  
  Fillers <- names(rhs_i)
  Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
  
  rhs_i <- rhs_i[,!(names(rhs_i) %in% Fillers)]
  
  rhs_i <- rhs_i  %>% 
    dplyr::mutate(dplyr::across(dplyr::where(is.character), stringr::str_trim))
  
  acdi  <- acdi %>% 
    dplyr::mutate(dplyr::across(dplyr::where(is.character), stringr::str_trim))
  
  rhs_i <- rhs_i   %>% dplyr::select(-ZAD, -shift_zad)
  if (lib == T){
    rhs_i <- rhs_i %>% sjlabelled::set_label(libelles[!is.na(libelles)])
  }
  if (tolower_names){
    names(rhs_i) <- tolower(names(rhs_i))
    names(acdi) <- tolower(names(acdi))
  }
  
  rhs_1 = list(rhs = rhs_i, acdi = acdi)
  attr(rhs_1,"problems") <- synthese_import
  deux <- Sys.time()
  #cat("Données rhs importées en : ", deux-un, " secondes\n")
  return(rhs_1)
}

