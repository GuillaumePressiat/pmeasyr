
##############################################
####################### MCO ##################
##############################################

#' ~ MCO - Import des RUM
#'
#' Import des RUM. 4 types d'imports possibles.
#'
#' Formats depuis 2011 pris en charge
#'
#' Structure du nom du fichier attendu (entrée pour Genrsa) :
#' \emph{finess.annee.moisc.rum}
#'
#' \strong{750712184.2016.2.rum}
#'
#' Types d'imports :
#' \tabular{ll}{
#' 1 XLight : \tab partie fixe\cr
#' 2 Light : \tab partie fixe +  streaming des actes, dad et das\cr
#' 3 Standard : \tab partie fixe + table acdi\cr
#' 4 Standard+ : \tab Import standard (3) + stream\cr
#' }
#'
#' \strong{Principe du streaming :}
#' Mise en chaîne de caractères de la succession d'actes CCAM au cours du RUM, par exemple, pour un RUM :
#' \samp{"ACQK001, LFQK002, MCQK001, NAQK015, PAQK002, PAQK900, YYYY600, ZZQP004"}
#'
#' La recherche d'un (ou d'une liste d') acte(s) sur un RUM est largement accélérée, comparée à une requête sur la large table acdi par une requête du type :
#'
#' \code{grepl("ZZQP004",rum$actes)}  # toutes les lignes de  RUM avec au moins un ZZQP004
#' \code{grepl("ZZQP004|EBLA003",rum$actes)}  # toutes les lignes de  RUM avec au moins un ZZQP004 ou un EBLA003
#'
#' @param finess Finess du In a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param typi Type d'import, par defaut a 3, a 0 : propose a l'utilisateur de choisir au lancement
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~...   parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une classe S3 contenant les tables (data.frame, tibble) importées (rum, actes, das et dad si import 3 et 4)
#'
#' @examples
#' \dontrun{
#'    irum('750712184',2015,12,'~/Documents/data/mco', typi = 1) -> rum15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}}, \code{\link{ileg_mco}}, \code{\link{iano_mco}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @importFrom utils View data unzip modifyList
#' @importFrom magrittr '%>%'
#' @export irum
#' @usage irum(finess, annee, mois, path, lib = T, typi = 3, tolower_names = F, ...)
#' @export
irum <- function(finess, annee, mois, path, lib = T, typi = 3, tolower_names = F, ...){
  UseMethod("irum")
}



#' @export
irum.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irum.default, param2)
}



#' @export
irum.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irum.default, param2)
}

#' @export
irum.default <- function(finess, annee, mois, path, lib = T, typi = 3, tolower_names = F, ...){
  if (annee < 2011 | annee > 2020){
    stop("Année PMSI non prise en charge\n")
  }
  if (mois < 1 | mois > 12){
    stop("Mois incorrect\n")
  }
  if (!(typi %in% 0:4)){
    stop("Type d'import incorrect : 0 ou 1, 2, 3 et 4\n")
  }
  
  # op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  # Import de la table
  # cat(paste("Import des RUM",annee,paste0("M",mois),"\n"))
  # cat(paste("L'objet retourné prendra la forme d'une classe S3.
  #           $rum pour accéder à la table RUM
  #           $das pour accéder à la table DAS
  #           $dad pour accéder à la table DAD
  #           $actes pour accéder à la table ACTES\n\n"))

  extz <- function(x,pat){unlist(lapply(stringr::str_extract_all(x,pat),toString) )}

  format <- pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rum", an == substr(annee,3,4))

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
  
  pmeasyr::formats %>% dplyr::filter(table == "rum", substr(an,1,4) == as.character(annee)) -> rg
  
  # regexpr et curseurs par version
  if (length(unique(rg$an)) == 1) {
    situation_al = 1
    zac <- rg$rg[rg$z == "zac"]
    zd <- rg$rg[rg$z == "zd"]
    zal <- rg$rg[rg$z == "zal"]
    zdad <- rg$rg[rg$z == "zdad"]
    
    curs_al <- rg$curseur[rg$z == "zal"]
    curs_d <- rg$curseur[rg$z == "zd"]
    curs_dad <- rg$curseur[rg$z == "zdad"]
  } else  {
    
    levan <- sort(unique(rg$an))
    vers <- substr(sort(unique(rg$an)),6,9)
    # Regpexpr
    # Curseurs
    zac <- unique(rg$rg[rg$z == "zac"])
    if (length(unique(rg$rg[rg$z == "zd"])) > 1){
      zd1 <- rg$rg[rg$z == "zd" & rg$an == levan[1]]
      zd2 <- rg$rg[rg$z == "zd" & rg$an == levan[2]]
      curs_d1 <- rg$curseur[rg$z == "zd" & rg$an == levan[1]]
      curs_d2 <- rg$curseur[rg$z == "zd" & rg$an == levan[2]]
      situation_d = 2
    } else {
      situation_d = 1
      zd <- unique(rg$rg[rg$z == "zd"])
      curs_d <- unique(rg$curseur[rg$z == "zd"])
    }
    if (length(unique(rg$rg[rg$z == "zdad"])) > 1){
      zdad1 <- rg$rg[rg$z == "zdad" & rg$an == levan[1]]
      zdad2 <- rg$rg[rg$z == "zdad" & rg$an == levan[2]]
      curs_dad1 <- rg$rg[rg$z == "zdad" & rg$an == levan[1]]
      curs_dad2 <- rg$rg[rg$z == "zdad" & rg$an == levan[2]]
      situation_dad = 2
    } else {
      situation_dad = 1
      zdad <- unique(rg$rg[rg$z == "zdad"])
      curs_dad <- unique(rg$curseur[rg$z == "zdad"])
    }
    if (length(unique(rg$rg[rg$z == "zal"])) > 1){
      zal1 <- rg$rg[rg$z == "zal" & rg$an == levan[1]]
      zal2 <- rg$rg[rg$z == "zal" & rg$an == levan[2]]
      curs_al1 <- rg$curseur[rg$z == "zal" & rg$an == levan[1]]
      curs_al2 <- rg$curseur[rg$z == "zal" & rg$an == levan[2]]
      situation_al = 2
    } else {
      situation_al = 1
      zal <- unique(rg$rg[rg$z == "zal"])
      curs_al <- unique(rg$curseur[rg$z == "zal"])
    }
  }
  
  zad <- function(rum_i){
    if (situation_al == 1){
      rum_i %>% dplyr::mutate(
        ac = ifelse(NBACTE>0,stringr::str_sub(ZAD,curs_d*NBDAS+curs_dad*NBDAD+1,curs_d*NBDAS+curs_dad*NBDAD+curs_al*NBACTE),""),
        lactes = stringr::str_extract_all(ac,zal),
        actes = extz(ac,zac),
        
        das_ = ifelse(NBDAS>0,stringr::str_sub(ZAD,1,curs_d*NBDAS),""),
        ldas= stringr::str_extract_all(das_,zd),
        das = extz(das_,zd),
        
        dad_ = ifelse(NBDAD>0,stringr::str_sub(ZAD,curs_d*NBDAS+1,curs_d*NBDAD+curs_dad*NBDAS),""),
        ldad = stringr::str_extract_all(dad_,zdad),
        dad = extz(dad_,zdad)
      ) %>% dplyr::select(-ac,-das_,-dad_)
    } else {
      rum_i %>% dplyr::mutate(
        ac = ifelse(NBACTE>0,dplyr::if_else(NOVERG == vers[1],
                                            stringr::str_sub(ZAD,curs_d*NBDAS+curs_dad*NBDAD+1,
                                                             curs_d*NBDAS+curs_dad*NBDAD+curs_al1*NBACTE),
                                            stringr::str_sub(ZAD,curs_d*NBDAS+curs_dad*NBDAD+1,
                                                             curs_d*NBDAS+curs_dad*NBDAD+curs_al2*NBACTE)), ""),
        lactes = dplyr::if_else(NOVERG == vers[1], stringr::str_extract_all(ac,zal1),stringr::str_extract_all(ac,zal2)),
        actes = extz(ac,zac),
        
        das_ = dplyr::if_else(NBDAS>0,stringr::str_sub(ZAD,1,curs_d*NBDAS),""),
        ldas= stringr::str_extract_all(das_,zd),
        das = extz(das_,zd),
        
        dad_ = dplyr::if_else(NBDAD>0,stringr::str_sub(ZAD,curs_d*NBDAS+1,curs_dad*NBDAD+curs_d*NBDAS),""),
        ldad = stringr::str_extract_all(dad_,zdad),
        dad = extz(dad_,zdad)
      ) %>% dplyr::select(-ac,-das_,-dad_)
    }
  }
  zad3 <- function(rum_i){
    if (situation_al == 1){
      rum_i %>% dplyr::mutate(
        ac = ifelse(NBACTE>0,stringr::str_sub(ZAD,curs_d*NBDAS+curs_dad*NBDAD+1,
                                              curs_d*NBDAS+curs_dad*NBDAD+curs_al*NBACTE),""),
        lactes = stringr::str_extract_all(ac,zal),
        
        das_ = ifelse(NBDAS>0,stringr::str_sub(ZAD,1,curs_d*NBDAS),""),
        ldas= stringr::str_extract_all(das_,zd),
        
        dad_ = ifelse(NBDAD>0,stringr::str_sub(ZAD,8*NBDAS+1,8*NBDAD+8*NBDAS),""),
        ldad = stringr::str_extract_all(dad_,zdad)
      ) %>% dplyr::select(-ac,-das_,-dad_)
    } else {
      rum_i %>% dplyr::mutate(
        ac = ifelse(NBACTE>0 & NOVERG == vers[1],
                    stringr::str_sub(ZAD,curs_d*NBDAS+curs_dad*NBDAD+1,
                                     curs_d*NBDAS+curs_dad*NBDAD+curs_al1*NBACTE),
                    ifelse(NBACTE>0 & NOVERG == vers[2],
                           stringr::str_sub(ZAD,curs_d*NBDAS+curs_dad*NBDAD+1,
                                            curs_d*NBDAS+curs_dad*NBDAD+curs_al2*NBACTE), "")),
        lactes = dplyr::if_else(NOVERG == vers[1], stringr::str_extract_all(ac,zal1),stringr::str_extract_all(ac,zal2)),
        
        das_ = dplyr::if_else(NBDAS>0,stringr::str_sub(ZAD,1,curs_d*NBDAS),""),
        ldas= stringr::str_extract_all(das_,zd),
        
        dad_ = dplyr::if_else(NBDAD>0,stringr::str_sub(ZAD,curs_d*NBDAS+1,curs_dad*NBDAD+curs_d*NBDAS),""),
        ldad = stringr::str_extract_all(dad_,zdad)
      ) %>% dplyr::select(-ac,-das_,-dad_)
    }
  }
  
  if (annee==2011){
    
    i <- function(annee,mois){
      rum_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rss.txt"),
                               readr::fwf_widths(c(2,6,1,3,NA),c("NOCLAS","CDGHM","Fil1","NOVERG","RUM")),
                               col_types = readr::cols('c','c','c','c','c'),
                               na=character(), ...)  %>%
        dplyr::mutate(
          CDERG         = stringr::str_sub(RUM,1,3),
          NOFINESS      = stringr::str_sub(RUM,4,12),
          NOVERS        = stringr::str_sub(RUM,13,15),
          NORSS         = stringr::str_sub(RUM,16,35),
          NAS           = stringr::str_sub(RUM,36,55),
          NORUM         = stringr::str_sub(RUM,56,65),
          DTNAIS        = stringr::str_sub(RUM,66,73),
          SXPMSI        = stringr::str_sub(RUM,74,74),
          CDURM         = stringr::str_sub(RUM,75,78),
          KYTYPAUTLIT   = stringr::str_sub(RUM,79,80),
          D8EEUE        = stringr::str_sub(RUM,81,88),
          MDEEUE        = stringr::str_sub(RUM,89,89),
          TYTRPR        = stringr::str_sub(RUM,90,90),
          D8SOUE        = stringr::str_sub(RUM,91,98),
          MDSOUE        = stringr::str_sub(RUM,99,99),
          TYTRDS        = stringr::str_sub(RUM,100,100),
          CDRESI        = stringr::str_sub(RUM,101,105),
          PDNAIS        = stringr::str_sub(RUM,106,109) %>% as.numeric(),
          AGEGEST       = stringr::str_sub(RUM,110,111) %>% as.integer(),
          DDR2          = ifelse(NOVERG=='115', ""                            , stringr::str_sub(RUM,112,119)),
          NBSEAN        = ifelse(NOVERG=='115', stringr::str_sub(RUM,112,113) , stringr::str_sub(RUM,120,121)) %>% as.integer(),
          NBDAS         = ifelse(NOVERG=='115', stringr::str_sub(RUM,114,115) , stringr::str_sub(RUM,122,123)) %>% as.integer(),
          NBDAD         = ifelse(NOVERG=='115', stringr::str_sub(RUM,116,117) , stringr::str_sub(RUM,124,125)) %>% as.integer(),
          NBACTE        = ifelse(NOVERG=='115', stringr::str_sub(RUM,118,120) , stringr::str_sub(RUM,126,128)) %>% as.integer(),
          DP            = ifelse(NOVERG=='115', stringr::str_sub(RUM,121,128) , stringr::str_sub(RUM,129,137)),
          DR            = ifelse(NOVERG=='115', stringr::str_sub(RUM,129,136) , stringr::str_sub(RUM,138,146)),
          IGS           = ifelse(NOVERG=='115', stringr::str_sub(RUM,137,138) , stringr::str_sub(RUM,147,149)),
          CONFCDRSS     = ifelse(NOVERG=='115', stringr::str_sub(RUM,139,140) , stringr::str_sub(RUM,150,150)),
          RDT_TYPMACH   = ifelse(NOVERG=='115', stringr::str_sub(RUM,141,141) , stringr::str_sub(RUM,151,151)),
          RDT_TYPDOSIM  = ifelse(NOVERG=='115', stringr::str_sub(RUM,142,142) , stringr::str_sub(RUM,152,152)),
          NBFAISC       = ifelse(NOVERG=='115', stringr::str_sub(RUM,143,143) , stringr::str_sub(RUM,153,153)) %>% as.integer(),
          ZAD           = ifelse(NOVERG=='115', stringr::str_sub(RUM,154,stringr::str_length(RUM))     , stringr::str_sub(RUM,181,stringr::str_length(RUM)))
        ) %>% dplyr::select(-RUM)
    }
  }
  if (annee>=2012){
    
    i <- function(annee,mois){
      suppressWarnings(readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rss.txt"),
                                       readr::fwf_widths(af,an), col_types = at , na=character(), ...))}
  }
  
  former <- function(cla, col1){
    switch(cla,
           'trim' = col1 %>% stringr::str_trim(),
           'c'   = col1,
           'i'   = col1 %>% as.integer(),
           'n2'  = (col1 %>% as.numeric() )/100,
           'n3'  = (col1 %>% as.numeric() )/1000,
           'dmy' = lubridate::dmy(col1))
  }
  
  if (typi !=0){
    #cat("Lecture du fichier / parsing fixe...\n")
    rum_i <- i(annee,mois) %>% dplyr::mutate(
      DTNAIS=lubridate::dmy(DTNAIS),
      D8EEUE=lubridate::dmy(D8EEUE),
      D8SOUE=lubridate::dmy(D8SOUE),
      DP    = stringr::str_trim(DP),
      DR    = stringr::str_trim(DR)) %>% 
      dplyr::mutate(DUREESEJPART = as.integer(difftime(D8SOUE, D8EEUE, units= c("days"))))
    readr::problems(rum_i) -> synthese_import
  }
  if (typi== 1){
    
    Fillers <- names(rum_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    
    rum_i <- rum_i[,!(names(rum_i) %in% Fillers)] %>% dplyr::select(-ZAD)
    # Libelles
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      rum_i <- rum_i  %>%  sjlabelled::set_label(c(v, "Durée rum"))
    }
    
    if (tolower_names){
      names(rum_i) <- tolower(names(rum_i))
    }
    rum_1 <- list(rum = rum_i)
    class(rum_1) <- append(class(rum_1),"RUM")
    deux<-Sys.time()
    #cat(paste("MCO RUM XLight",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("(Seule la partie fixe du RUM a été chargée)\n")
    return(rum_1)
  }
  if (typi== 2){
    #cat("Traitement | Parsing variable...\n")
    rum_i <- zad(rum_i) %>% dplyr::select(-lactes,-ldas,-ldad,-ZAD )
    rum_i <- rum_i %>%
      dplyr::mutate(das = stringr::str_replace_all(das, "\\s{1,},", ","),
                    dad = stringr::str_replace_all(dad, "\\s{1,},", ","))
    Fillers <- names(rum_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    
    rum_i <- rum_i[,!(names(rum_i) %in% Fillers)]
    if (lib==T){
      v <- libelles
      v <- c(v[!is.na(v)], "Durée rum", "Stream Actes","Stream Das", "Stream Dad")
      rum_i <- rum_i %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(rum_i) <- tolower(names(rum_i))
    }
    rum_1 <- list(rum = rum_i)
    class(rum_1) <- append(class(rum_1),"RUM")
    deux<-Sys.time()
    #cat(paste("MCO RUM Light",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    return(rum_1)
  }
  if (typi== 3){
    #cat("Traitement | Parsing variable...\n")
    rum_i <- zad3(rum_i)
    
    if (situation_al == 1){
      #cat("Actes en ligne : ")
      un_i<-Sys.time()
      actes <- purrr::flatten_chr(rum_i$lactes)
      df <- rum_i %>% dplyr::select(NAS, NORUM, NBACTE)
      df <- as.data.frame(lapply(df, rep, df$NBACTE), stringsAsFactors = F) %>% dplyr::tbl_df()
      actes <- dplyr::bind_cols(df,data.frame(var = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
      fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rum_actes",  an == substr(annee,3,4))
      deb <- fa$position
      fin <- fa$fin
      u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
      for (i in 1:length(deb)){
        temp <- dplyr::as_data_frame(former(fa$type[i], u(actes$var, i)))
        names(temp) <- fa$nom[i]
        actes <- dplyr::bind_cols(actes, temp)
      }
      actes %>% dplyr::select(-var, - NBACTE) -> actes
      deux_i<-Sys.time()
      #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    } else {
      #cat("Actes en ligne : ")
      un_i<-Sys.time()
      actes <- purrr::flatten_chr(rum_i$lactes)
      df <- rum_i %>% dplyr::select(NAS, NORUM, NOVERG, NBACTE)
      df <- as.data.frame(lapply(df, rep, df$NBACTE), stringsAsFactors = F) %>% dplyr::tbl_df()
      actes <- dplyr::bind_cols(df,data.frame(var = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
      fa1 <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rum_actes",
                                                 an == paste0(substr(annee,3,4), '_',vers[1]))
      deb1 <- fa1$position
      fin1 <- fa1$fin
      fa2 <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rum_actes",
                                                 an == paste0(substr(annee,3,4), '_',vers[2]))
      deb2 <- fa2$position
      fin2 <- fa2$fin
      u <- function( i, actes){
        as.vector(as.matrix(actes %>%
                              dplyr::mutate(temp = dplyr::if_else(NOVERG == vers[1], stringr::str_sub(var, deb1[i], fin1[i]),
                                                                  stringr::str_sub(var, deb2[i], fin2[i]))) %>%
                              dplyr::select(temp)))}
      for (i in 1:length(deb1)){
        temp <- dplyr::as_data_frame(former(fa1$type[i], u(i, actes)))
        names(temp) <- fa1$nom[i]
        actes <- dplyr::bind_cols(actes, temp)
      }
      actes %>% dplyr::select(-var, - NBACTE, - NOVERG) -> actes
      deux_i<-Sys.time()
      #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
      fa <- fa2
    }
    
    #cat("Das en ligne : ")
    un_i<-Sys.time()
    das <- purrr::flatten_chr(rum_i$ldas) %>% stringr::str_trim()
    df <- rum_i %>% dplyr::select(NAS,NORUM,NBDAS)
    df <- as.data.frame(lapply(df, rep, df$NBDAS), stringsAsFactors = F) %>% dplyr::tbl_df()
    das <- dplyr::bind_cols(df,data.frame(DAS = stringr::str_trim(das), stringsAsFactors = F) ) %>% dplyr::tbl_df()  %>% dplyr::select(-NBDAS)
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    #cat("Dad en ligne : ")
    un_i<-Sys.time()
    dad <- purrr::flatten_chr(rum_i$ldad)
    df <- rum_i %>% dplyr::select(NAS,NORUM,NBDAD)
    df <- as.data.frame(lapply(df, rep, df$NBDAD), stringsAsFactors = F) %>% dplyr::tbl_df()
    dad <- dplyr::bind_cols(df,data.frame(DAD = stringr::str_trim(dad), stringsAsFactors = F) ) %>% dplyr::tbl_df()  %>% dplyr::select(-NBDAD)
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    if (lib == T){
      actes %>% sjlabelled::set_label(c('N° administratif du séjour','N° du RUM', fa$libelle)) -> actes
      das %>% sjlabelled::set_label(c('N° administratif du séjour','N° du RUM', 'Diagnostic associé')) -> das
      dad %>% sjlabelled::set_label(c('N° administratif du séjour','N° du RUM', 'Donnée à visée documentaire')) -> dad
    }
    
    
    
    Fillers <- names(rum_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    
    rum_i <- rum_i[,!(names(rum_i) %in% Fillers)] %>% dplyr::select(-ZAD, -ldad, -lactes, -ldas)
    
    # Libelles
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      rum_i <- rum_i   %>%  sjlabelled::set_label(c(v, "Durée rum"))
    }
    
    if (tolower_names){
      names(rum_i) <- tolower(names(rum_i))
      names(actes) <- tolower(names(actes))
      names(das) <- tolower(names(das))
      names(dad) <- tolower(names(dad))
    }
    rum_1 <- list(rum = rum_i, actes = actes, das = das, dad = dad)
    class(rum_1) <- append(class(rum_1),"RUM")
    deux<-Sys.time()
    #cat(paste("MCO RUM Standard",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    return(rum_1)
  }
  if (typi== 4){
    #cat("Traitement | Parsing variable...\n")
    rum_i <- zad(rum_i)
    
    if (situation_al == 1){
      #cat("Actes en ligne : ")
      un_i<-Sys.time()
      actes <- purrr::flatten_chr(rum_i$lactes)
      df <- rum_i %>% dplyr::select(NAS, NORUM, NBACTE)
      df <- as.data.frame(lapply(df, rep, df$NBACTE), stringsAsFactors = F) %>% dplyr::tbl_df()
      actes <- dplyr::bind_cols(df,data.frame(var = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
      fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rum_actes",  an == substr(annee,3,4))
      deb <- fa$position
      fin <- fa$fin
      u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
      for (i in 1:length(deb)){
        temp <- dplyr::as_data_frame(former(fa$type[i], u(actes$var, i)))
        names(temp) <- fa$nom[i]
        actes <- dplyr::bind_cols(actes, temp)
      }
      actes %>% dplyr::select(-var, - NBACTE) -> actes
      deux_i<-Sys.time()
      #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    } else {
      #cat("Actes en ligne : ")
      un_i<-Sys.time()
      actes <- purrr::flatten_chr(rum_i$lactes)
      df <- rum_i %>% dplyr::select(NAS, NORUM,  NOVERG, NBACTE)
      df <- as.data.frame(lapply(df, rep, df$NBACTE), stringsAsFactors = F) %>% dplyr::tbl_df()
      actes <- dplyr::bind_cols(df,data.frame(var = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
      fa1 <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rum_actes",
                                                 an == paste0(substr(annee,3,4), '_',vers[1]))
      deb1 <- fa1$position
      fin1 <- fa1$fin
      fa2 <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rum_actes",
                                                 an == paste0(substr(annee,3,4), '_',vers[2]))
      deb2 <- fa2$position
      fin2 <- fa2$fin
      u <- function( i, actes){
        as.vector(as.matrix(actes %>%
                              dplyr::mutate(temp = dplyr::if_else(NOVERG == vers[1], stringr::str_sub(var, deb1[i], fin1[i]),
                                                                  stringr::str_sub(var, deb2[i], fin2[i]))) %>%
                              dplyr::select(temp)))}
      for (i in 1:length(deb1)){
        temp <- dplyr::as_data_frame(former(fa1$type[i], u(i, actes)))
        names(temp) <- fa1$nom[i]
        actes <- dplyr::bind_cols(actes, temp)
      }
      actes %>% dplyr::select(-var, -NBACTE, - NOVERG) -> actes
      deux_i<-Sys.time()
      #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
      fa <- fa2
    }
    
    #cat("Das en ligne : ")
    un_i<-Sys.time()
    das <- purrr::flatten_chr(rum_i$ldas) %>% stringr::str_trim()
    df <- rum_i %>% dplyr::select(NAS,NORUM,NBDAS)
    df <- as.data.frame(lapply(df, rep, df$NBDAS), stringsAsFactors = F) %>% dplyr::tbl_df()
    das <- dplyr::bind_cols(df,data.frame(DAS = stringr::str_trim(das), stringsAsFactors = F) ) %>% dplyr::tbl_df()  %>% dplyr::select(-NBDAS)
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    #cat("Dad en ligne : ")
    un_i<-Sys.time()
    dad <- purrr::flatten_chr(rum_i$ldad)
    df <- rum_i %>% dplyr::select(NAS,NORUM,NBDAD)
    df <- as.data.frame(lapply(df, rep, df$NBDAD), stringsAsFactors = F) %>% dplyr::tbl_df()
    dad <- dplyr::bind_cols(df,data.frame(DAD = stringr::str_trim(dad), stringsAsFactors = F) ) %>% dplyr::tbl_df()  %>% dplyr::select(-NBDAD)
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    if (lib == T){
      actes %>% sjlabelled::set_label(c('N° administratif du séjour','N° du RUM', fa$libelle)) -> actes
      das %>% sjlabelled::set_label(c('N° administratif du séjour','N° du RUM', 'Diagnostic associé')) -> das
      dad %>% sjlabelled::set_label(c('N° administratif du séjour','N° du RUM', 'Donnée à visée documentaire')) -> dad
    }
    
    
    rum_i <- rum_i %>%  dplyr::select(-lactes,-ldas,-ldad,-ZAD ) %>% 
      dplyr::mutate(das = stringr::str_replace_all(das, "\\s{1,},", ","),
                    dad = stringr::str_replace_all(dad, "\\s{1,},", ","))
    
    
    Fillers <- names(rum_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    
    rum_i <- rum_i[,!(names(rum_i) %in% Fillers)]
    # Libelles
    if (lib==T){
      v <- libelles
      v <- c(v[!is.na(v)], "Durée rum", "Stream Actes","Stream Das", "Stream Dad")
      rum_i <- rum_i  %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(rum_i) <- tolower(names(rum_i))
      names(actes) <- tolower(names(actes))
      names(das) <- tolower(names(das))
      names(dad) <- tolower(names(dad))
    }
    rum_1 <- list(rum = rum_i, actes = actes, das = das, dad = dad)
    class(rum_1) <- append(class(rum_1),"RUM")
    deux<-Sys.time()
    attr(rum_i,"problems") <- synthese_import
    #cat(paste("MCO RUM Standard+",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    return(rum_1)
  }
  
  cat("Quel type d'import ?\n")
  typo <- data.frame(Type=c(1,
                            2,
                            3,
                            4),
                     Import=c('XLight    : Partie fixe',
                              'Light     : Partie fixe + stream en ligne des actes, das et dad',
                              'Standard  : Partie fixe + table acdi',
                              'Standard+ : Partie fixe + stream + table acdi '),
                     Temps=c('Très Rapide','Rapide','Rapide','Long'),
                     `Temps rapporté`=c('= 1','* 5 (~)','* 4 (~)','* 7 (~)'))
  
  cat(knitr::kable(typo), sep = "\n")
  n <- readline(prompt="Taper le type d'import voulu : ")
  return(irum(finess,annee,mois,path,lib,n, ...))
}


#' ~ MCO - Import des RSA
#'
#' Import des RSA. 6 types d'imports possibles.
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Genrsa) :
#' \emph{finess.annee.moisc.rsa}
#'
#' \strong{750712184.2016.2.rsa}
#'
#' Types d'imports :
#' \tabular{ll}{
#' 1 Light : \tab partie fixe (très rapide)\cr
#' 2 Light+ : \tab Partie fixe + stream en ligne (+) actes et das\cr
#' 3 Light++ : \tab Partie fixe + stream en ligne (++) actes, das, typaut um et dpdr des um\cr
#' 4 Standard : \tab Partie fixe + création des tables acdi et rsa_um\cr
#' 5 Standard+ : \tab Partie fixe + création des tables acdi et rsa_um + stream (+)\cr
#' 6 Standard++ : \tab Partie fixe + création des tables acdi et rsa_um + stream (++)\cr
#' }
#'
#'
#' \strong{Principe du streaming :}
#' Mise en chaîne de caractères de la succession d'actes CCAM au cours du RUM, par exemple, pour un RUM :
#' \samp{"ACQK001, LFQK002, MCQK001, NAQK015, PAQK002, PAQK900, YYYY600, ZZQP004"}
#'
#' La recherche d'un (ou d'une liste d') acte(s) sur un RUM est largement accélérée, comparée à une requête sur la large table acdi par une requête du type :
#'
#' \code{grepl("ZZQP004",rsa$actes)}  # toutes les lignes de RSA avec au moins un ZZQP004
#'
#' \code{e66 <- grepl('E66',das)|grepl('E66',dpdrum)}  # toutes les lignes de RSA avec un diagnostic E66
#'
#' Cela permet de n'utiliser que la seule table rsa avec stream et d'avoir les infos sur les séjours directement : nb séjours, journées, entrée / sortie (...) plutôt que d'avoir à utiliser et croiser les tables acdi, rsa_um avec rsa.
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param typi Type d'import, par defaut a 4, a 0 : propose a l'utilisateur de choisir au lancement
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une classe S3 contenant les tables (data.frame, tbl_df ou tbl) importées  (rsa, rsa_um, actes et das si import > 3)
#'
#' @examples
#' \dontrun{
#'    irsa('750712184',2015,12,'~/Documents/data/mco') -> rsa15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{ileg_mco}}, \code{\link{iano_mco}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irsa(finess, annee, mois, path, lib = T, typi = 4, tolower_names = F, ...)
#' @export irsa
#' @export
irsa <- function(finess, annee, mois, path, lib = T, typi = 4, tolower_names = F, ...){
  UseMethod('irsa')
}





#' @export
irsa.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irsa.default, param2)
}



#' @export
irsa.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irsa.default, param2)
}

#' @export
irsa.default <- function(finess, annee, mois, path, lib = T, typi = 4, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  if (!(typi %in% 0:6)){
    stop("Type d'import incorrect : 0 ou 1, 2, 3, 4, 5 et 6\n")
  }
  
  
  #op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  
  # cat(paste("L'objet retourné prendra la forme d'une classe S3.
  #           $rsa pour accéder à la table RSA
  #           $rsa_um pour accéder à la table RSA_UM
  #           $das pour accéder à la table des DAS
  #           $actes pour accéder à la table des ACTES\n\n"))

  format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rsa', an == substr(as.character(annee),3,4))
  
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
  
  former <- function(cla, col1){
    switch(cla,
           'trim' = col1 %>% stringr::str_trim(),
           'c'   = col1,
           'i'   = col1 %>% as.integer(),
           'n2'  = (col1 %>% as.numeric() )/100,
           'n3'  = (col1 %>% as.numeric() )/1000,
           'dmy' = lubridate::dmy(col1))
  }
  
  
  if (typi !=0){
    #cat('Lecture du fichier | Parsing partie fixe...\n')
    rsa_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rsa"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ... ) 
    readr::problems(rsa_i) -> synthese_import
    
    rsa_i <- rsa_i %>%
      dplyr::mutate(DP = stringr::str_trim(DP),
                    DR = stringr::str_trim(DR),
      ghm = paste0(RSACMD, RSATYPE, RSANUM, RSACOMPX),
                    anseqta = dplyr::if_else(MOISSOR < "03", as.character(annee - 1), as.character(annee)))
    
  }
  
  if (typi== 1){
    deux<-Sys.time()
    #cat(paste("MCO RSA Light",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("(Seule la partie fixe du RSA a été chargée)\n")
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)] %>% dplyr::select(-ZA)
    # Libelles
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      rsa_i <- rsa_i   %>%  sjlabelled::set_label(c(v, 'Ghm', 'Année séq. de tarifs'))
    }
    if (tolower_names){
      names(rsa_i) <- tolower(names(rsa_i))
    }
    rsa_1 <- list(rsa = rsa_i)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    attr(rsa_1,"problems") <- synthese_import
    return(rsa_1)
  }
  pmeasyr::formats %>% dplyr::filter(table == "rsa" & an == as.integer(annee)) -> rg
  
  zac = rg$rg[rg$z == 'zac']
  zd  = rg$rg[rg$z == 'zd']
  zum = rg$rg[rg$z == 'zum']
  zal = rg$rg[rg$z == 'zal']
  
  cd = rg$curseur[rg$z == 'zd']
  cum = rg$curseur[rg$z == 'zum']
  cal = rg$curseur[rg$z == 'zal']
  
  fzad <- function(rsa){
    if (as.integer(annee) > 2011){
      return(rsa %>% dplyr::mutate(
        TYPGLOB = ifelse(NBAUTPGV>0,stringr::str_sub(ZA,1,2*NBAUTPGV),""),
        RDTH    = ifelse(NB_RDTH>0,stringr::str_sub(ZA,2*NBAUTPGV+1,2*NBAUTPGV+7*NB_RDTH),""),
        RUMS    = ifelse(as.numeric(NBRUM)>0,stringr::str_sub(ZA,2*NBAUTPGV+7*NB_RDTH+1,2*NBAUTPGV+7*NB_RDTH+cum*NBRUM),""),
        DAS     = ifelse(NDAS>0,stringr::str_sub(ZA,2*NBAUTPGV+7*NB_RDTH+cum*NBRUM + 1,2*NBAUTPGV + 7*NB_RDTH + cum*NBRUM + cd*NDAS),""),
        ACTES   = ifelse(`NA`>0,stringr::str_sub(ZA,2*NBAUTPGV+7*NB_RDTH+cum*NBRUM + cd*NDAS + 1,2*NBAUTPGV + 7*NB_RDTH + cum*NBRUM + cd*NDAS + cal*`NA`),"")
      ))}
    else if (as.integer(annee) == 2011){
      return(rsa %>% dplyr::mutate(
        RDTH    = ifelse(NB_RDTH>0,stringr::str_sub(ZA,1,7*NB_RDTH),""),
        RUMS    = ifelse(as.numeric(NBRUM)>0,stringr::str_sub(ZA,7*NB_RDTH+1,7*NB_RDTH+cum*NBRUM),""),
        DAS     = ifelse(NDAS>0,stringr::str_sub(ZA,7*NB_RDTH+cum*NBRUM + 1,7*NB_RDTH + cum*NBRUM + cd*NDAS),""),
        ACTES   = ifelse(`NA`>0,stringr::str_sub(ZA,7*NB_RDTH+cum*NBRUM + cd*NDAS + 1,7*NB_RDTH + cum*NBRUM + cd*NDAS + cal*`NA`),"")))
    }
  }
  
  if (typi == 2){
    #cat('Import Light+ | Streaming des actes et das...\n')
    rsa_i <- fzad(rsa_i)
    rsa_i  <- rsa_i %>%
      dplyr::mutate(
        actes  = extz(ACTES,zac),                   # Stream des actes
        das    = extz(DAS,zd)) %>%                   # Stream des das
      dplyr::select(-ZA,-ACTES,-RUMS,-DAS)
    
    rsa_i <- rsa_i %>%
      dplyr::mutate(das = stringr::str_replace_all(das, "\\s{1,},", ","))
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)]
    
    # Libelles
    if (lib==T){
      v <- libelles
      if (annee==2011){
        v <- c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Supp. Radiothérapies", "Stream Actes", "Stream Das")
      }
      else {
        v <- c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes", "Stream Das")
      }
      rsa_i <- rsa_i %>%  sjlabelled::set_label(v)
    }
    
    if (tolower_names){
      names(rsa_i) <- tolower(names(rsa_i))
    }
    rsa_1 <- list(rsa = rsa_i)
    
    
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    deux<-Sys.time()
   #cat(paste("MCO RSA Light+",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    attr(rsa_1,"problems") <- synthese_import
    return(rsa_1)
    
  }
  fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rsa_um",  an == substr(annee,3,4))
  debum <- fa[fa$nom == "TYPAUT1",]$position
  finum <- fa[fa$nom == "TYPAUT1",]$fin
  
  debdpdr <- fa[fa$nom == "DPUM",]$position
  findpdr <- fa[fa$nom == "DRUM",]$fin
  
  if (typi == 3){
    #cat('Import Light++ | Streaming des actes, das, typaut UM et DP/DR des UM...\n')
    rsa_i <- fzad(rsa_i)
    rsa_i  <- rsa_i %>%
      dplyr::mutate(
        actes  = extz(ACTES,zac),                   # Stream des actes
        lum    = stringr::str_extract_all(RUMS,zum),         # Liste des UM
        um     = unlist(lapply(lapply(lum,          # Stream des types d'UM
                                      function(x){substr(x,debum,finum)}),
                               function(y){toString(y)})),
        dpdrum = unlist(lapply(lapply(lum,          # Stream des dpdr d'UM
                                      function(x){substr(x,debdpdr,findpdr)}),
                               function(y){toString(y)})),
        das    = extz(DAS,zd)) %>%                   # Stream des das
      dplyr::select(-ZA,-RUMS,-ACTES,-DAS,-lum)
    
    rsa_i <- rsa_i %>%
      dplyr::mutate(das = stringr::str_replace_all(das, "\\s{1,},", ","),
                    dpdrum = stringr::str_replace_all(dpdrum, "\\s{1,},", ","))
    
    deux<-Sys.time()
    
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)]
    
    if (lib==T){
      v <- libelles
      if (annee==2011) {
        v <- c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das")
      }
      else{
        v <-   c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das")
      }
      rsa_i <- rsa_i %>%  sjlabelled::set_label(v)
    }
    
    if (tolower_names){
      names(rsa_i) <- tolower(names(rsa_i))
    }
    
    rsa_1 <- list(rsa = rsa_i)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    #cat(paste("MCO RSA Light++",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("La table rsa est dans l'environnement de travail\n")
    
    attr(rsa_1,"problems") <- synthese_import
    return(rsa_1)
  }
  
  if (typi == 4){
    
    
    #cat('Traitement | Parsing partie variable...\n')
    
    rsa_i <- fzad(rsa_i)
    
    rsa_i  <- rsa_i %>%
      dplyr::mutate(lactes = stringr::str_extract_all(ACTES,zal),           # Liste des actes
                    lum    = stringr::str_extract_all(RUMS,zum),           # Liste des UM
                    ldas   = stringr::str_extract_all(DAS,zd) ) %>%           # Liste de das
      dplyr::select(-ZA,-RUMS,-ACTES,-DAS)
    
    #cat("Passages UM en ligne : ")
    un_i<-Sys.time()
    rsa_um <- purrr::flatten_chr(rsa_i$lum)
    df <- rsa_i %>% dplyr::select(CLE_RSA,NBRUM)
    df <- as.data.frame(lapply(df, rep, df$NBRUM), stringsAsFactors = F) %>% dplyr::tbl_df()
    if (as.integer(annee) <= 2012){
      df <- df %>% dplyr::mutate(NSEQRUM = stringr::str_pad(dplyr::row_number(CLE_RSA), 2,"left","0"))}
    rsa_um <- dplyr::bind_cols(df,data.frame(var = rsa_um, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rsa_um",  an == substr(annee,3,4))
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    for (i in 1:length(deb)){
      temp <- dplyr::as_data_frame(former(fa$type[i], u(rsa_um$var, i)))
      names(temp) <- fa$nom[i]
      rsa_um <- dplyr::bind_cols(rsa_um, temp)
    }
    rsa_um %>% dplyr::select(-var, -NBRUM) -> rsa_um
    if (lib == T){
      if (as.integer(annee) <= 2012){
        rsa_um %>% sjlabelled::set_label(c('Clé RSA','N° Séquentiel du RUM', fa$libelle)) -> rsa_um}
      else
        rsa_um %>% sjlabelled::set_label(c('Clé RSA', fa$libelle)) -> rsa_um
    }
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    # das
    #cat("Das en ligne : ")
    un_i<-Sys.time()
    das <- purrr::flatten_chr(rsa_i$ldas) %>% stringr::str_trim()
    df <- rsa_um %>% dplyr::select(CLE_RSA,NSEQRUM,NBDIAGAS)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGAS), stringsAsFactors = F) %>% dplyr::tbl_df()
    das <- dplyr::bind_cols(df,data.frame(DAS = das, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    das <- das %>% dplyr::select(-NBDIAGAS)
    if (lib == T){
      das %>% sjlabelled::set_label(c('Clé RSA', 'N° séquentiel du RUM',  'Diagnostic associé')) -> das
    }
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    # actes
    #cat("Actes en ligne : ")
    un_i<-Sys.time()
    actes <- purrr::flatten_chr(rsa_i$lactes)
    df <- rsa_um %>% dplyr::select(CLE_RSA,NSEQRUM,NBACTE)
    df <- as.data.frame(lapply(df, rep, df$NBACTE), stringsAsFactors = F) %>% dplyr::tbl_df()
    actes <- dplyr::bind_cols(df,data.frame(var = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rsa_actes", an == substr(annee,3,4))
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    for (i in 1:length(deb)){
      temp <- dplyr::as_data_frame(former(fa$type[i], u(actes$var, i)))
      names(temp) <- fa$nom[i]
      actes <- dplyr::bind_cols(actes, temp)
    }
    actes %>% dplyr::select(-var, -NBACTE) -> actes
    if (lib == T){
      actes %>% sjlabelled::set_label(c('Clé RSA', 'N° séquentiel du RUM', fa$libelle)) -> actes
    }
    
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    deux<-Sys.time()
    
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)]
    
    # Libelles
    rsa_i <- rsa_i %>% dplyr::select(-lactes, - lum, - ldas)
    
    if (lib==T){
      v <- libelles
      if (annee==2011){
        v <- c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Supp. Radiothérapies")
      }
      else {
        v <- c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Types Aut. à Portée Globale", "Supp. Radiothérapies")
      }
      rsa_i <- rsa_i %>%  sjlabelled::set_label(v)
    }
    
    if (tolower_names){
      names(rsa_i) <- tolower(names(rsa_i))
      names(actes) <- tolower(names(actes))
      names(das) <- tolower(names(das))
      names(rsa_um) <- tolower(names(rsa_um))
    }
    rsa_1 <- list(rsa = rsa_i,
                  actes = actes,
                  das = das,
                  rsa_um=rsa_um)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    #cat(paste("MCO RSA Standard",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    attr(rsa_1,"problems") <- synthese_import
    return(rsa_1)
  }
  if (typi == 5){
    #cat('Import standard+\n')
    #cat('Traitement | Parsing partie variable...\n')
    
    rsa_i <- fzad(rsa_i)
    rsa_i  <- rsa_i %>%
      dplyr::mutate(lactes = stringr::str_extract_all(ACTES,zal),           # Liste des actes
                    actes  = extz(ACTES,zac),                      # Stream des actes
                    lum    = stringr::str_extract_all(RUMS,zum),           # Liste des UM
                    ldas   = stringr::str_extract_all(DAS,zd),            # Liste de das
                    das    = extz(DAS,zd)) %>%                       # Stream des das
      dplyr::select(-ZA,-RUMS,-ACTES,-DAS)
    
    rsa_i <- rsa_i %>%
      dplyr::mutate(das = stringr::str_replace_all(das, "\\s{1,},", ","))
    
    #cat("Passages UM en ligne : ")
    un_i<-Sys.time()
    rsa_um <- purrr::flatten_chr(rsa_i$lum)
    df <- rsa_i %>% dplyr::select(CLE_RSA,NBRUM)
    df <- as.data.frame(lapply(df, rep, df$NBRUM), stringsAsFactors = F) %>% dplyr::tbl_df()
    if (as.integer(annee) <= 2012){
      df <- df %>% dplyr::mutate(NSEQRUM = stringr::str_pad(dplyr::row_number(CLE_RSA), 2,"left","0"))}
    rsa_um <- dplyr::bind_cols(df,data.frame(var = rsa_um, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rsa_um",  an == substr(annee,3,4))
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    for (i in 1:length(deb)){
      temp <- dplyr::as_data_frame(former(fa$type[i], u(rsa_um$var, i)))
      names(temp) <- fa$nom[i]
      rsa_um <- dplyr::bind_cols(rsa_um, temp)
    }
    rsa_um %>% dplyr::select(-var, -NBRUM) -> rsa_um
    if (lib == T){
      if (as.integer(annee) <= 2012){
        rsa_um %>% sjlabelled::set_label(c('Clé RSA','N° Séquentiel du RUM', fa$libelle)) -> rsa_um}
      else
        rsa_um %>% sjlabelled::set_label(c('Clé RSA', fa$libelle)) -> rsa_um
    }
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    # das
    #cat("Das en ligne : ")
    un_i<-Sys.time()
    das <- purrr::flatten_chr(rsa_i$ldas) %>% stringr::str_trim()
    df <- rsa_um %>% dplyr::select(CLE_RSA,NSEQRUM,NBDIAGAS)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGAS), stringsAsFactors = F) %>% dplyr::tbl_df()
    das <- dplyr::bind_cols(df,data.frame(DAS = das, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    das <- das %>% dplyr::select(-NBDIAGAS)
    if (lib == T){
      das %>% sjlabelled::set_label(c('Clé RSA', 'N° séquentiel du RUM',  'Diagnostic associé')) -> das
    }
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    # actes
    #cat("Actes en ligne : ")
    un_i<-Sys.time()
    actes <- purrr::flatten_chr(rsa_i$lactes)
    df <- rsa_um %>% dplyr::select(CLE_RSA,NSEQRUM,NBACTE)
    df <- as.data.frame(lapply(df, rep, df$NBACTE), stringsAsFactors = F) %>% dplyr::tbl_df()
    actes <- dplyr::bind_cols(df,data.frame(var = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rsa_actes", an == substr(annee,3,4))
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    for (i in 1:length(deb)){
      temp <- dplyr::as_data_frame(former(fa$type[i], u(actes$var, i)))
      names(temp) <- fa$nom[i]
      actes <- dplyr::bind_cols(actes, temp)
    }
    actes %>% dplyr::select(-var, -NBACTE) -> actes
    if (lib == T){
      actes %>% sjlabelled::set_label(c('Clé RSA', 'N° séquentiel du RUM', fa$libelle)) -> actes
    }
    
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    deux<-Sys.time()
    
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)]
    
    # Libelles
    
    deux<-Sys.time()
    rsa_i <- rsa_i %>% dplyr::select(-lactes,-lum,-ldas)
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)]
    
    if (lib==T){
      v <- libelles
      if (annee==2011) {
        v <- c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Supp. Radiothérapies", "Stream Actes", "Stream Das")
      }
      else{
        v <-  c(v[!is.na(v)], 'Ghm', 'Année séq. de tarifs', "Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes", "Stream Das")
      }
      
      rsa_i <- rsa_i %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(rsa_i) <- tolower(names(rsa_i))
      names(actes) <- tolower(names(actes))
      names(das) <- tolower(names(das))
      names(rsa_um) <- tolower(names(rsa_um))
    }
    
    rsa_1 <- list(rsa = rsa_i , actes = actes, das = das, rsa_um=rsa_um)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    #cat(paste("MCO RSA Standard+",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("Les tables rsa, acdi et rsa_um sont dans l'environnement de travail\n")
    
    attr(rsa_1,"problems") <- synthese_import
    return(rsa_1)
    
  }
  if (typi == 6){
    #cat('Import standard++\n')
    #cat('Traitement | Parsing partie variable...\n')
    rsa_i <- fzad(rsa_i)
    rsa_i  <- rsa_i %>%
      dplyr::mutate(lactes = stringr::str_extract_all(ACTES,zal),           # Liste des actes
                    actes  = extz(ACTES,zac),                      # Stream des actes
                    #na     = stringr::str_count(ZA,zal),                 # Nombre d'actes trouvés    #
                    lum    = stringr::str_extract_all(RUMS,zum),           # Liste des UM
                    #num    = stringr::str_count(ZA,zum),                 # Nombre d'UM trouvés       #
                    um     = unlist(lapply(lapply(lum,          # Stream des types d'UM
                                                  function(x){substr(x,debum,finum)}),
                                           function(y){toString(y)})),
                    dpdrum = unlist(lapply(lapply(lum,          # Stream des dpdr d'UM
                                                  function(x){substr(x,debdpdr,findpdr)}),
                                           function(y){toString(y)})),
                    ldas   = stringr::str_extract_all(DAS,zd),            # Liste de das
                    das    = extz(DAS,zd)) %>%                       # Stream des das
      dplyr::select(-ZA,-RUMS,-ACTES,-DAS)
    
    rsa_i <- rsa_i %>%
      dplyr::mutate(das = stringr::str_replace_all(das, "\\s{1,},", ","),
                    dpdrum = stringr::str_replace_all(dpdrum, "\\s{1,},", ","))
    
    #cat("Passages UM en ligne : ")
    un_i<-Sys.time()
    rsa_um <- purrr::flatten_chr(rsa_i$lum)
    df <- rsa_i %>% dplyr::select(CLE_RSA,NBRUM)
    df <- as.data.frame(lapply(df, rep, df$NBRUM), stringsAsFactors = F) %>% dplyr::tbl_df()
    if (as.integer(annee) <= 2012){
      df <- df %>% dplyr::mutate(NSEQRUM = stringr::str_pad(dplyr::row_number(CLE_RSA), 2,"left","0"))}
    rsa_um <- dplyr::bind_cols(df,data.frame(var = rsa_um, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rsa_um",  an == substr(annee,3,4))
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    for (i in 1:length(deb)){
      temp <- dplyr::as_data_frame(former(fa$type[i], u(rsa_um$var, i)))
      names(temp) <- fa$nom[i]
      rsa_um <- dplyr::bind_cols(rsa_um, temp)
    }
    rsa_um %>% dplyr::select(-var, -NBRUM) -> rsa_um
    if (lib == T){
      if (as.integer(annee) <= 2012){
        rsa_um %>% sjlabelled::set_label(c('Clé RSA','N° Séquentiel du RUM', fa$libelle)) -> rsa_um}
      else
        rsa_um %>% sjlabelled::set_label(c('Clé RSA', fa$libelle)) -> rsa_um
    }
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    # das
    #cat("Das en ligne : ")
    un_i<-Sys.time()
    das <- purrr::flatten_chr(rsa_i$ldas) %>% stringr::str_trim()
    df <- rsa_um %>% dplyr::select(CLE_RSA,NSEQRUM,NBDIAGAS)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGAS), stringsAsFactors = F) %>% dplyr::tbl_df()
    das <- dplyr::bind_cols(df,data.frame(DAS = das, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    das <- das %>% dplyr::select(-NBDIAGAS)
    if (lib == T){
      das %>% sjlabelled::set_label(c('Clé RSA', 'N° séquentiel du RUM',  'Diagnostic associé')) -> das
    }
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    # actes
    #cat("Actes en ligne : ")
    un_i<-Sys.time()
    actes <- purrr::flatten_chr(rsa_i$lactes)
    df <- rsa_um %>% dplyr::select(CLE_RSA,NSEQRUM,NBACTE)
    df <- as.data.frame(lapply(df, rep, df$NBACTE), stringsAsFactors = F) %>% dplyr::tbl_df()
    actes <- dplyr::bind_cols(df,data.frame(var = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    fa <-  pmeasyr::formats %>% dplyr::filter(champ == "mco", table == "rsa_actes", an == substr(annee,3,4))
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    for (i in 1:length(deb)){
      temp <- dplyr::as_data_frame(former(fa$type[i], u(actes$var, i)))
      names(temp) <- fa$nom[i]
      actes <- dplyr::bind_cols(actes, temp)
    }
    actes %>% dplyr::select(-var, -NBACTE) -> actes
    if (lib == T){
      actes %>% sjlabelled::set_label(c('Clé RSA', 'N° séquentiel du RUM', fa$libelle)) -> actes
    }
    
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    deux<-Sys.time()
    rsa_i <- rsa_i %>% dplyr::select(-lactes,-lum,-ldas)
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)]
    
    if (lib==T){
      v <- libelles
      if (annee==2011) {
        v <- c(v[!is.na(v)],"Ghm", "Année séq. de tarifs", "Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das")
      }
      else{
        v <-  c(v[!is.na(v)],"Ghm", "Année séq. de tarifs", "Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das")
      }
      rsa_i <- rsa_i %>%  sjlabelled::set_label(v)
      
    }
    if (tolower_names){
      names(rsa_i) <- tolower(names(rsa_i))
      names(actes) <- tolower(names(actes))
      names(das) <- tolower(names(das))
      names(rsa_um) <- tolower(names(rsa_um))
    }
    
    rsa_1 <- list(rsa = rsa_i , actes = actes, das = das, rsa_um = rsa_um)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    #cat(paste("MCO RSA Standard++",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("Les tables rsa, acdi et rsa_um sont dans l'environnement de travail\n")
    
    attr(rsa_1,"problems") <- synthese_import
    return(rsa_1)
  }
  cat("Quel type d'import ?\n")
  typo <- data.frame(Type=c(1,
                            2,
                            3,
                            4,
                            5,
                            6),
                     Import=c('Light      : Partie fixe',
                              'Light+     : Partie fixe + stream en ligne (+) actes et das',
                              'Light++    : Partie fixe + stream en ligne (++) actes, das, typaut um et dpdr des um',
                              'Standard   : Partie fixe + création des tables acdi et rsa_um',
                              'Standard+  : Partie fixe + création des tables acdi et rsa_um + stream (+)',
                              'Standard++ : Partie fixe + création des tables acdi et rsa_um + stream (++)'),
                     Temps=c('Très Rapide','Rapide','Long','Rapide', 'Long','Long'),
                     `Temps rapporté`=c('= 1','* 4 (~)','* 9 (~)','* 4 (~)','* 6 (~)', '* 10 (~)'))
  
  cat(knitr::kable(typo),sep='\n')
  n <- readline(prompt="Taper le type d'import voulu : ")
  return(irsa(finess,annee,mois,path,lib,n, ...))
}

#' ~ TRA - Import du TRA
#'
#' Import du fichier TRA, 4 champs PMSI couverts.
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Genrsa) :
#' \emph{finess.annee.moisc.tra}
#'
#' \strong{750712184.2016.2.tra}
#'
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param champ Champ PMSI du TRA a integrer ("mco", "ssr", "had", "psy_rpsa", ", "psy_r3a"), par defaut "mco"
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premières lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame ou tbl_df) qui contient : - Clé RSA - NORSS - Numéro de ligne du fichier RSS d'origine (rss.ini) - NAS - Date d'entrée du séjour - GHM groupage du RSS (origine) - Date de sortie du séjour
#'
#' @examples
#' \dontrun{
#'    itra('750712184',2015,12,'~/Documents/data/champ_pmsi') -> tra15
#' }
#'
#' @author G. Pressiat
#'
#' @usage itra(finess, annee, mois, path, lib = T, tolower_names = F, champ = "mco")
#' @seealso \code{\link{irum}}, \code{\link{irsa}}, \code{\link{ileg_mco}}, \code{\link{iano_mco}}, \code{\link{irha}}, \code{\link{irapss}}, \code{\link{irpsa}}, \code{\link{ir3a}}, 
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}

#' @export itra
#' @export
itra <- function(...){
  UseMethod('itra')
}





#' @export
itra.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(itra.default, param2)
}



#' @export
itra.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(itra.default, param2)
}

#' @export
itra.default <- function(finess, annee, mois, path, lib = T, champ= "mco", tolower_names = F, ... ){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  if (!grepl("psy",champ)){
    champ1 = champ
    format <- pmeasyr::formats %>% dplyr::filter(champ == champ1, table == 'tra')
  } else {
    champ1 = champ
    format <- pmeasyr::formats %>% dplyr::filter(champ == "psy", table == champ1)
  }
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
  
  if (champ=="mco"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra.txt"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    readr::problems(tra_i) -> synthese_import
    
    tra_i <- tra_i %>%
      dplyr::mutate(DTENT  = lubridate::dmy(DTENT),
                    DTSORT = lubridate::dmy(DTSORT),
                    NOHOP = paste0("000",stringr::str_sub(NAS,1,2)))
  }
  if (champ=="had"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    readr::problems(tra_i) -> synthese_import
    
    tra_i <- tra_i %>%
      dplyr::mutate(DTENT  = lubridate::dmy(DTENT),
                    DTSORT = lubridate::dmy(DTSORT),
                    NOHOP = paste0("000",stringr::str_sub(NAS,1,2)),
                    DTNAI = lubridate::dmy(DTNAI),
                    DT_DEB_SEQ = lubridate::dmy(DT_DEB_SEQ),
                    DT_FIN_SEQ = lubridate::dmy(DT_FIN_SEQ),
                    DT_DEB_SS_SEQ = lubridate::dmy(DT_DEB_SS_SEQ),
                    DT_FIN_SS_SEQ = lubridate::dmy(DT_FIN_SS_SEQ))
  }
  if (champ=="ssr"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    readr::problems(tra_i) -> synthese_import
    tra_i <- tra_i %>%
      dplyr::mutate(NOHOP = paste0("000",stringr::str_sub(NAS,1,2)))
  }
  if (champ=="psy_rpsa"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra.txt"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    readr::problems(tra_i) -> synthese_import
    tra_i <- tra_i %>%
      dplyr::mutate(NOHOP = paste0("000",stringr::str_sub(NAS,1,2)),
                    DTENT  = lubridate::dmy(DTENT),
                    DTSORT = lubridate::dmy(DTSORT),
                    DT_DEB_SEQ = lubridate::dmy(DT_DEB_SEQ),
                    DT_FIN_SEQ = lubridate::dmy(DT_FIN_SEQ))
  }
  if (champ=="psy_r3a"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra.raa.txt"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    readr::problems(tra_i) -> synthese_import
    tra_i <- tra_i %>%
      dplyr::mutate(DTACTE  = lubridate::dmy(DTACTE),
                    DTACTE_2  = lubridate::dmy(DTACTE_2))
  }
  
  if (lib==T & champ !="psy_r3a"){
    if (tolower_names){
      names(tra_i) <- tolower(names(tra_i))
    }
    v <- c(libelles, 'Établissement')
    return(tra_i  %>%  sjlabelled::set_label(v))
  }
  
  if (lib==T & champ =="psy_r3a"){
    if (tolower_names){
      names(tra_i) <- tolower(names(tra_i))
    }
    v <- libelles
    return(tra_i  %>%  sjlabelled::set_label(v))
  }
  
  if (tolower_names){
    names(tra_i) <- tolower(names(tra_i))
  }
  attr(tra_i,"problems") <- synthese_import
  return(tra_i)
}

#' ~ MCO - Import des Anohosp
#'
#' Import du fichier ANO In ou Out.
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu  :
#' \emph{finess.annee.moisc.ano}
#' \emph{finess.annee.moisc.ano.txt}
#'
#' \strong{750712184.2016.2.ano}
#' \strong{750712184.2016.2.ano.txt}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des données sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param typano Type de donnees In / Out
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame ou tbl_df) qui contient les données Anohosp in / out
#'
#' @examples
#' \dontrun{
#'    iano_mco('750712184',2015,12,'~/Documents/data/mco') -> ano_out15
#'    iano_mco('750712184',2015,12,'~/Documents/data/mco', typano = "in") -> ano_in15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @export iano_mco
#' @usage iano_mco(finess, annee, mois, path, lib = T, tolower_names = F, typano = "out")
#' @export
iano_mco <- function( ...){
  UseMethod('iano_mco')
}





#' @export
iano_mco.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iano_mco.default, param2)
}



#' @export
iano_mco.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iano_mco.default, param2)
}

#' @export
iano_mco.default <- function(finess, annee, mois, path, typano = c("out", "in"), lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  typano <- match.arg(typano)
  if (!(typano %in% c('in', 'out'))){
    stop('Paramètre typano incorrect')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  if (typano=="out"){
    
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rsa_ano', an == substr(as.character(annee),3,4))
    
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
    if (annee>=2013){
      ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                               readr::fwf_widths(af,an), col_types = at , na=character(), ...) 
      
      readr::problems(ano_i) -> synthese_import
      
      ano_i <- ano_i %>%
                         dplyr::mutate(DTSORT   = lubridate::dmy(DTSORT),
                                       DTENT    = lubridate::dmy(DTENT),
                                       cok = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                                                (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') +
                                                (CRCDNAI=='0') + (CRCSEXE=='0')==9),
                                       MTFACTMO = MTFACTMO/100,
                                       MTFORJOU = MTFORJOU/100,
                                       MTFACTOT = MTFACTOT/100,
                                       MTBASERM = MTBASERM/100,
                                       MTRMBAMC = MTRMBAMC/100,
                                       TAUXRM   = TAUXRM  /100,
                                       MTMALPAR = MTMALPAR/100)
      
      
    }
    if (2011<annee & annee<2013){
      
      ano_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                                              readr::fwf_widths(af,an), col_types =at, na=character(), ...)  
  readr::problems(ano_i) -> synthese_import
  ano_i <- ano_i %>%
    dplyr::mutate(DTSORT   = lubridate::dmy(DTSORT),
                  DTENT    = lubridate::dmy(DTENT),
                  cok      = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                                (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') == 7),
                  MTFACTMO = MTFACTMO/100,
                  MTFORJOU = MTFORJOU/100,
                  MTFACTOT = MTFACTOT/100,
                  MTBASERM = MTBASERM/100,
                  MTRMBAMC = MTRMBAMC/100,
                  TAUXRM   = TAUXRM  /100,
                  MTMALPAR = MTMALPAR/100)
  
    }
    if (annee == 2011){
      ano_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                             readr::fwf_widths(af,an), col_types =at, na=character(), ...)  
      readr::problems(ano_i) -> synthese_import
      
      ano_i <- ano_i %>%
                          dplyr::mutate(DTSORT   = lubridate::dmy(DTSORT),
                                        DTENT    = lubridate::dmy(DTENT),
                                        cok      = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                                                      (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') == 7),
                                        MTFACTMO = MTFACTMO/100,
                                        MTFORJOU = MTFORJOU/100,
                                        MTFACTOT = MTFACTOT/100,
                                        MTBASERM = MTBASERM/100,
                                        TAUXRM   = TAUXRM  /100,
                                        MTMALPAR = MTMALPAR/100)
    }
    
    Fillers <- names(ano_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    ano_i <- ano_i[,!(names(ano_i) %in% Fillers)]
    
    if (lib==T){
      v <- c(libelles[!is.na(libelles)], "Chaînage Ok")
      ano_i <- ano_i  %>%  sjlabelled::set_label(v)
    }
  }
  
  if (typano=="in"){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rum_ano', an == substr(as.character(annee),3,4))
    
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
    
    
    if (2011<annee){
      ano_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano.txt"),
                             readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
       
      readr::problems(ano_i) -> synthese_import
      
      ano_i <- ano_i %>% 
        dplyr::mutate(DTHOSP   = lubridate::dmy(DTHOSP),
                      MTFACTMO = MTFACTMO/100,
                      MTFORJOU = MTFORJOU/100,
                      MTFACTOT = MTFACTOT/100,
                      MTRMAMC  = MTRMAMC /100,
                      MTBASERM = MTBASERM/100,
                      TAUXRM   = TAUXRM  /100,
                      MTMAJPAR = MTMAJPAR/100)
    }
    if (annee == 2011){
      ano_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano.txt"),
                             readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
      
      readr::problems(ano_i) -> synthese_import
      
      ano_i <- ano_i %>% 
        dplyr::mutate(
          MTFACTMO = MTFACTMO/100,
          MTFORJOU = MTFORJOU/100,
          MTFACTOT = MTFACTOT/100,
          MTBASERM = MTBASERM/100,
          TAUXRM   = TAUXRM  /100,
          MTMAJPAR = MTMAJPAR/100)
    }
    
    Fillers <- names(ano_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    ano_i <- ano_i[,!(names(ano_i) %in% Fillers)]
    
    if (lib==T){
      v <- libelles[!is.na(libelles)]
      ano_i <- ano_i  %>%  sjlabelled::set_label(v)
    }
  }
  
  if (tolower_names){
    names(ano_i) <- tolower(names(ano_i))
  }
  attr(ano_i,"problems") <- synthese_import
  return(ano_i)
}

#' ~ MCO - Import des Med
#'
#' Import des fichiers MED In ou Out.
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param typmed Type de donnees In / Out
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tibble) contenant les médicaments In ou Out 
#' (T2A, ATU et thrombo selon l'existence des fichiers : si le fichier n'existe pas, pas de donnée importée). 
#' Pour discriminer le type de prestation, la colonne TYPEPREST donne l'information : T2A 06 - ATU 09 - THROMBO 10
#'
#' @examples
#' \dontrun{
#'    imed_mco('750712184',2015,12,'~/Documents/data/mco') -> med_out15
#'    imed_mco('750712184',2015,12,'~/Documents/data/mco', typmed = "in") -> med_in15
#' }
#'
#' @author G. Pressiat
#'
#' @usage imed_mco(finess, annee, mois, path, lib = T, tolower_names = F, typmed = c('out', 'in'))
#' @seealso \code{\link{irum}}, \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @export imed_mco
#' @export
imed_mco <- function(...){
  UseMethod('imed_mco')
}




#' @export
imed_mco.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(imed_mco.default, param2)
}



#' @export
imed_mco.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(imed_mco.default, param2)
}

#' @export
imed_mco.default <- function(finess, annee, mois, path, typmed = c("out", "in"), lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  typmed <- match.arg(typmed)
  if (!(typmed %in% c('in', 'out'))){
    stop('Paramètre typmed incorrect')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  if (typmed=="out"){
    # med_out
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rsa_med', an == substr(as.character(annee),3,4))
    
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
    
    med_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".med"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    readr::problems(med_i) -> synthese_import
    
    med_i <- med_i %>%
      dplyr::mutate(NBADM = NBADM/1000,
                    PRIX =  PRIX /1000)
    
    
    info = file.info(paste0(path,"/",finess,".",annee,".",mois,".medatu"))
    if (info$size >0 & !is.na(info$size)){
      med_i2<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".medatu"),
                              readr::fwf_widths(af,an), col_types =at, na=character(), ...)
      synthese_import <- dplyr::bind_rows(synthese_import, readr::problems(med_i2))
      
      med_i2 <- med_i2 %>%
        dplyr::mutate(NBADM = NBADM/1000,
                      PRIX =  PRIX /1000)
      med_i <- rbind(med_i,med_i2)
    }
    
    info = file.info(paste0(path,"/",finess,".",annee,".",mois,".medthrombo"))
    if (info$size >0 & !is.na(info$size)){
      med_i3<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".medthrombo"),
                              readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
      
      synthese_import <- dplyr::bind_rows(synthese_import, readr::problems(med_i3))
      
      med_i3 <- med_i3 %>%
        dplyr::mutate(NBADM = NBADM/1000,
                      PRIX =  PRIX /1000)
      
      med_i <- rbind(med_i,med_i3)
    }
    if (lib==T){
      v <- libelles
      med_i <- med_i  %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(med_i) <- tolower(names(med_i))
    }
    attr(med_i,"problems") <- synthese_import
    return(med_i)
  }
  if (typmed=="in"){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rum_med', an == substr(as.character(annee),3,4))
    
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
    info = file.info(paste0(path,"/",finess,".",annee,".",mois,".med.txt"))
    if (info$size >0 & !is.na(info$size)){
      med_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".med.txt"),
                             readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
      
      readr::problems(med_i) -> synthese_import
      
      med_i <- med_i %>%
        dplyr::mutate(NBADM = NBADM/1000,
                      PRIX =  PRIX /1000)
    }
    if (lib==T){
      v <- libelles
      v<- v[!is.na(v)]
      med_i <- med_i %>%  sjlabelled::set_label(v)
    }
    med_i %>% dplyr::mutate(DTDISP = lubridate::dmy(DTDISP)) -> med_i
    if (tolower_names){
      names(med_i) <- tolower(names(med_i))
    }
    attr(med_i,"problems") <- synthese_import
    return(med_i)
  }
  
}

#' ~ MCO - Import des DMI
#'
#' Import des fichiers DMI In ou Out.
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param typdmi Type de donnees In / Out
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les dispositifs médicaux implantables In ou Out (T2A, ATU et thrombo selon l'existence des fichiers : si le fichier n'existe pas, pas de donnée importée). Pour discriminer le type de prestation, la colonne TYPEPREST donne l'information : T2A 06 - ATU 09 - THROMBO 10
#'
#' @examples
#' \dontrun{
#'    idmi_mco('750712184',2015,12,'~/Documents/data/mco') -> dmi_out15
#'    idmi_mco('750712184',2015,12,'~/Documents/data/mco', typdmi = "in") -> dmi_in15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}}
#' @export idmi_mco
#' @export
idmi_mco <- function(...){
  UseMethod('idmi_mco')
}


#' @export
idmi_mco.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(idmi_mco.default, param2)
}


#' @export
idmi_mco.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(idmi_mco.default, param2)
}

#' @export
idmi_mco.default <- function(finess, annee, mois, path, typdmi = c("out", "in"), lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  typdmi <- match.arg(typdmi)
  if (!(typdmi %in% c('in', 'out'))){
    stop('Paramètre typdmi incorrect')
  }
  
  
  #op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  if (typdmi=="out"){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rsa_dmi', an == substr(as.character(annee),3,4))
    
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
    dmi_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".dmip"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    
    readr::problems(dmi_i) -> synthese_import
    
    dmi_i <- dmi_i %>%
      dplyr::mutate(PRIX   =  PRIX /1000)
    
    
    if (lib==T){
      v <- libelles
      dmi_i <- dmi_i  %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(dmi_i) <- tolower(names(dmi_i))
    }
    attr(dmi_i,"problems") <- synthese_import
    return(dmi_i)
  }
  if (typdmi=="in"){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rum_dmi', an == substr(as.character(annee),3,4))
    
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
    dmi_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".dmi.txt"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    
    readr::problems(dmi_i) -> synthese_import
    
    dmi_i <- dmi_i %>%
      dplyr::mutate(PRIX   =  PRIX /1000,
                    DTPOSE = lubridate::dmy(DTPOSE))
    
    
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      dmi_i <- dmi_i  %>% dplyr::select(-Fil1,-Fil2) %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(dmi_i) <- tolower(names(dmi_i))
    }
    attr(dmi_i,"problems") <- synthese_import
    return(dmi_i)
  }
}

#' ~ MCO - Import des erreurs Leg
#'
#' Import de la liste d'erreurs de génération Genrsa
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param reshape booleen TRUE/FALSE : la donnee doit-elle etre restructuree ? une ligne = une erreur, sinon, une ligne = un sejour. par defaut a F
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les erreurs Out.
#'
#' @examples
#' \dontrun{
#'    ileg_mco('750712184',2015,12,'~/Documents/data/mco') -> leg15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @export ileg_mco
#' @usage ileg_mco(finess, annee, mois, path, reshape = F, tolower_names = F, ...)
#' @export
ileg_mco <- function(...){
  UseMethod('ileg_mco')
}


#' @export
ileg_mco.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(ileg_mco.default, param2)
}



#' @export
ileg_mco.list <- function(l , ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(ileg_mco.default, param2)
}

#' @export
ileg_mco.default <- function(finess, annee, mois, path, reshape = F, tolower_names = F, ...){
  
  leg_i <- readr::read_lines(paste0(path,"/",finess,".",annee,".",mois,".leg"), ...)
  
  extz <- function(x,pat){unlist(lapply(stringr::str_extract_all(x,pat),toString) )}
  
  u <- stringr::str_split(leg_i, "\\;", simplify = T)
  leg_i1 <- dplyr::data_frame(FINESS    = u[,1],
                              MOIS      = u[,2],
                              ANNEE     = u[,3],
                              CLE_RSA   = u[,4],
                              NBERR     = u[,5] %>% as.integer())
  
  leg_i1 <- as.data.frame(lapply(leg_i1, rep, leg_i1$NBERR), stringsAsFactors = F)
  legs <- u[,6:ncol(u)]
  legs<- legs[legs != ""] 
  leg_i1 <- dplyr::bind_cols(leg_i1, data.frame(EG = as.character(legs), stringsAsFactors = F))
  
  if (reshape==T){
    if (tolower_names){
      names(leg_i1) <- tolower(names(leg_i1))
    }
    return(leg_i1)
  }
  
  leg_i1 %>% 
    dplyr::group_by(FINESS, MOIS, ANNEE, CLE_RSA, NBERR) %>%
    dplyr::summarise(EG = paste(EG, collapse = ", ")) -> leg_i1
  if (tolower_names){
    names(leg_i1) <- tolower(names(leg_i1))
  }
  return(dplyr::ungroup(leg_i1))
  
}


#' ~ TRA - Ajout du TRA aux donnees Out
#'
#' Ajout du TRA par dplyr::inner_join
#'
#'
#' @param table Table a laquelle rajouter le tra
#' @param tra tra a rajouter
#' @param sel Variable a garder du tra ; sel = 1 : numero de sejour, sel = 2 : toutes les variables
#' @param champ Champ PMSI : mco, had, ssr, psy : deux tra en psy : psy_rpsa, psy_r3a
#'
#' @return Une table contenant le inner_join entre table et tra
#'
#' @examples
#' \dontrun{
#'    med <- imed_mco('750712184',2015,12,"~/Documents/data/mco","out")
#'    tra <- itra('750712184',2015,12,"~/Documents/data/mco")
#'    med <- inner_tra(med,tra)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}}, \code{\link{imed_mco}}, \code{\link{irpsa}}, \code{\link{irha}}, \code{\link{irapss}}

#' @export
inner_tra <- function(table, tra, sel = 1, champ = "mco"){
#inner_tra <- function(table, tra ){
  
  suppressMessages(  dplyr::inner_join(table, tra) )
  if (champ == "mco"){
    if (sel==1){
      return( suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(c(1:2,4,8)))))
    }
    if (sel==2){
      return( suppressMessages( dplyr::inner_join(table, tra )))
    }
  }
  if (champ == "had"){
    if (sel==1){
      return( suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(1:4))))
    }
    if (sel==2){
      return( suppressMessages( dplyr::inner_join(table, tra)))
    }
  }
  if (champ == "ssr"){
    if (sel==1){
      return(suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(c(1,3,4,5)))))
    }
    if (sel==2){
      return(suppressMessages(  dplyr::inner_join(table, tra)))
    }
  }
  if (champ == "psy_rpsa"){
    if (sel==1){
      return(suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(c(3:5,10)))))
    }
    if (sel==2){
      return(suppressMessages(  dplyr::inner_join(table, tra)))
    }
  }
  if (champ == "psy_r3a"){
    if (sel==1){
      return(suppressMessages( dplyr::inner_join(table, tra)))
    }
    if (sel==2){
      return(suppressMessages(  dplyr::inner_join(table, tra)))
    }
  }
  if (!(sel %in% 1:2)){
    print("Paramètre sel incorrect")
    return(NULL)
  }
  if (!(champ %in% c('mco','had','ssr','psyrpsa','psyr3a'))){
    print("Paramètre champ incorrect")
    return(NULL)
  }

}


#' ~ MCO - Import des DIAP
#'
#' Imports des fichiers DIAP In / Out
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param typdiap Type de donnees In / Out
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les dialyses péritonéales In ou Out.
#'
#' @examples
#' \dontrun{
#'    idiap <- idiap('750712184',2015,12,"~/Documents/data/mco")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}}
#' @usage idiap(finess, annee, mois, path, 
#' typdiap = c("out", "in"), 
#' lib = T, tolower_names = F, ...)
#' @export idiap
#' @export
idiap <- function(...){
  UseMethod('idiap')
}


#' @export
idiap.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(idiap.default, param2)
}


#' @export
idiap.list <- function(l , ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(idiap.default, param2)
}

#' @export
idiap.default <- function(finess, annee, mois, path, 
                          typdiap = c("out", "in"), lib = T, 
                          tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  typdiap <- match.arg(typdiap)
  if (!(typdiap %in% c('in', 'out'))){
    stop('Paramètre typdiap incorrect')
  }
  
  
  #op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  if (typdiap=="out"){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rsa_diap', an == substr(as.character(annee),3,4))
    
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
    
    diap_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".diap"),
                            readr::fwf_widths(af,an), col_types =at, na=character(), ...)
    readr::problems(diap_i) -> synthese_import
    
    
    if (lib==T){
      v <- libelles
      diap_i <- diap_i  %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(diap_i) <- tolower(names(diap_i))
    }
    attr(diap_i,"problems") <- synthese_import
    return(diap_i)
  }
  if (typdiap=="in"){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'ffc_in', an == substr(as.character(annee),3,4))
    
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
    
    diap_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".diap.txt"),
                            readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    readr::problems(diap_i) -> synthese_import
    
    diap_i <- diap_i %>%
      dplyr::mutate(DTDEBUT = lubridate::dmy(DTDEBUT))
    
    
    if (lib==T){
      
      v <- libelles
      diap_i <- diap_i  %>%  sjlabelled::set_label(v)
    }
    if (tolower_names){
      names(diap_i) <- tolower(names(diap_i))
    }
    attr(diap_i,"problems") <- synthese_import
    return(diap_i)
  }
  
}



#' ~ MCO - Import des donnees UM du Out
#'
#' Imports du fichier IUM MCO
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires à passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les informations structures du Out.
#'
#' @examples
#' \dontrun{
#'    um <- iium('750712184',2015,12,"~/Documents/data/mco")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iium(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export iium
#' @export
iium <- function(...){
  UseMethod('iium')
}


#' @export
iium.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iium.default, param2)
}

#' @export
iium.list <- function(l , ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iium.default, param2)
}

#' @export
iium.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'um', an == substr(as.character(annee),3,4))
  
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
  
  ium_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ium"),
                         readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
  readr::problems(ium_i) -> synthese_import
  
  ium_i <- ium_i %>%
    dplyr::mutate(DTEAUT = lubridate::dmy(DTEAUT))
  
  
  if (lib==T){
    v <- libelles
    ium_i <- ium_i  %>%  sjlabelled::set_label(v)
  }
  if (tolower_names){
    names(ium_i) <- tolower(names(ium_i))
  }
  attr(ium_i,"problems") <- synthese_import
  return(ium_i)
}

#' ~ MCO - Import des PO
#'
#' Imports des fichiers PO In / Out
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param typpo Type de donnees In / Out
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#' @return Une table (data.frame, tibble) contenant les prélèvements d'organes In ou Out.
#'
#' @examples
#' \dontrun{
#'    po <- ipo('750712184',2015,12,"~/Documents/data/mco")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}}, utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ipo(finess, annee, mois, path, typpo = c("out", "in"), lib = T, tolower_names = F, ...)
#' @export ipo
#' @export
ipo <- function( ...){
  UseMethod('ipo')
}


#' @export
ipo.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(ipo.default, param2)
}


#' @export
ipo.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(ipo.default, param2)
}

#' @export
ipo.default <- function(finess, annee, mois, path, typpo = c("out", "in"), lib = T, tolower_names = F, ...){
if (annee<2011|annee > 2020){
  stop('Année PMSI non prise en charge\n')
}
if (mois<1|mois>12){
  stop('Mois incorrect\n')
}
typpo <- match.arg(typpo)
if (!(typpo %in% c('in', 'out'))){
  stop('Paramètre typpo incorrect')
}


#op <- options(digits.secs = 6)
un<-Sys.time()


if (typpo=="out"){
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'rsa_po', an == substr(as.character(annee),3,4))
  
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
  
  po_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".porg"),
                        readr::fwf_widths(af,an), col_types =at, na=character(), ...)
  readr::problems(po_i) -> synthese_import
  
  
  if (lib==T){
    v <- libelles
    po_i <- po_i  %>%  sjlabelled::set_label(v)
  }
  if (tolower_names){
    names(po_i) <- tolower(names(po_i))
  }
  
  attr(po_i,"problems") <- synthese_import
  return(po_i)
}
if (typpo=="in"){
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'mco', table == 'ffc_in', an == substr(as.character(annee),3,4))
  
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
  
  po_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".porg.txt"),
                        readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
  readr::problems(po_i) -> synthese_import
  
  po_i <- po_i %>%
    dplyr::mutate(DTDEBUT = lubridate::dmy(DTDEBUT))
  
  
  if (lib==T){
    v <- libelles
    po_i <- po_i  %>%  sjlabelled::set_label(v)
  }
  if (tolower_names){
    names(po_i) <- tolower(names(po_i))
  }
  attr(po_i,"problems") <- synthese_import
  return(po_i)
}

}




##############################################
####################### HAD ##################
##############################################

#' ~ HAD - Import des RAPSS
#'
#' Imports du fichier RAPSS
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Paprica) :
#' \emph{finess.annee.moisc.rapss}
#'
#' \strong{750712184.2016.2.rapss}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des données sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une classe S3 contenant les tables (data.frame, tbl_df ou tbl) importées (rapss, acdi, ght).
#'
#' @examples
#' \dontrun{
#'    um <- iium('750712184',2015,12,"~/Documents/data/had")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{iano_had}}, \code{\link{ileg_had}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irapss(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export irapss
#' @export
irapss <- function(...){
  UseMethod('irapss')
}


#' @export
irapss.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irapss.default, param2)
}


#' @export
irapss.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irapss.default, param2)
}

#' @export
irapss.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  # cat(paste("L'objet retourné prendra la forme d'une classe S3.
  #           $rapss pour accéder à la table RSA
  #           $acdi pour accéder à la table ACDI
  #           $ght pour accéder aux ght etb et paprica\n\n"))
  
  
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'had', table == 'rapss', an == substr(as.character(annee),3,4))
  
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
  
  rapss_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rapss"),
                             readr::fwf_widths(af,an), col_types = at , na=character(),...)
  readr::problems(rapss_i) -> synthese_import
  
  if (annee==2011){
    zght <- ".{5}"
    zd   <- ".{1,6}"
    zA   <- ".{1,17}"
    
    rapss_i <- rapss_i %>% dplyr::mutate(
      # Diagnostics et actes
      da       = ifelse(NBDA>0,stringr::str_sub(Z,1,NBDA*6),""),
      lda    = stringr::str_extract_all(da,zd),
      za       = ifelse(NBZA>0,stringr::str_sub(Z,1+NBDA*6,NBDA*6+NBZA*17),""),
      lactes    = stringr::str_extract_all(za,zA),
      
      # groupage Etablissement
      NOVRPSS = stringr::str_sub(Z, 1+NBDA*6+NBZA*17, NBDA*6+NBZA*17+3),
      ETB_VCLASS = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+3, NBDA*6+NBZA*17+3+2),
      ETB_CDRETR = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+3+2, NBDA*6+NBZA*17+3+2+3),
      ETB_GHPC  = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+3+2+3, NBDA*6+NBZA*17+3+2+3+4),
      ETB_NBGHT  = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+3+2+3+4, NBDA*6+NBZA*17+3+2+3+4+1) %>% as.numeric(),
      etb_ght   = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+13,NBDA*6+NBZA*17+13+5*ETB_NBGHT),
      letb_ght  = stringr::str_extract_all(etb_ght,zght),
      
      # groupage Paprica
      PAPRICA_VCLASS = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+13+5*ETB_NBGHT,NBDA*6+NBZA*17+13+5*ETB_NBGHT+2),
      PAPRICA_CDRETR = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2,NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3),
      PAPRICA_GHPC = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3,NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3+4),
      PAPRICA_NBGHT  = stringr::str_sub(Z, 1+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3+4,NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3+4+1) %>% as.numeric(),
      pap_ght   = stringr::str_sub(Z,1+NBDA*6+NBZA*17+13+5*ETB_NBGHT+10,NBDA*6+NBZA*17+13+5*ETB_NBGHT+10+PAPRICA_NBGHT*5),
      lpap_ght  = stringr::str_extract_all(pap_ght,zght)
    )
  }
  
  if (2011< annee & annee<=2014){
    zght <- ".{5}"
    zd   <- ".{1,6}"
    zA   <- ".{1,17}"
    
    rapss_i <- rapss_i %>% dplyr::mutate(
      # Diagnostics et actes
      dmpp     = ifelse(NBDIAGMPP>0,stringr::str_sub(Z,1,NBDIAGMPP*6),""),
      ldmpp    = stringr::str_extract_all(dmpp,zd),
      dmpa     = ifelse(NBDIAGMPA>0,stringr::str_sub(Z,1+NBDIAGMPP*6,NBDIAGMPP*6+NBDIAGMPA*6),""),
      ldmpa    = stringr::str_extract_all(dmpa,zd),
      da       = ifelse(NBDA>0,stringr::str_sub(Z,1+NBDIAGMPP*6+NBDIAGMPA*6,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6),""),
      lda    = stringr::str_extract_all(da,zd),
      za       = ifelse(NBZA>0,stringr::str_sub(Z,1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17),""),
      lactes    = stringr::str_extract_all(za,zA),
      
      # groupage Etablissement
      NOVRPSS = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3),
      ETB_VCLASS = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3+2),
      ETB_CDRETR = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3+2, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3+2+3),
      ETB_GHPC  = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3+2+3, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3+2+3+4),
      ETB_NBGHT  = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3+2+3+4, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+3+2+3+4+1) %>% as.numeric(),
      etb_ght   = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT),
      letb_ght  = stringr::str_extract_all(etb_ght,zght),
      
      # groupage Paprica
      PAPRICA_VCLASS = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2),
      PAPRICA_CDRETR = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3),
      PAPRICA_GHPC = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3+4),
      PAPRICA_NBGHT  = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3+4,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+2+3+4+1) %>% as.numeric(),
      pap_ght   = stringr::str_sub(Z,1+ NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+10,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*17+13+5*ETB_NBGHT+10+PAPRICA_NBGHT*5),
      lpap_ght  = stringr::str_extract_all(pap_ght,zght)
    )
    
  }
  
  if (annee>2014){
    zght <- ".{5}"
    zd   <- ".{1,6}"
    zA   <- ".{1,19}"
    
    rapss_i <- rapss_i %>% dplyr::mutate(
      DP = stringr::str_trim(DP),
      # Diagnostics et actes
      dmpp     = ifelse(NBDIAGMPP>0,stringr::str_sub(Z,1,NBDIAGMPP*6),""),
      ldmpp    = stringr::str_extract_all(dmpp,zd),
      dmpa     = ifelse(NBDIAGMPA>0,stringr::str_sub(Z,1+NBDIAGMPP*6,NBDIAGMPP*6+NBDIAGMPA*6),""),
      ldmpa    = stringr::str_extract_all(dmpa,zd),
      da       = ifelse(NBDA>0,stringr::str_sub(Z,1+NBDIAGMPP*6+NBDIAGMPA*6,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6),""),
      lda    = stringr::str_extract_all(da,zd),
      za       = ifelse(NBZA>0,stringr::str_sub(Z,1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19),""),
      lactes    = stringr::str_extract_all(za,zA),
      
      # groupage Etablissement
      NOVRPSS = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3),
      ETB_VCLASS = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3+2),
      ETB_CDRETR = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3+2, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3+2+3),
      ETB_GHPC  = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3+2+3, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3+2+3+4),
      ETB_NBGHT  = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3+2+3+4, NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+3+2+3+4+1) %>% as.numeric(),
      etb_ght   = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT),
      letb_ght  = stringr::str_extract_all(etb_ght,zght),
      
      # groupage Paprica
      PAPRICA_VCLASS = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+2),
      PAPRICA_CDRETR = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+2,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+2+3),
      PAPRICA_GHPC = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+2+3,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+2+3+4),
      PAPRICA_NBGHT  = stringr::str_sub(Z, 1+NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+2+3+4,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+2+3+4+1) %>% as.numeric(),
      pap_ght   = stringr::str_sub(Z,1+ NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+10,NBDIAGMPP*6+NBDIAGMPA*6+NBDA*6+NBZA*19+13+5*ETB_NBGHT+10+PAPRICA_NBGHT*5),
      lpap_ght  = stringr::str_extract_all(pap_ght,zght)
    )
  }
  
  if (annee>2011){
    actes <- purrr::flatten_chr(rapss_i$lactes)
    df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,NBZA)
    df <- as.data.frame(lapply(df, rep, df$NBZA), stringsAsFactors = F) %>% dplyr::tbl_df()
    actes <- dplyr::bind_cols(df,data.frame(ZACTES = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    actes <- dplyr::mutate(actes, CODE = 'A') %>% dplyr::select(-NBZA) %>% dplyr::mutate(
      DELAI  = stringr::str_sub(ZACTES, 1, 4) %>% as.numeric(),
      CDCCAM = stringr::str_sub(ZACTES, 5,11),
      PHASE  = stringr::str_sub(ZACTES,14,14),
      ACT    = stringr::str_sub(ZACTES,15,15),
      EXTDOC = stringr::str_sub(ZACTES,16,16),
      NBEXEC = stringr::str_sub(ZACTES,17,18),
      INDVAL = stringr::str_sub(ZACTES,19,19))
    
    da <- purrr::flatten_chr(rapss_i$lda)
    df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    da <- dplyr::mutate(da, CODE = 'DA') %>% dplyr::select(-NBDA)
    
    dmpp <- purrr::flatten_chr(rapss_i$ldmpp)
    df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,NBDIAGMPP)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGMPP), stringsAsFactors = F) %>% dplyr::tbl_df()
    dmpp <- dplyr::bind_cols(df,data.frame(DMPP = stringr::str_trim(dmpp), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    dmpp <- dplyr::mutate(dmpp, CODE = 'DMPP') %>% dplyr::select(-NBDIAGMPP)
    
    dmpa <- purrr::flatten_chr(rapss_i$ldmpa)
    df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,NBDIAGMPA)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGMPA), stringsAsFactors = F) %>% dplyr::tbl_df()
    dmpa <- dplyr::bind_cols(df,data.frame(DMPA = stringr::str_trim(dmpa), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    dmpa <- dplyr::mutate(dmpa, CODE = 'DMPA') %>% dplyr::select(-NBDIAGMPA)
    
    acdi <- dplyr::bind_rows(actes,da,dmpp,dmpa) %>% dplyr::select(-ZACTES)
    rapss_i <- rapss_i %>% dplyr::select(-c(FILLER,Z,da,za,dmpp,dmpa,lda,ldmpp,ldmpa,lactes))
    
  }
  
  if (annee==2011){
    actes <- purrr::flatten_chr(rapss_i$lactes)
    df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,NBZA)
    df <- as.data.frame(lapply(df, rep, df$NBZA), stringsAsFactors = F) %>% dplyr::tbl_df()
    actes <- dplyr::bind_cols(df,data.frame(ZACTES = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    actes <- dplyr::mutate(actes, CODE = 'A') %>% dplyr::select(-NBZA) %>% dplyr::mutate(
      DELAI  = stringr::str_sub(ZACTES, 1, 4) %>% as.numeric(),
      CDCCAM = stringr::str_sub(ZACTES, 5,11),
      PHASE  = stringr::str_sub(ZACTES,14,14),
      ACT    = stringr::str_sub(ZACTES,15,15),
      EXTDOC = stringr::str_sub(ZACTES,16,16),
      NBEXEC = stringr::str_sub(ZACTES,17,18),
      INDVAL = stringr::str_sub(ZACTES,19,19))
    
    da <- purrr::flatten_chr(rapss_i$lda)
    df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    da <- dplyr::mutate(da, CODE = 'DA') %>% dplyr::select(-NBDA)
    
    acdi <- dplyr::bind_rows(actes,da) %>% dplyr::select(-ZACTES)
    rapss_i <- rapss_i %>% dplyr::select(-c(Z,da,za,lda,lactes))
  }
  
  etb_ght <- purrr::flatten_chr(rapss_i$letb_ght)
  df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,NOVRPSS, VCLASS = ETB_VCLASS, CDRETR = ETB_CDRETR,
                                  GHPC = ETB_GHPC, NBGHT = ETB_NBGHT)
  df <- as.data.frame(lapply(df, rep, df$NBGHT), stringsAsFactors = F) %>% dplyr::tbl_df()
  etb_ght <- dplyr::bind_cols(df,data.frame(etb_ght, stringsAsFactors = F) ) %>% dplyr::tbl_df()  %>%
    dplyr::mutate(TYPGHT='ETAB',
                  NUMGHT = stringr::str_sub(etb_ght,1,2),
                  JOURSGHT = stringr::str_sub(etb_ght,3,5) %>% as.numeric() ) %>%
    dplyr::select(-etb_ght)
  
  pap_ght <- purrr::flatten_chr(rapss_i$lpap_ght)
  df <- rapss_i %>% dplyr::select(NOSEQSEJ,NOSEQ,NOSOUSSEQ,VCLASS=PAPRICA_VCLASS, CDRETR = PAPRICA_CDRETR, GHPC = PAPRICA_GHPC,
                                  NBGHT = PAPRICA_NBGHT )
  df <- as.data.frame(lapply(df, rep, df$NBGHT), stringsAsFactors = F) %>% dplyr::tbl_df()
  pap_ght <- dplyr::bind_cols(df,data.frame(pap_ght, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>%
    dplyr::mutate(TYPGHT='PAPRICA',
                  NUMGHT = stringr::str_sub(pap_ght,1,2),
                  JOURSGHT = stringr::str_sub(pap_ght,3,5) %>% as.numeric() ) %>%
    dplyr::select(-pap_ght)
  
  ght <- dplyr::bind_rows(etb_ght,pap_ght)
  
  rapss_i <- rapss_i %>% dplyr::select(-c(lpap_ght,letb_ght))
  rapss_i <- rapss_i %>% dplyr::select(- dplyr::starts_with("PAP"),- dplyr::starts_with("ETB"),-NOVRPSS)
  acdi[is.na(acdi)] <- ""
  rapss_i[is.na(rapss_i)] <- ""
  ght[is.na(ght)] <- ""
  if (lib==T){
    
    ght <- ght %>% sjlabelled::set_label(c('N° du séjour HAD', 'N° de la séquence', 'N° de la sous-séquence',
                                       'N° de version du RPSS','Version de classification',
                                       'Codes retours', 'Groupe homogène de prise en charge',
                                       'Nombre de GHT','Type de GHT', 'N° du GHT', 'Nombre de jours du GHT'))
    
    if (annee==2011){
      acdi <- acdi %>% sjlabelled::set_label(c('N° du séjour HAD', 'N° de la séquence', 'N° de la sous-séquence',
                                           'Type de code (A : Acte, DA : Diagnostic Associé)',
                                           "Délai depuis la date d'entrée","Code CCAM","Phase", "Activité", "Extension documentaire",
                                           "Nombre d'exécécutions", "Indic Validité de l'acte","Diagnostic Associé"))
    }else{
      acdi <- acdi %>% sjlabelled::set_label(c('N° du séjour HAD', 'N° de la séquence', 'N° de la sous-séquence',
                                           'Type de code (A : Acte, DA : Diagnostic Associé, DMPP : Diagnostic Mode principal, DMPA : Diagnostic Mode associé)',
                                           "Délai depuis la date d'entrée","Code CCAM","Phase", "Activité", "Extension documentaire",
                                           "Nombre d'exécécutions", "Indic Validité de l'acte","Diagnostic Associé", "Diagnostic MPP",
                                           "Diagnostic MPA"))
    }
    
  }
  
  Fillers <- names(rapss_i)
  Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
  rapss_i <- rapss_i[,!(names(rapss_i) %in% Fillers)]
  
  if (lib==T){
    rapss_i <- rapss_i  %>% sjlabelled::set_label(libelles[!is.na(libelles)])
  }
  if (tolower_names){
    names(rapss_i) <- tolower(names(rapss_i))
    names(acdi) <- tolower(names(acdi))
    names(ght) <- tolower(names(ght))
  }
  
  rapss_1 <- list(rapss = rapss_i, acdi = acdi, ght = ght)
  attr(rapss_1, "problems") <- synthese_import
  return(rapss_1)
  
}

#' ~ HAD - Import des Anohosp
#'
#' Imports du fichier Ano Out
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Paprica) :
#' \emph{finess.annee.moisc.ano}
#'
#' \strong{750712184.2016.2.ano}
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
#' @return Une table (data.frame, tbl_df) contenant les données Anohosp HAD du Out.
#'
#' @examples
#' \dontrun{
#'    anoh <- iano_had('750712184',2015,12,"~/Documents/data/had")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iano_had(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export iano_had
#' @export
iano_had <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  UseMethod('iano_had')
}


#' @export
iano_had.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iano_had.default, param2)
}


#' @export
iano_had.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iano_had.default, param2)
}

#' @export
iano_had.default <- function(finess, annee,mois, path, lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'had', table == 'rapss_ano', an == substr(as.character(annee),3,4))
  
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
  if (annee<=2012){
    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...) 
    readr::problems(ano_i) -> synthese_import
    
    ano_i <- ano_i %>%
      dplyr::mutate(DTSOR   = lubridate::dmy(DTSOR),
                    DTENT    = lubridate::dmy(DTENT),
                    cok = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                             (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') ==7),
                    MTFACTMO = MTFACTMO/100,
                    MTFORJOU = MTFORJOU/100,
                    MTFACTOT = MTFACTOT/100,
                    MTBASERM = MTBASERM/100,
                    TAUXRM   = TAUXRM  /100)
  }
  if (annee>2012){
    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...) 
    readr::problems(ano_i) -> synthese_import
    
    ano_i <- ano_i %>%
      dplyr::mutate(DTSOR   = lubridate::dmy(DTSOR),
                    DTENT    = lubridate::dmy(DTENT),
                    cok = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                             (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') +
                             (CRCDNAI=='0') + (CRCSEXE=='0')==9),
                    MTFACTMO = MTFACTMO/100,
                    MTFORJOU = MTFORJOU/100,
                    MTFACTOT = MTFACTOT/100,
                    MTBASERM = MTBASERM/100,
                    MTRMBAMC = MTRMBAMC/100,
                    TAUXRM   = TAUXRM  /100)
  }
  if (lib==T){
  ano_i <- ano_i %>% sjlabelled::set_label(c(libelles,'Chaînage Ok'))
  ano_i <- ano_i %>% dplyr::select(-dplyr::starts_with("Fill"))
  }
  if (tolower_names){
    names(ano_i) <- tolower(names(ano_i))
  }

  attr(ano_i,"problems") <- synthese_import
  return(ano_i)
}

#' ~ HAD - Import des Med
#'
#' Imports du fichier Med Out
#'
#' Formats depuis 2011 pris en charge
#'   
#' import des med, medatu et mchl si le fichier existe
#' 
#' Structure du nom du fichier attendu (sortie de Paprica) :
#' \emph{finess.annee.moisc.med}
#'
#' \strong{750712184.2016.2.med}
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
#' @return Une table (data.frame, tbl_df) contenant les données médicaments HAD du Out.
#'
#' @examples
#' \dontrun{
#'    medh <- imed_had('750712184',2015,12,"~/Documents/data/had")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}}
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage imed_had(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export imed_had
#' @export
imed_had <- function(...){
  UseMethod('imed_had')
}


#' @export
imed_had.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(imed_had.default, param2)
}

#' @export
imed_had.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(imed_had.default, param2)
}

#' @export
imed_had.default <- function(finess, annee, mois, path, lib=T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'had', table == 'rapss_med', an == substr(as.character(annee),3,4))
  
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
  med_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".med"),
                           readr::fwf_widths(af,an), col_types = at , na=character(), ...) 
  readr::problems(med_i) -> synthese_import
  
  med_i <- med_i %>%
    dplyr::mutate(NBADM = NBADM/1000,
                  PRIX  = PRIX /1000) %>% sjlabelled::set_label(libelles)
  
  info = file.info(paste0(path,"/",finess,".",annee,".",mois,".medatu"))
  if (info$size >0 & !is.na(info$size)){
    med_i2<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".medatu"),
                            readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
     synthese_import <- dplyr::bind_rows(synthese_import, readr::problems(med_i2))
    
    med_i2 <- med_i2 %>%
      dplyr::mutate(NBADM = NBADM/1000,
                    PRIX =  PRIX /1000) %>% sjlabelled::set_label(libelles)
    med_i <- rbind(med_i,med_i2)
  }
  info = file.info(paste0(path,"/",finess,".",annee,".",mois,".mchl"))
  if (info$size >0 & !is.na(info$size)){
    med_i3<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".mchl"),
                            readr::fwf_widths(af,an), col_types =at, na=character(), ...) 
    synthese_import <- dplyr::bind_rows(synthese_import, readr::problems(med_i3))
    
    med_i3 <- med_i3 %>%
      dplyr::mutate(NBADM = NBADM/1000,
                    PRIX =  PRIX /1000) %>% sjlabelled::set_label(libelles)
    med_i <- rbind(med_i, med_i3)
  }
  if (tolower_names){
    names(med_i) <- tolower(names(med_i))
  }
  
  attr(med_i,"problems") <- synthese_import
  return(med_i)
}

#' ~ HAD - Import des erreurs Leg
#'
#' Import de la liste d'erreurs de génération Paprica
#'
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param reshape booleen TRUE/FALSE : la donnee doit-elle etre restructuree ? une ligne = une erreur, sinon, une ligne = un sejour. par defaut a F
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#'
#' @return Une table (data.frame, tbl_df) contenant les erreurs Out.
#'
#' @examples
#' \dontrun{
#'    ileg_had('750712184',2015,12,'~/Documents/data/had') -> leg15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ileg_had(finess, annee, mois, path, reshape = F, tolower_names = F, ...)
#' @export ileg_had
#' @export
ileg_had <- function(...){
  UseMethod('ileg_had')
}


#' @export
ileg_had.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(ileg_had.default, param2)
}

#' @export
ileg_had.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(ileg_had.default, param2)
}

#' @export
ileg_had.default <- function(finess, annee, mois, path, reshape = F, tolower_names = F, ...){
  
  leg_i <- readr::read_lines(paste0(path,"/",finess,".",annee,".",mois,".leg"))
  
  extz <- function(x,pat){unlist(lapply(stringr::str_extract_all(x,pat),toString) )}
  
  u <- stringr::str_split(leg_i, "\\;", simplify = T)
  leg_i1 <- dplyr::data_frame(FINESS    = u[,1] %>% as.character(),
                              MOIS      = u[,2] %>% as.character(),
                              ANNEE     = u[,3] %>% as.character(),
                              NOSEQSEJ  = u[,4] %>% as.character(),
                              NOSEQ     = u[,5] %>% as.character(),
                              NOSOUSSEQ = u[,6] %>% as.character(),
                              NBERR     = u[,7] %>% as.integer())
  
  leg_i1 <- as.data.frame(lapply(leg_i1, rep, leg_i1$NBERR), stringsAsFactors = F)
  legs <- u[,8:ncol(u)]
  legs<- legs[legs != ""] 
  leg_i1 <- dplyr::bind_cols(leg_i1, data.frame(EG = as.character(legs), stringsAsFactors = F))
  
  if (reshape==T){
    if (tolower_names){
      names(leg_i) <- tolower(names(leg_i))
    }
    return(leg_i1)
  }
  
  leg_i1 %>% 
    dplyr::group_by(FINESS, MOIS, ANNEE, NOSEQSEJ, NOSEQ, NOSOUSSEQ, NBERR) %>%
    dplyr::summarise(EG = paste(EG, collapse = ", ")) -> leg_i1
  if (tolower_names){
    names(leg_i) <- tolower(names(leg_i))
  }
  return(dplyr::ungroup(leg_i1))
}

##############################################
####################### SSR ##################
##############################################

#' ~ SSR - Import des RHA
#'
#' Import des RHA
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du fichier Out de GENRHA a integrer
#' @param annee Annee de la periode (du fichier Out)
#' @param mois Mois de la periode (du fichier Out)
#' @param path Chemin d'acces au fichier .rha
#' @param lib Attribution de libelles aux colonnes
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max=10e3} pour lire les 1000 premieres lignes
#'
#' @examples
#' \dontrun{
#'    irha('750712184',2015,12,'pathpath/') -> rha15
#' }
#' @author G. Pressiat
#' @seealso \code{\link{iano_ssr}}, \code{\link{ileg_ssr}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irha(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export irha
#' @export
irha <- function(...){
  UseMethod('irha')
}


#' @export
irha.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irha.default, param2)
}

#' @export
irha.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irha.default, param2)
}

#' @export
irha.default <- function(finess, annee, mois, path, lib=T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  #op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'ssr', table == 'rha', an == substr(as.character(annee),3,4))
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
  suppressWarnings(rha_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rha"),
                                            readr::fwf_widths(af,an), col_types = at , na=character(), ...)) 
  
  readr::problems(rha_i) -> synthese_import
  
  rha_i <- rha_i %>%
    dplyr::mutate(FPPC = stringr::str_trim(FPPC),
                  MMP = stringr::str_trim(MMP),
                  AE = stringr::str_trim(AE))
  
  # if (annee > 2016){
  #   rha_i <- rha_i %>% dplyr::mutate(RR = as.numeric(RR) / 100)
  # }
  # 
  if (annee >  2014){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DELAI  = stringr::str_sub(ccam,1,4),
                    CDCCAM = stringr::str_sub(ccam,5,11),
                    DESCRI = stringr::str_sub(ccam, 12,13) %>% stringr::str_trim(),
                    PHASE  = stringr::str_sub(ccam,14,14),
                    ACT    = stringr::str_sub(ccam,15,15),
                    EXTDOC = stringr::str_sub(ccam,16,16),
                    NBEXEC = stringr::str_sub(ccam,17,18),
                    INDVAL = stringr::str_sub(ccam,19,19)
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
                    NBPATIND    = stringr::str_sub(csarr,19,19),
                    NBEXEC      = stringr::str_sub(csarr,20,21),
                    INDVAL      = stringr::str_sub(csarr,22,22),
                    DELAI       = stringr::str_sub(csarr,23,26),
                    NBPATREEL   = stringr::str_sub(csarr,27,28),
                    NBINT       = stringr::str_sub(csarr,29,30),
                    EXTDOCcsarr = stringr::str_sub(csarr,31,32)
      ) %>% dplyr::select(-csarr)
    }
    
    zad <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,6*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,6}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,6*NBDA+1,6*NBDA + 32*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,32}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,6*NBDA+1+32*NBCSARR,6*NBDA + 32*NBCSARR + 19*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,19}")
      )
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='DA') %>% dplyr::select(-NBDA) %>%
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CSARR') %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    
    if (lib == T){
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CCAM)","Diagnostic associé",
                   "Code CSARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Nb réel de patients", "Nb d'intervenants","Extension documentaire CSARR", "Code CCAM", "Partie descriptive","Phase CCAM",
                   "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  if (annee == 2014){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DELAI  = stringr::str_sub(ccam,1 , 4),
                    CDCCAM = stringr::str_sub(ccam,5 ,11),
                    PHASE  = stringr::str_sub(ccam,12,12),
                    ACT    = stringr::str_sub(ccam,13,13),
                    EXTDOC = stringr::str_sub(ccam,14,14),
                    NBEXEC = stringr::str_sub(ccam,15,16),
                    INDVAL = stringr::str_sub(ccam,17,17)
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
                    NBPATIND    = stringr::str_sub(csarr,19,19),
                    NBEXEC      = stringr::str_sub(csarr,20,21),
                    INDVAL      = stringr::str_sub(csarr,22,22),
                    DELAI       = stringr::str_sub(csarr,23,26),
                    NBPATREEL   = stringr::str_sub(csarr,27,28),
                    NBINT       = stringr::str_sub(csarr,29,30),
                    EXTDOCcsarr = stringr::str_sub(csarr,31,32)
      ) %>% dplyr::select(-csarr)
    }
    
    zad <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,6*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,6}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,6*NBDA+1,6*NBDA + 32*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,32}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,6*NBDA+1+32*NBCSARR,6*NBDA + 32*NBCSARR + 17*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,17}")
      )
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='DA') %>% dplyr::select(-NBDA) %>%
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CSARR') %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    if (lib == T){
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CCAM)","Diagnostic associé",
                   "Code CSARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Nb réel de patients", "Nb d'intervenants","Extension documentaire CSARR", "Code CCAM", "Phase CCAM",
                   "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  if (annee == 2013){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DELAI  = stringr::str_sub(ccam,1 , 4),
                    CDCCAM = stringr::str_sub(ccam,5 ,11),
                    PHASE  = stringr::str_sub(ccam,12,12),
                    ACT    = stringr::str_sub(ccam,13,13),
                    EXTDOC = stringr::str_sub(ccam,14,14),
                    NBEXEC = stringr::str_sub(ccam,15,16),
                    INDVAL = stringr::str_sub(ccam,17,17)
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
                    NBPATIND    = stringr::str_sub(csarr,19,19),
                    NBEXEC      = stringr::str_sub(csarr,20,21),
                    INDVAL      = stringr::str_sub(csarr,22,22),
                    DELAI       = stringr::str_sub(csarr,23,26)) %>%
        dplyr::select(-csarr)
    }
    
    zad <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,ZAD, NBDA, NBCSARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,6*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,6}"),
        
        csarr = ifelse(NBCSARR>0,stringr::str_sub(ZAD,6*NBDA+1,6*NBDA + 26*NBCSARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,26}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,6*NBDA+1+26*NBCSARR,6*NBDA + 26*NBCSARR + 17*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,17}")
      )
    
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='DA')%>% dplyr::select(-NBDA) %>%
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    
    if (lib == T){
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CDARR / CCAM)","Diagnostic associé",
                   "Code CSARR","Code CDARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Code CCAM", "Phase CCAM", "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  if (annee == 2012){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DELAI  = stringr::str_sub(ccam,1 , 4),
                    CDCCAM = stringr::str_sub(ccam,5 ,11),
                    PHASE  = stringr::str_sub(ccam,12,12),
                    ACT    = stringr::str_sub(ccam,13,13),
                    EXTDOC = stringr::str_sub(ccam,14,14),
                    NBEXEC = stringr::str_sub(ccam,15,16),
                    INDVAL = stringr::str_sub(ccam,17,17)
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
                    NBPATIND    = stringr::str_sub(csarr,19,19),
                    NBEXEC      = stringr::str_sub(csarr,20,21),
                    INDVAL      = stringr::str_sub(csarr,22,22),
                    DELAI       = stringr::str_sub(csarr,23,26)) %>%
        dplyr::select(-csarr)
    }
    
    zad <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,ZAD, NBDA, NBCDARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,6*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,6}"),
        
        csarr = ifelse(NBCDARR>0,stringr::str_sub(ZAD,6*NBDA+1,6*NBDA + 26*NBCDARR),""),
        lcsarr = stringr::str_extract_all(csarr, ".{1,26}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,6*NBDA+1+26*NBCDARR,6*NBDA + 26*NBCDARR + 17*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,17}")
      )
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='DA') %>% dplyr::select(-NBDA) %>%
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCDARR)
    df <- as.data.frame(lapply(df, rep, df$NBCDARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) )%>% dplyr::tbl_df() %>% dplyr::select(-NBCDARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <- dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    
    if (lib == T){
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CDARR / CCAM)","Diagnostic associé",
                   "Code CSARR","Code CDARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Code CCAM", "Phase CCAM", "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  if (annee == 2011){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DELAI  = stringr::str_sub(ccam,1 , 4),
                    CDCCAM = stringr::str_sub(ccam,5 ,11),
                    PHASE  = stringr::str_sub(ccam,12,12),
                    ACT    = stringr::str_sub(ccam,13,13),
                    EXTDOC = stringr::str_sub(ccam,14,14),
                    NBEXEC = stringr::str_sub(ccam,15,16),
                    INDVAL = stringr::str_sub(ccam,17,17)
      ) %>% dplyr::select(-ccam)
    }
    
    fzdarr <- function(cdarr){
      dplyr::mutate(cdarr,
                    CDINTER       = stringr::str_sub(cdarr,1,2),
                    CDARR       = stringr::str_sub(cdarr,3,6),
                    CODE        = "CDARR",
                    NBEXEC      = stringr::str_sub(cdarr,7,8),
                    INDVAL      = stringr::str_sub(cdarr,9,9)) %>%
        dplyr::select(-cdarr)
    }
    
    zad <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,ZAD, NBDA, NBCDARR, NBCCAM) %>%
      dplyr::mutate(
        
        da = ifelse(NBDA>0,stringr::str_sub(ZAD,1,6*NBDA),""),
        lda = stringr::str_extract_all(da,".{1,6}"),
        
        cdarr = ifelse(NBCDARR>0,stringr::str_sub(ZAD,6*NBDA+1,6*NBDA + 9*NBCDARR),""),
        lcdarr = stringr::str_extract_all(cdarr, ".{1,9}"),
        
        ccam = ifelse(NBCCAM>0,stringr::str_sub(ZAD,6*NBDA+1+9*NBCDARR, 6*NBDA + 9*NBCDARR + 17*NBCCAM),""),
        lccam = stringr::str_extract_all(ccam, ".{1,17}")
      )
    da <- purrr::flatten_chr(zad$lda)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = da, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='DA') %>% dplyr::select(-NBDA) %>%
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA) %>% dplyr::mutate(DA = stringr::str_trim(DA))
    
    cdarr <- purrr::flatten_chr(zad$lcdarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCDARR)
    df <- as.data.frame(lapply(df, rep, df$NBCDARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    cdarr <- dplyr::bind_cols(df,data.frame(cdarr = cdarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CDARR') %>% dplyr::select(-NBCDARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzdarr(cdarr), fzacte(ccam))
    
    if (lib == T){
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CDARR / CCAM)","Diagnostic associé",
                   "Code de l'intervenant", "Code CDARR", "Nb de réalisations","Acte compatible avec la semaine",
                   "Délai depuis la date d'entrée dans l'UM",
                   "Code CCAM", "Phase CCAM", "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjlabelled::set_label(labelacdi)
    }
  }
  
  
  acdi[is.na(acdi)] <- ""
  acdi$NBEXEC <- acdi$NBEXEC  %>%  as.numeric()
  acdi$DELAI <- acdi$DELAI  %>%  as.numeric()
  if (annee>2014){acdi$NBPATREEL <- acdi$NBPATREEL  %>%  as.numeric()}
  Fillers <- names(rha_i)
  Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
  
  rha_i <- rha_i[,!(names(rha_i) %in% Fillers)]
  
  rha_i <- rha_i   %>% dplyr::select(-ZAD)
  if (lib == T){
   rha_i <- rha_i %>% sjlabelled::set_label(libelles[!is.na(libelles)])
  }
  if (tolower_names){
    names(rha_i) <- tolower(names(rha_i))
    names(acdi) <- tolower(names(acdi))
  }
  
  rha_1 = list(rha = rha_i, acdi = acdi)
  attr(rha_1,"problems") <- synthese_import
  deux <- Sys.time()
  #cat("Données RHA importées en : ", deux-un, " secondes\n")
  return(rha_1)
}

#' ~ SSR - Import des Anohosp
#'
#' Import du fichier Ano Out
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Genrha) :
#' \emph{finess.annee.moisc.ano}
#'
#' \strong{750712184.2016.2.ano}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... paramètres supplementaires à passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes, \code{progress = F, skip =...}
#'
#' @return Une table (data.frame, tbl_df) contenant les données Anohosp SSR du Out.
#'
#' @examples
#' \dontrun{
#'    anoh <- iano_ssr('750712184',2015,12,"~/Documents/data/ssr")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irha}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' 
#' @usage iano_ssr(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export iano_ssr
#' @export
iano_ssr <- function(...){
  UseMethod('iano_ssr')
}



#' @export
iano_ssr.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iano_ssr.default, param2)
}

#' @export
iano_ssr.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iano_ssr.default, param2)
}

#' @export
iano_ssr.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'ssr', table == 'rha_ano', an == substr(as.character(annee),3,4))
  
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
  
  ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                           readr::fwf_widths(af,an), col_types = at , na=character(), ...) 
  readr::problems(ano_i) -> synthese_import
  
  if (annee>2012){
    
    ano_i <- ano_i %>%
      dplyr::mutate(DTSOR   = lubridate::dmy(DTSOR),
                    DTENT   = lubridate::dmy(DTENT),
                    cok = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                             (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') +
                             (CRCDNAI=='0') + (CRCSEXE=='0')==9),
                    MTFACTMO = MTFACTMO/100,
                    MTFORJOU = MTFORJOU/100,
                    MTFACTOT = MTFACTOT/100,
                    MTBASERM = MTBASERM/100,
                    MTRMBAMC = MTRMBAMC/100,
                    TAUXRM   = TAUXRM  /100)
  }
  if (annee<2013){

    
    ano_i <- ano_i %>%
      dplyr::mutate(DTSOR   = lubridate::dmy(DTSOR),
                    DTENT   = lubridate::dmy(DTENT),
                    cok = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                             (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') == 7),
                    MTFACTMO = MTFACTMO/100,
                    MTFORJOU = MTFORJOU/100,
                    MTFACTOT = MTFACTOT/100,
                    MTBASERM = MTBASERM/100,
                    TAUXRM   = TAUXRM  /100)
  }
  
  if (lib == T){
  ano_i <- ano_i %>% sjlabelled::set_label(c(libelles,'Chaînage Ok'))
  ano_i <- ano_i %>% dplyr::select(-dplyr::starts_with("FIL"))
  }
  
  if (tolower_names){
    names(ano_i) <- tolower(names(ano_i))
  }
  

  attr(ano_i,"problems") <- synthese_import
  return(ano_i)
}

#' ~ SSR - Import des SSRHA
#'
#' Import du fichier SHA
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Genrha) :
#' \emph{finess.annee.moisc.sha}
#'
#' \strong{750712184.2016.2.sha}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles a la table : T
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données SHA, et a partir de 2017 une liste de deux tables (sha et gme)
#'
#' @examples
#' \dontrun{
#'    sha <- issrha('750712184',2015,12,"~/Documents/data/ssr")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irha}}, \code{\link{ileg_ssr}}, \code{\link{iano_ssr}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage issrha(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export issrha
#' @export
issrha <- function(...){
  UseMethod('issrha')
}



#' @export
issrha.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(issrha.default, param2)
}

#' @export
issrha.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(issrha.default, param2)
}

#' @export
issrha.default <- function(finess, annee,mois, path, lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'ssr', table == 'ssrha', an == substr(as.character(annee),3,4))
  
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
  

  ssrha_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".sha"),
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...) 
  readr::problems(ssrha_i) -> synthese_import
  
  if (lib == T){
    ssrha_i <- ssrha_i %>%
      sjlabelled::set_label(libelles)
  }

  if (annee > 2016){
    zac  <- ssrha_i %>% dplyr::select(NBZGP, ZGP)
    fixe <- ssrha_i %>% dplyr::select(NOFINESS, NOSEQSEJ, NBZGP)
    zac1 <- purrr::flatten_chr(stringr::str_extract_all(zac$ZGP, '.{1,13}'))
    fixe <- as.data.frame(lapply(fixe, rep, fixe$NBZGP), stringsAsFactors = F)
    gp <- data.frame(zac1 = as.character(zac1), stringsAsFactors = F)
    gp <- dplyr::mutate(gp, 
                        GME = stringr::str_sub(zac1, 1, 6),
                        GMT = stringr::str_sub(zac1, 7, 10),
                        NJ = stringr::str_sub(zac1, 11, 13) %>% as.integer()) %>%
      dplyr::select(-zac1)
    
    if (lib == T){
    gp <- sjlabelled::set_label(gp, c("GME", "GMT", "Nombre de jours de présence"))
    }
    
    gp <- dplyr::as_tibble(dplyr::bind_cols(fixe, gp))
    
    if (tolower_names){
      names(ssrha_i) <- tolower(names(ssrha_i))
      names(gp) <- tolower(names(gp))
    }
    ssrha_1 <- list(ssrha = ssrha_i, gme = gp)
    attr(ssrha_1,"problems") <- synthese_import
    return(ssrha_1)
  }
  if (tolower_names){
    names(ssrha_i) <- tolower(names(ssrha_i))
  }
  
  attr(ssrha_i,"problems") <- synthese_import
  return(ssrha_i)
}

#' ~ SSR - Import des erreurs Leg
#'
#' Import de la liste d'erreurs de génération Genrha
#'
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param reshape booleen TRUE/FALSE : la donnee doit-elle etre restructuree ? une ligne = une erreur, sinon, une ligne = un sejour. par defaut a F
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#'
#' @return Une table (data.frame, tbl_df) contenant les erreurs Out.
#'
#' @examples
#' \dontrun{
#'    ileg_had('750712184',2015,12,'~/Documents/data/ssr') -> leg15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irha}}, \code{\link{issrha}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ileg_ssr(finess, annee, mois, path, reshape = F, tolower_names = F, ...)
#' @export ileg_ssr
#' @export
ileg_ssr <- function(...){
  UseMethod('ileg_ssr')
}


#' @export
ileg_ssr.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(ileg_ssr.default, param2)
}

#' @export
ileg_ssr.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(ileg_ssr.default, param2)
}

#' @export
ileg_ssr.default <- function(finess, annee, mois, path, reshape = F, tolower_names = F, ...){
  
  leg_i <- readr::read_lines(paste0(path,"/",finess,".",annee,".",mois,".leg"))
  
  extz <- function(x,pat){unlist(lapply(stringr::str_extract_all(x,pat),toString) )}
  
  u <- stringr::str_split(leg_i, "\\;", simplify = T)
  leg_i1 <- dplyr::data_frame(FINESS    = u[,1] %>% as.character(),
                              MOIS      = u[,2] %>% as.character(),
                              ANNEE     = u[,3] %>% as.character(),
                              NOSEQSEJ  = u[,4] %>% as.character(),
                              NOSEQRHS  = u[,5] %>% as.character(),
                              NBERR     = u[,6] %>% as.integer())
  
  leg_i1 <- as.data.frame(lapply(leg_i1, rep, leg_i1$NBERR), stringsAsFactors = F)
  legs <- u[,7:ncol(u)]
  legs<- legs[legs != ""] 
  leg_i1 <- dplyr::bind_cols(leg_i1, data.frame(EG = as.character(legs), stringsAsFactors = F))
  
  if (reshape==T){
    if (tolower_names){
      names(leg_i1) <- tolower(names(leg_i1))
    }
    return(leg_i1)
    
  }
  
  leg_i1 %>% 
    dplyr::group_by(FINESS, MOIS, ANNEE, NOSEQSEJ, NOSEQRHS, NBERR) %>%
    dplyr::summarise(EG = paste(EG, collapse = ", ")) -> leg_i1
  if (tolower_names){
    names(leg_i1) <- tolower(names(leg_i1))
  }
  return(dplyr::ungroup(leg_i1))
}

#' ~ SSR - Import des Med
#'
#' Imports du fichier Med Out
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Genrha) :
#' \emph{finess.annee.moisc.med}
#'
#' \strong{750712184.2017.2.med}
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
#' @return Une table (data.frame, tbl_df) contenant les données médicaments SSR du Out.
#'
#' @examples
#' \dontrun{
#'    meds <- imed_ssr('750712184',2015,12,"~/Documents/data/ssr")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}}
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage imed_ssr(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export imed_ssr
#' @export
imed_ssr <- function(...){
  UseMethod('imed_ssr')
}


#' @export
imed_ssr.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(imed_ssr.default, param2)
}

#' @export
imed_ssr.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(imed_ssr.default, param2)
}

#' @export
imed_ssr.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'ssr', table == 'rha_med', an == substr(as.character(annee),3,4))
  
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
  info = file.info(paste0(path,"/",finess,".",annee,".",mois,".med"))
  if (info$size >0 & !is.na(info$size)){
  med_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".med"),
                           readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
    dplyr::mutate(NBADM = NBADM/1000,
                  PRIX  = PRIX /1000) %>% sjlabelled::set_label(libelles)
  }
  else {
    med_i <- dplyr::tbl_df(data.frame())
    }
  info = file.info(paste0(path,"/",finess,".",annee,".",mois,".medatu"))
  if (info$size >0 & !is.na(info$size)){
    med_i2<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".medatu"),
                            readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(NBADM = NBADM/1000,
                    PRIX =  PRIX /1000) %>% sjlabelled::set_label(libelles)
    med_i <- rbind(med_i,med_i2)
  }
  
  Fillers <- names(med_i)
  Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
  med_i <- med_i[,!(names(med_i) %in% Fillers)]
  if (tolower_names){
    names(med_i) <- tolower(names(med_i))
  }
  return(med_i)
}

#' ~ SSR - Import des donnees UM du Out
#'
#' Imports du fichier IUM SSR
#'
#' Formats depuis 2013 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires à passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les informations structures du Out.
#'
#' @examples
#' \dontrun{
#'    um <- iium_ssr('750712184',2015,12,"~/Documents/data/ssr")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iium_ssr(finess, annee, mois, path, lib = T, ...)
#' @export iium_ssr
#' @export
iium_ssr <- function(...){
  UseMethod('iium_ssr')
}


#' @export
iium_ssr.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iium_ssr.default, param2)
}

#' @export
iium_ssr.list <- function(l , ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iium_ssr.default, param2)
}

#' @export
iium_ssr.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2013|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'ssr', table == 'rha_um', an == substr(as.character(annee),3,4))
  
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
  
  ium_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ium"),
                         readr::fwf_widths(af,an), col_types =at, na=character(), ...)
  readr::problems(ium_i) -> synthese_import
  
  if (lib==T){
    v <- libelles
    ium_i <- ium_i  %>%  sjlabelled::set_label(v)
  }
  if (tolower_names){
    names(ium_i) <- tolower(names(ium_i))
  }
  
  attr(ium_i,"problems") <- synthese_import
  return(ium_i)
}


##############################################
####################### PSY ##################
##############################################

#' ~ PSY - Import des RPSA
#'
#' Import du fichier RPSA
#'
#' Formats depuis 2012 pris en charge
#' Structure du nom du fichier attendu (sortie de Pivoine) :
#' \emph{finess.annee.moisc.rpsa}
#'
#' \strong{750712184.2016.2.rpsa}
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
#' @return Une table (data.frame, tbl_df) contenant les données RPSA.
#'
#' @examples
#' \dontrun{
#'    rpsa <- irpsa('750712184',2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{ir3a}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irpsa(finess, annee, mois, path, lib = T, tolower_names = F, ...) 
#' @export irpsa
#' @export
irpsa <- function(...){
  UseMethod('irpsa')
}



#' @export
irpsa.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irpsa.default, param2)
}

#' @export
irpsa.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irpsa.default, param2)
}

#' @export
irpsa.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2012|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'psy', table == 'rpsa', an == substr(as.character(annee),3,4))
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
  
  suppressWarnings(rpsa_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rpsa"),
                                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)) 
  
  readr::problems(rpsa_i) -> synthese_import
  
  rpsa_i <- rpsa_i %>%
    dplyr::mutate(DP = stringr::str_trim(DP))
  
  
  if (annee < 2017){
    zad <- rpsa_i %>% dplyr::select(NOSEQSEJ, NOSEQ,NBDA,ZAD) %>%  dplyr::mutate(da  = ifelse(NBDA>0,ZAD,""),
                                                                                 lda = stringr::str_extract_all(da, '.{1,6}'))
    
  da <- purrr::flatten_chr(zad$lda) %>% stringr::str_trim()
  
  df <- zad %>% dplyr::select(NOSEQSEJ, NOSEQ,NBDA)
  df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
  da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBDA)
  
  
  rpsa_i$ZAD[is.na(rpsa_i$ZAD)] <- ""
  rpsa_i <- rpsa_i %>% dplyr::mutate(das = extz(ZAD, ".{1,6}")) %>% dplyr::select(-ZAD)
  
  if (lib == T){
  rpsa_i <- rpsa_i %>% sjlabelled::set_label(c(libelles[-length(libelles)], "Stream DA ou facteurs associés"))
  
  da <- da %>% sjlabelled::set_label(c('N° séquentiel de séjour','N° séquentiel de séquence au sein du séjour',
                                   'Diagnostics et facteurs associés'))
  }
  
  if (tolower_names){
    names(rpsa_i) <- tolower(names(rpsa_i))
    names(da) <- tolower(names(da))
  }
  rpsa_1 = list(rpsa = rpsa_i, das = da)
  }
  
  if (annee > 2016){
    rpsa_i <- rpsa_i %>%  
      dplyr::mutate(da  = ifelse(NBDA>0,stringr::str_sub(ZAD,1, NBDA*6),""),
                    lda = stringr::str_extract_all(da, '.{1,6}'),
                    actes = ifelse(NBZA>0,stringr::str_sub(ZAD,NBDA*6+1,1+ NBDA*6 + NBZA*17),""),
                    lactes = stringr::str_extract_all(actes, '.{1,17}'))
    
    zad <- rpsa_i
    da <- purrr::flatten_chr(zad$lda) %>% stringr::str_trim()
    
    df <- zad %>% dplyr::select(NOSEQSEJ, NOSEQ,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBDA)
    
    actes <- purrr::flatten_chr(zad$lactes)
    
    df <- zad %>% dplyr::select(NOSEQSEJ, NOSEQ,NBZA)
    df <- as.data.frame(lapply(df, rep, df$NBZA), stringsAsFactors = F) %>% dplyr::tbl_df()
    actes <- dplyr::bind_cols(df,data.frame(ACTES = actes, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBZA)
    
    fzacte <- function(actes){
      dplyr::mutate(actes,
                    DELAI  = stringr::str_sub(ACTES,1,3) %>% as.integer(),
                    CDCCAM = stringr::str_sub(ACTES,4,10),
                    DESCRI = stringr::str_sub(ACTES,11,12) %>% stringr::str_trim(),
                    PHASE  = stringr::str_sub(ACTES,13,13),
                    ACT    = stringr::str_sub(ACTES,14,14),
                    EXTDOC = stringr::str_sub(ACTES,15,15),
                    NBEXEC = stringr::str_sub(ACTES,16,17) %>% as.integer()
      ) %>% dplyr::select(-ACTES)
    }
    
    fzacte(actes) -> actes
    rpsa_i$ZAD[is.na(rpsa_i$ZAD)] <- ""
    rpsa_i <- rpsa_i %>% dplyr::mutate(das = extz(da, ".{1,6}"), 
                                       actes = extz(actes, "[A-Z]{4}[0-9]{3}")) %>% dplyr::select(-ZAD, -da, -lactes, -lda)
    libelles[is.na(libelles)] <- ""
    
    if (lib == T){
    rpsa_i <- rpsa_i %>% sjlabelled::set_label(c(libelles[-length(libelles)], "Stream actes","Stream DA ou facteurs associés"))
    
    da <- da %>% sjlabelled::set_label(c('N° séquentiel de séjour','N° séquentiel de séquence au sein du séjour',
                                     'Diagnostics et facteurs associés'))
    
    
    actes <- actes %>% sjlabelled::set_label(c('N° séquentiel de séjour','N° séquentiel de séquence au sein du séjour',
                                     "Délai depuis la date d'entrée", "Code CCAM",
                                     "Extension PMSI", "Code de la phase", "Code de l'activité", "Extension documentaire", "Nombre de réalisations"))
    }
    if (tolower_names){
      names(rpsa_i) <- tolower(names(rpsa_i))
      names(da) <- tolower(names(da))
      names(actes) <- tolower(names(actes))
    }
    rpsa_1 = list(rpsa = rpsa_i, das = da, actes = actes)
  }
  
  attr(rpsa_1,"problems") <- synthese_import
  return(rpsa_1)
}


#' ~ PSY - Import des R3A
#'
#' Import du fichier R3A
#'
#' Formats depuis 2012 pris en charge
#' Structure du nom du fichier attendu (sortie de Pivoine) :
#' \emph{finess.annee.moisc.r3a}
#'
#' \strong{750712184.2016.3.r3a}
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
#' @return Une table (data.frame, tbl_df) contenant les données R3A.
#'
#' @examples
#' \dontrun{
#'    r3a <- ir3a('750712184',2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irpsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ir3a(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export ir3a
#' @export
ir3a <- function(...){
  UseMethod('ir3a')
}



#' @export
ir3a.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(ir3a.default, param2)
}

#' @export
ir3a.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(ir3a.default, param2)
}

#' @export
ir3a.default <- function(finess, annee, mois, path, lib = T, tolower_names = F, ...){
  if (annee<2012|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'psy', table == 'r3a', an == substr(as.character(annee),3,4))
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
  
  suppressWarnings(r3a_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".r3a"),
                                            readr::fwf_widths(af,an), col_types = at , na=character(), ...)) 
  
  readr::problems(r3a_i) -> synthese_import
  
  r3a_i <- r3a_i %>%
    dplyr::mutate(DP = stringr::str_trim(DP))
  
  zad <- r3a_i %>% dplyr::select(NOSEQSEJ,NOORDR,NBDA,ZAD) %>%  dplyr::mutate(da  = ifelse(NBDA>0,ZAD,""),
                                                                              lda = stringr::str_extract_all(da, '.{1,6}'))
  
  da <- purrr::flatten_chr(zad$lda)
  
  df <- zad %>% dplyr::select(NOSEQSEJ,NOORDR,NBDA)
  df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
  da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBDA)
  
  
  r3a_i$ZAD[is.na(r3a_i$ZAD)] <- ""
  r3a_i <- r3a_i %>% dplyr::mutate(das = extz(ZAD, ".{1,6}")) %>% dplyr::select(-ZAD)
  
  if (lib == T){
  r3a_i <- r3a_i %>% sjlabelled::set_label(c(libelles[-length(libelles)], "Stream DA ou facteurs associés"))
  
  da <- da %>% sjlabelled::set_label(c('N° séquentiel de séjour',"N° d'ordre", 'Diagnostics et facteurs associés'))
  }
  
  if (tolower_names){
    names(r3a_i) <- tolower(names(r3a_i))
    names(da) <- tolower(names(da))
  }
  
  r3a_1 = list(r3a = r3a_i, das = da)
  
  attr(r3a_1,"problems") <- synthese_import
  return(r3a_1)
}

#' ~ PSY - Import des Anohosp
#'
#' Import du fichier Ano Out
#'
#' Formats depuis 2012 pris en charge
#' Structure du nom du fichier attendu (sortie de Genrha) :
#' \emph{finess.annee.moisc.ano}
#'
#' \strong{750712184.2016.2.ano}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données Anohosp SSR du Out.
#'
#' @examples
#' \dontrun{
#'    anoh <- iano_psy('750712184',2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irpsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iano_psy(finess, annee, mois, path, lib = T, tolower_names = F, ...)
#' @export iano_psy
#' @export
iano_psy <- function(...){
  UseMethod('iano_psy')
}



#' @export
iano_psy.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iano_psy.default, param2)
}

#' @export
iano_psy.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iano_psy.default, param2)
}

#' @export
iano_psy.default <- function(finess, annee, mois, path, lib=T, tolower_names = F, ...){
  if (annee<2012|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  format <- pmeasyr::formats %>% dplyr::filter(champ == 'psy', table == 'rpsa_ano', an == substr(as.character(annee),3,4))
  
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
  
  if (annee<=2012){
    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  
    readr::problems(ano_i) -> synthese_import
    ano_i <- ano_i %>%
      dplyr::mutate(DTSOR   = lubridate::dmy(DTSOR),
                    DTENT    = lubridate::dmy(DTENT),
                    cok = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                             (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') ==7),
                    MTFACTMO = MTFACTMO/100,
                    MTFORJOU = MTFORJOU/100,
                    MTFACTOT = MTFACTOT/100,
                    MTBASERM = MTBASERM/100,
                    TAUXRM   = TAUXRM  /100,
                    MTRMBAMC = MTRMBAMC/100)
  }
  if (annee>2012){
    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  
    readr::problems(ano_i) -> synthese_import
    ano_i <- ano_i %>%
      dplyr::mutate(DTSOR   = lubridate::dmy(DTSOR),
                    DTENT    = lubridate::dmy(DTENT),
                    cok = ((CRSECU=='0')+(CRDNAI=='0')+ (CRSEXE=='0') + (CRNODA=='0') +
                             (CRFUSHOSP=='0') + (CRFUSPMSI=='0') + (CRDTENT=='0') +
                             (CRCDNAI=='0') + (CRCSEXE=='0')==9),
                    MTFACTMO = MTFACTMO/100,
                    MTFORJOU = MTFORJOU/100,
                    MTFACTOT = MTFACTOT/100,
                    MTBASERM = MTBASERM/100,
                    MTRMBAMC = MTRMBAMC/100,
                    TAUXRM   = TAUXRM  /100)
  }
  
  if (lib == T){
  ano_i <- ano_i %>% sjlabelled::set_label(c(libelles,'Chaînage Ok'))
  ano_i <- ano_i %>% dplyr::select(-dplyr::starts_with("Fill"))
  }
  
  if (tolower_names){
    names(ano_i) <- tolower(names(ano_i))
  }
  
  attr(ano_i,"problems") <- synthese_import
  return(ano_i)
}


##############################################
####################### DICO #################
##############################################


#' ~ Dico - Dictionnaire des tables
#'
#' Obtenir le dictionnaire d'une table
#'
#'
#' @param table Table dont on veut le dictionnaire de variables
#'
#' @examples
#' \dontrun{
#' # N'importer qu'une ligne du fichier :
#'    irsa('750712184', 2016, 8, '~/path/path', typi= 1, n_max = 1) -> import
#'    dico(import$rsa)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}}, \code{\link{irum}}

#' @export
dico <- function(table){
  dplyr::data_frame(
    nom   = names(table),
    label = sjlabelled::get_label(table),
    type  = sapply(table, class)
  ) %>% sjlabelled::set_label(c("Nom de la variable","Libellé, de la variable", "Type"))
}

##############################################
####################### Tidy #################
##############################################

#' ~ Tidy - Tidy Diagnostics
#'
#' Restructurer les diagnostics
#'
#' On obtient une table contenant tous les diagnostics par séjour, sur le principe suivant :
#' Une variable numérique indique la position des diagnostics
#' - pour les rsa : 1 : DP du rsa, 2 : DR du rsa, 3 : DPUM, 4 : DRUM, 5 : DAS
#' - pour les rum : 1 : DP du rum, 2 : DR du rum, 3 : DAS, 4 : DAD
#' - pour les rha : 1 : MMP du rha, 2 : FPPC du rha, 3 : AE, 4 : DA
#' 
#' @param d Objet S3 resultat de l'import pmeasyr (irsa, irum, irha)
#' @param include booleen : defaut a T; T : restructure l'objet S3 (agglomere dp, dr, das et dad, par exemple)
#'
#' @examples
#' \dontrun{
#' # avec include = T
#' irum('750712184', 2016, 8, '~/path/path', typi = 3) -> d1
#' tdiag(d1) -> d1
#' d1$diags
#' d1$actes
#' d1$dad
#' irsa('750712184', 2016, 8, '~/path/path', typi = 4) -> d1
#' tdiag(d1, include = F) -> alldiag
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}}, \code{\link{irum}}, \code{\link{irha}}

#' @export
tdiag <- function (d,  include = T){
  
  
  if (names(d)[1] == "rsa") {
    if ('DP' %in% names(d$rsa)){
    temp <- d$rsa %>% dplyr::select(CLE_RSA, NSEQRUM = NOSEQRUM, DP, DR) %>%
      sjlabelled::set_label(rep("", 4))
    e <- temp %>% tidyr::gather(position, diag, -CLE_RSA, - NSEQRUM,
                                na.rm = T)
    f <- e %>% dplyr::filter(diag != "")
    g <- d$das  %>% dplyr::select(CLE_RSA,NSEQRUM,diag = DAS) %>% dplyr::mutate(position = "DAS")
    h <- dplyr::bind_rows(f, g)
    temp <- d$rsa_um %>% dplyr::select(CLE_RSA, NSEQRUM, DPUM, DRUM) %>%
      sjlabelled::set_label(rep("", 4))
    e <- temp %>% tidyr::gather(position, diag, -CLE_RSA, - NSEQRUM,
                                na.rm = T)
    f <- e %>% dplyr::filter(diag != "")
    h <- dplyr::bind_rows(h, f) %>% dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "DP", `2` = "DR", `3` = "DPUM", `4` = "DRUM",
                                                                                                         `5` = "DAS"))))
    
    # h <- sjlabelled::remove_all_labels(h)
    if (!is.null(sjlabelled::get_label(d$rsa$NOFINESS))){
    h <- h %>% sjlabelled::set_label(c("Clé rsa", "N° du RUM","1:DP, 2:DR, 3:DPUM, 4:DRUM, 5:DAS",
                                   "Diagnostic"))
    }
    
    if (include == F) {
      return(h)
    }
    else {
      return(list(rsa = d$rsa, rsa_um = d$rsa_um, actes = d$actes, diags = h))
    }
    }
    else if ('dp' %in% names(d$rsa)){
      temp <- d$rsa %>% dplyr::select(cle_rsa, nseqrum = noseqrum, dp, dr) %>%
        sjlabelled::set_label(rep("", 4))
      e <- temp %>% tidyr::gather(position, diag, -cle_rsa, - nseqrum,
                                  na.rm = T)
      f <- e %>% dplyr::filter(diag != "")
      g <- d$das  %>% dplyr::select(cle_rsa,nseqrum,diag = das) %>% dplyr::mutate(position = "das")
      h <- dplyr::bind_rows(f, g)
      temp <- d$rsa_um %>% dplyr::select(cle_rsa, nseqrum, dpum, drum) %>%
        sjlabelled::set_label(rep("", 4))
      e <- temp %>% tidyr::gather(position, diag, -cle_rsa, - nseqrum,
                                  na.rm = T)
      f <- e %>% dplyr::filter(diag != "")
      h <- dplyr::bind_rows(h, f) %>% dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "dp", `2` = "dr", `3` = "dpum", `4` = "drum",
                                                                                                           `5` = "das"))))
      # h <- sjlabelled::remove_all_labels(h)
      
      if (!is.null(sjlabelled::get_label(d$rsa$nofiness))){
      h <- h %>% sjlabelled::set_label(c("Clé rsa", "N° du RUM","1:DP, 2:DR, 3:DPUM, 4:DRUM, 5:DAS",
                                         "Diagnostic"))
      }
      
      if (include == F) {
        return(h)
      }
      else {
        return(list(rsa = d$rsa, rsa_um = d$rsa_um, actes = d$actes, diags = h))
      }
    }
  }
  if (names(d)[1]  == "rum") {
    if ("DP" %in% names(d$rum)){
    temp <- d$rum %>% dplyr::select(NAS,NORUM, DP, DR) %>% sjlabelled::set_label(rep("",4))
    e <- temp %>% tidyr::gather(position, diag, -NAS,- NORUM, na.rm = T)
    f <- e %>% dplyr::filter(diag != "")
    g <- d$das %>% dplyr::rename(diag = DAS) %>% dplyr::mutate(position = "DAS")
    g2 <- d$dad %>% dplyr::rename(diag = DAD) %>% dplyr::mutate(position = "DAD")
    h <- dplyr::bind_rows(list(f, g, g2)) %>%
      dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "DP", `2` = "DR", `3` = "DAS", `4`="DAD"))))
    
    h <- sjlabelled::remove_all_labels(h)
   
     if (!is.null(sjlabelled::get_label(d$rum$NOFINESS))){
    h <- h %>% sjlabelled::set_label(c("N° administratif du séjour", "N° du RUM",
                                   "1:DP, 2:DR, 3:DAS, 4:DAD", "Diagnostic"))
    }
    if (include == F) {
      return(h)
    }
    else {
      return(list(rum = d$rum, actes = d$actes, diags = h))
    }
    }
    else if ("dp" %in% names(d$rum)){
      temp <- d$rum %>% dplyr::select(nas, norum, dp, dr) %>% sjlabelled::set_label(rep("",4))
      e <- temp %>% tidyr::gather(position, diag, -nas,- norum, na.rm = T)
      f <- e %>% dplyr::filter(diag != "")
      g <- d$das %>% dplyr::rename(diag = das) %>% dplyr::mutate(position = "das")
      g2 <- d$dad %>% dplyr::rename(diag = dad) %>% dplyr::mutate(position = "dad")
      h <- dplyr::bind_rows(list(f, g, g2)) %>%
        dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "dp", `2` = "dr", `3` = "das", `4`="dad"))))
      
      h <- sjlabelled::remove_all_labels(h)
     
       if (!is.null(sjlabelled::get_label(d$rum$nofiness))){
      h <- h %>% sjlabelled::set_label(c("N° administratif du séjour", "N° du RUM",
                                         "1:DP, 2:DR, 3:DAS, 4:DAD", "Diagnostic"))}
      if (include == F) {
        return(h)
      }
      else {
        return(list(rum = d$rum, actes = d$actes, diags = h))
      }
    }
  }
  if (names(d)[1] == "rha") {
    if ("MMP" %in% names(d$rha)){
    temp <- d$rha %>% dplyr::select(NOSEQSEJ, NOSEQRHS, MMP, FPPC, AE) %>%
      sjlabelled::set_label(rep("", 5))
    e <- temp %>% tidyr::gather(position, diag, -NOSEQSEJ, - NOSEQRHS,
                                na.rm = T)
    f <- e %>% dplyr::filter(diag != "")
    g <- d$acdi  %>% dplyr::filter(CODE == 'DA') %>% dplyr::select(NOSEQSEJ,NOSEQRHS,diag = DA) %>% dplyr::mutate(position = "DA")
    h <- dplyr::bind_rows(f, g)
    h <- dplyr::bind_rows(h, f) %>% dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "MMP", `2` = "FPPC", `3` = "AE", `4` = "DA"))))
    
    
    
    h <- h %>% sjlabelled::set_label(c("N° séquentiel du séjour", "N° séquentiel du RHS","1:MMP, 2:FPPC, 3:AE, 4:DA",
                                   "Diagnostic"))
    
    
    if (include == F) {
      return(h)
    }
    else {
      return(list(rha = d$rha, acdi = d$acdi, diags = h))
    }
  }
    else if ("mmp" %in% names(d$rha)){
      temp <- d$rha %>% dplyr::select(noseqsej, noseqrhs, mmp, fppc, ae) %>%
        sjlabelled::set_label(rep("", 5))
      e <- temp %>% tidyr::gather(position, diag, -noseqsej, - noseqrhs,
                                  na.rm = T)
      f <- e %>% dplyr::filter(diag != "")
      g <- d$acdi  %>% dplyr::filter(code == 'DA') %>% dplyr::select(noseqsej,noseqrhs,diag = da) %>% dplyr::mutate(position = "da")
      h <- dplyr::bind_rows(f, g)
      h <- dplyr::bind_rows(h, f) %>% dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "mmp", `2` = "fppc", `3` = "ae", `4` = "da"))))
      
      
      h <- h %>% sjlabelled::set_label(c("N° séquentiel du séjour", "N° séquentiel du RHS","1:MMP, 2:FPPC, 3:AE, 4:DA",
                                         "Diagnostic"))
      
      if (include == F) {
        return(h)
      }
      else {
        return(list(rha = d$rha, acdi = d$acdi, diags = h))
      }
    }
    
    }
  
}


##############################################
####################### RSF ##################
##############################################
#' ~ RSF - Import des Rafael
#'
#' Import des Rafael et des Rafael reprises
#'
#' Formats depuis 2012 pour les rsfa
#' Formats depuis 2014 pour les rsfa-maj (reprise 2013)
#' 
#' @return Une classe S3 contenant les tables (data.frame, tbl_df ou tbl) importées  (rafaels)
#'
#' @examples
#' \dontrun{
#'    irafael('750712184',2015,12,'~/Documents/data/rsf') -> rsfa15
#'    irafael('750712184',2015,12,'~/Documents/data/rsf', lister = 'C', lamda = T) -> rsfa14_lamda
#' }
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par défaut a TRUE ; necessite le package \code{sjlabelled}
#' @param stat avec stat = T, un tableau synthetise le nombre de lignes par type de rafael
#' @param lister Liste des types d'enregistrements a importer
#' @param lamda a TRUE, importe les fichiers \code{rsfa-maj} de reprise de l'annee passee
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... Autres parametres a specifier \code{n_max = 1e3}, ...
#' @author G. Pressiat
#'
#' @seealso \code{\link{iano_rafael}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irafael(finess, annee, mois, path, lib = T, tolower_names = F, 
#' stat = T, lister = c("A", "B",
#' "C", "H", "L", "M", "P"), lamda = F, ...)
#' @export irafael
#' @export
irafael <- function(...){
  UseMethod('irafael')
}



#' @export
irafael.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irafael.default, param2)
}

#' @export
irafael.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irafael.default, param2)
}

#' @export
irafael.default <- function(finess, annee, mois, path, lib = T, stat = T, 
                            lister = c('A', 'B', 'C', 'H', 'L', 'M',  'P'), 
                            lamda = F, tolower_names = F, ...){
  if (annee<2011|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  if (lamda == F){
    # cat(paste("Import des RSFA / Rafael", annee, paste0("M",mois),"\n"))
    # cat(paste("L'objet retourné prendra la forme d'une classe S3.
    #           $A pour les Rafael A, et B, C, ...\n"))
    
    
    formats <- pmeasyr::formats %>% dplyr::filter(champ == "rsf", table == "rafael", an == substr(annee,3,4))
    
    r <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rsfa"),
                         readr::fwf_widths(NA, 'lon'),
                         col_types = readr::cols('c'),  ...)
    readr::problems(r) -> synthese_import
    
    typi_r <- 9
    if (annee > 2016){typi_r <- 11}
  }
  if (lamda == T){
    # cat(paste("Import des rsfa-maj", annee, paste0("M",mois),"\n"))
    # cat(paste("L'objet retourné prendra la forme d'une classe S3.
    #           $A pour les Rafael A, et B, C, ...\n"))
    # 
    
    formats <- pmeasyr::formats %>% dplyr::filter(champ == "rsf", table == "rafael-maj", an == substr(annee,3,4))
    
    r <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rsfa-maj"),
                         readr::fwf_widths(NA, 'lon'),
                         col_types = readr::cols('c'),  ...)
    readr::problems(r) -> synthese_import
    typi_r <- 27
    if (annee > 2017){typi_r <- 29}
  }
  
  former <- function(cla, col1){
    switch(cla,
           'c' = col1,
           'trim' = col1 %>% stringr::str_trim(),
           'i' = col1 %>% as.integer(),
           'n' = (col1 %>% as.numeric() )/100)
  }
  
  cutt <- function(typs, lib){
    fa <- formats %>% dplyr::filter(Typer == typs)
    
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    
    r %>% dplyr::filter(substr(lon,typi_r,typi_r) == typs) -> one
    for (i in 1:length(deb)){
      temp <- dplyr::as_data_frame(former(fa$cla[i], u(one$lon, i)))
      names(temp) <- fa$nom[i]
      one <- dplyr::bind_cols(one, temp)
    }
    one %>% dplyr::select(-lon) -> one
    if (lib == T){
      one %>% sjlabelled::set_label(fa$libelle) -> one
    }
    return(one)
  }
  
  
  if ('A' %in% lister){rafael_A <- suppressWarnings(cutt('A', lib))}else{rafael_A <- data.frame()}
  r %>% dplyr::filter(substr(lon,typi_r,typi_r) != 'A') -> r
  if ('B' %in% lister){rafael_B <- suppressWarnings(cutt('B', lib))}else{rafael_B <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'B') -> r
  if ('C' %in% lister){rafael_C <- suppressWarnings(cutt('C', lib))}else{rafael_C <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'C') -> r
  if ('M' %in% lister){rafael_M <- suppressWarnings(cutt('M', lib))}else{rafael_M <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'M') -> r
  if ('L' %in% lister){rafael_L <- suppressWarnings(cutt('L', lib))}else{rafael_L <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'L') -> r
  if ('P' %in% lister){rafael_P <- suppressWarnings(cutt('P', lib))}else{rafael_P <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'H') -> r
  if ('H' %in% lister){rafael_H <- suppressWarnings(cutt('H', lib))}else{rafael_H <- data.frame()}
  rm(r)
  
  deux<-Sys.time()
  #at(paste("Rafaels",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
  
  if (stat == T){
    print(
      knitr::kable(dplyr::data_frame(Rafael = c('A', 'B', 'C', 'H', 'L', 'M',  'P'),
                                     Lignes = c(nrow(rafael_A),
                                                nrow(rafael_B),
                                                nrow(rafael_C),
                                                nrow(rafael_H),
                                                nrow(rafael_L),
                                                nrow(rafael_M),
                                                nrow(rafael_P)))))
  }
  if (tolower_names){
    names(rafael_A) <- tolower(names(rafael_A))
    names(rafael_B) <- tolower(names(rafael_B))
    names(rafael_C) <- tolower(names(rafael_C))
    names(rafael_H) <- tolower(names(rafael_H))
    names(rafael_L) <- tolower(names(rafael_L))
    names(rafael_M) <- tolower(names(rafael_M))
    names(rafael_P) <- tolower(names(rafael_P))
  }
  
  r_ii <- list("A" = rafael_A,
               "B" = rafael_B,
               "C" = rafael_C,
               "H" = rafael_H,
               "L" = rafael_L,
               "M" = rafael_M,
               "P" = rafael_P)
  
  attr(r_ii,"problems") <- synthese_import
  return(r_ii)
  
}

#' ~ RSF - Import des Anohosp RSFA
#'
#' Import du fichier ANO-ACE RSF Out ou le ano-ace-maj (reprise) 
#'
#' Formats depuis 2012 pris en charge pour les ano-ace
#' Formats depuis 2014 pris en charge pour les ano-ace-maj (reprise 2013)
#' 
#' Structure du nom du fichier attendu  :
#' \emph{finess.annee.moisc.ano}
#'
#' \strong{750712184.2016.2.ano}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lamda a TRUE, importe le fichier ano-ace-maj
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}
#' @param tolower_names a TRUE les noms de colonnes sont tous en minuscules
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame ou tbl_df) qui contient les données Anohosp in / out
#'
#' @examples
#' \dontrun{
#'    iano_rafael('750712184', 2015, 12,'~/Documents/data/rsf') -> ano_out15
#'    iano_rafael('750712184', 2015, 12,'~/Documents/data/rsf', lamda = T) -> lamda_maj_ano_out14
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irafael}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iano_rafael(finess, annee, mois, path, lib = T, lamda = F, tolower_names = F, ...)
#' @export iano_rafael
#' @export
iano_rafael <- function(...){
  UseMethod('iano_rafael')
}



#' @export
iano_rafael.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(iano_rafael.default, param2)
}

#' @export
iano_rafael.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(iano_rafael.default, param2)
}

#' @export
iano_rafael.default <- function(finess, annee, mois, path,  lib = T, lamda = F, tolower_names = F, ...){
  if (annee<2012|annee > 2020){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  if (lamda == F){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'rsf', table == 'rafael_ano', an == substr(as.character(annee),3,4))
  }
  if (lamda == T){
    format <- pmeasyr::formats %>% dplyr::filter(champ == 'rsf', table == 'rafael_ano-maj', an == substr(as.character(annee),3,4))
  }
  
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
  if (lamda == F){    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano-ace"),
                                               readr::fwf_widths(af,an), col_types = at , na=character(), ...) 
  readr::problems(ano_i) -> synthese_import
  ano_i <- ano_i %>%
    dplyr::mutate(DTSORT   = lubridate::dmy(DTSORT),
                  DTENT    = lubridate::dmy(DTENT),
                  cok = ((CRNOSEC=='0')+(CRDNAIS=='0')+ (CRSEXE=='0') + (CRNAS=='0') +
                           (CRDENTR=='0') ==5)) %>% sjlabelled::set_label(c(libelles, 'Chaînage Ok'))
  }
  if (lamda == T){    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano-ace-maj"),
                                               readr::fwf_widths(af,an), col_types = at , na=character(), ...)  
  readr::problems(ano_i) -> synthese_import
  ano_i <- ano_i %>%
    dplyr::mutate(DTSORT   = lubridate::dmy(DTSORT),
                  DTENT    = lubridate::dmy(DTENT),
                  cok = ((CRNOSEC=='0')+(CRDNAIS=='0')+ (CRSEXE=='0') + (CRNAS=='0') +
                           (CRDENTR=='0') ==5)) %>% sjlabelled::set_label(c(libelles, 'Chaînage Ok'))
  }
  
  
  if (tolower_names){
    names(ano_i) <- tolower(names(ano_i))
  }
  attr(ano_i,"problems") <- synthese_import
  return(ano_i)
}

#' Table des formats
#'
#' @name formats
#' @docType data
#' @author G. Pressiat
#' @keywords data
NULL



#' ~ par - Noyau de parametres
#' 
#' Définir un noyau de paramètres
#' 
#' Voir exemple
#' 
#' @exportClass pm_param
#' @author G. Pressiat
#' 
#' @examples
#' \dontrun{
#' library(magrittr)
#' 
#' p <- noyau_pmeasyr(
#' finess = '750712184',
#' annee  = 2016,
#' mois   = 12,
#' path   = '~/Documents/data/mco',
#' progress = F
#' )
#' 
#' p %>% adezip(type = "out", liste = "")
#' 
#' p %>% irsa()     -> rsa
#' p %>% iano_mco() -> ano
#' p %>% ipo()      -> po
#' 
#' adezip(type = "in", liste = "")
#'  
#' p %>% irum()     -> rum
#' 
#' # Modifier le type d'import :
#' irsa(p, typi = 6) -> rsa
#' 
#' # Pour visualiser p : 
#' p
#' print(p)
#' }
#' @seealso \code{\link{noyau_skeleton}}
#' @export 
noyau_pmeasyr <- function(...){
  params <- list(...)
  #attr(params, "class") <- "list"
  attr(params, "class") <- "pm_param"
  #class(params) <- append(class(params),"pm_param")
  return(params)
}

#' @rdname noyau_pmeasyr
#' @export 
print.pm_param <- function(x, ...){
  i <- unlist(x) %>% 
    t() %>% 
    as.data.frame(stringsAsFactors=F)  %>% 
    tidyr::gather("parametre", "valeur")
  a <- as.character(knitr::kable(i))
  a <- paste0('', a)
  a <- c('*** Noyau de param pmeasyr ***\n', a)
  cat(a, sep="\n")

}

#' ~ par - Noyau de parametres
#' 
#' Générer un squelette de noyau de paramètres
#' 
#' Voir exemple
#' 
#' @author G. Pressiat
#' 
#' @examples
#' \dontrun{
#' 
#' noyau_skeleton()
#' ## résultat : 
#' ## noyau_pmeasyr(
#' ##   finess = '.........',
#' ##   annee  = ....,
#' ##   mois   = ..,
#' ##   path   = ''
#' ## ) -> p
#'
#' noyau_skeleton("alpha_bravo", T)
#' 
#' ## noyau_pmeasyr(
#' ##   finess = '.........',
#' ##   annee  = ....,
#' ##   mois   = ..,
#' ##   path   = ''
#' ## ) -> alpha_bravo
#' ## 
#' ## # adezip(alpha_bravo, type = 'out')
#' ## # adezip(alpha_bravo, type = 'in') 
#'
#' }
#'
#' @seealso \code{\link{noyau_pmeasyr}}
#' @export 
noyau_skeleton <- function(nom = "p", zip = F){
  cat(paste0("\nnoyau_pmeasyr(
  finess = \'.........\',
  annee  = ....,
  mois   = ..,
  path   = \'\'
) -> ", nom,  "\n\n"))
  
  if (zip){
    cat(paste0("# adezip(", nom, ", type = \'out\')\n# adezip(", nom, ", type = \'in\')"))
  }
}

##############################################
####################### LABELS ###############
##############################################




#' ~ Labels pour le PMSI
#'
#' Attribuer des libelles aux colonnes PMSI
#'
#' @param col Colonne à laquelle attribuer le libellé
#' @param Mode_entree  '6' : 'Mutation'
#' @param Mode_sortie  '9' : 'Décès'
#' @param Provenance  '1' : 'MCO'
#' @param Destination  '6' : 'HAD'
#' @param Sexe  '2' : 'Femme'
#'
#' @return Un vecteur caractère ou facteur
#'
#' @examples
#' \dontrun{
#'    labeleasier(rsa$rsa$SEXE, Sexe = T, F)
#'    labeleasier(rsa$rsa$DEST, Destination = T, F)
#' }
#'
#' @author G. Pressiat
#'
#'



#' @import forcats
#' @export
labeleasier <- function(col,
                        Mode_entree = F,
                        Mode_sortie = F,
                        Provenance = F,
                        Destination = F,
                        Sexe = F,
                        facteur = F){
  
  choix = c(Mode_entree,
            Mode_sortie,
            Provenance,
            Destination,
            Sexe)
  
  noms <- c('Mode_entree',
            'Mode_sortie',
            'Provenance',
            'Destination',
            'Sexe')
  
  if (sum( choix) != 1){
    stop("Un et un seul format doit être spécifié par TRUE en paramètre")}
  quoi <- noms[which(choix==T)]
  
  r <- suppressWarnings(switch(quoi,
                               "Mode_entree" = forcats::fct_recode(col,
                                                                   "PIE"        = "0",
                                                                   "Mutation"   = "6",
                                                                   "Transfert"  = "7",
                                                                   "Domicile"   = "8",
                                                                   "Inconnu"    = ''),
                               
                               "Mode_sortie" = forcats::fct_recode(col,
                                                                   "Mutation"  = "6",
                                                                   "Transfert" = "7",
                                                                   "Domicile"  = "8",
                                                                   "Décès"     = "9",
                                                                   "Inconnu"    = ''),
                               
                               "Provenance"    = forcats::fct_recode(col,
                                                                     "MCO"       = "1",
                                                                     "SSR"       = "2",
                                                                     "SLD"       = "3",
                                                                     "PSY"       = "4",
                                                                     "SAU"       = "5",
                                                                     "HAD"       = "6",
                                                                     "ESMS"      = "7",
                                                                     "SIAD"      = "8",
                                                                     "Inconnu"    = ''
                               ),
                               
                               "Destination"    = forcats::fct_recode(col,
                                                                      "MCO"       = "1",
                                                                      "SSR"       = "2",
                                                                      "SLD"       = "3",
                                                                      "PSY"       = "4",
                                                                      "SAU"       = "5",
                                                                      "HAD"       = "6",
                                                                      "ESMS"      = "7",
                                                                      "SIAD"      = "8",
                                                                      "Inconnu"    = ''
                               ),
                               
                               "Sexe"    = forcats::fct_recode(col,
                                                               "Homme"      = "1",
                                                               "Femme"      = "2",
                                                               "Inconnu"    = "9",
                                                               "Inconnu"    = '0',
                                                               "Inconnu"    = ''
                               )))
  if (facteur == T){return(r)}else{return(as.character(r))}
  
}

##############################################
####################### DB ###################
##############################################



`%+%` <- function(x,y){paste0(x,y)}



#' ~ db - Copier les rsa dans une db
#'
#' Copier les rsa, les passages um, les actes et les diagnostics des rsa, et ano dans une db
#' 
#' Les tables sont importées dans R puis copiées dans la db.
#' La table diag est créée, les variables ghm, année séquentielle des tarifs
#' et un champ caractère diagnostics sont ajoutés à la table rsa.
#' Le tra est ajouté aux tables.
#' 
#' @param con la connexion a la base de donnees (src_..)
#' @param p le noyau pmeasyr
#' @param remove a TRUE, les tables precedentes rsa de l'annee sont effacees avant
#' @param zip a TRUE les fichiers des archives sont dezippes et effaces apres integration dans la db
#' @param indexes index a ajouter a la table dans la base (voir \code{\link[dplyr]{copy_to}})
#' @return nothing
#' @export
#'
#' @usage db_mco_out(con, p, remove = T, zip = T, indexes = list(),  ...)
#' @examples
#' \dontrun{
#' purrr::quietly(db_mco_out)(con, p) -> statuts ; gc(); #ok
#' purrr::quietly(db_mco_out)(con, p, annee = 2017, mois = 7) -> statuts ; gc(); #..
#' }
db_mco_out <- function (con, p, remove = T, zip = T, indexes = list(), ...){
  p <- utils::modifyList(p, list(...))
  an <- substr(as.character(p$annee), 3, 4)
  if (remove == T) {
    #u <- DBI::dbListTables(con)
    u <- dplyr::src_tbls(con)
    lr <- u[grepl("_rsa_", u) & grepl(an, u)]
    lapply(lr, function(x) {
      DBI::dbRemoveTable(con$con, x)
    })
  }
  if (zip == T) {
    adezip(p, type = "out", liste = c("rsa", "ano", "tra"))
  }
  rsa <- pmeasyr::irsa(p, typi = 6) %>% pmeasyr::tdiag()
  rsa_ano <- pmeasyr::iano_mco(p)
  tra <- pmeasyr::itra(p)
  rsa$rsa <- pmeasyr::inner_tra(rsa$rsa, tra) %>% mutate(diags = paste0(dpdrum,
                                                                        das, sep = " "))
  rsa$actes <- pmeasyr::inner_tra(rsa$actes, tra)
  rsa$diags <- pmeasyr::inner_tra(rsa$diags, tra)
  rsa$rsa_um <- pmeasyr::inner_tra(rsa$rsa_um, tra)
  rsa_ano <- pmeasyr::inner_tra(rsa_ano, tra)
  
  purrr::flatten_int(purrr::map(indexes, length)) -> t_1
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsa$rsa))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsa$rsa),    "mco_" %+% an %+% "_rsa_rsa",   temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsa$actes))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsa$actes),  "mco_" %+% an %+% "_rsa_actes", temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsa$diags))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsa$diags),  "mco_" %+% an %+% "_rsa_diags", temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsa$rsa_um))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsa$rsa_um), "mco_" %+% an %+% "_rsa_um",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsa_ano))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsa_ano),    "mco_" %+% an %+% "_rsa_ano",   temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  
  if (zip == T) {
    pmeasyr::adelete(p)
  }
}


#' ~ db - Copier les rha dans une db
#'
#' Copier les rha, les actes, les diagnostics des rha, ssrha et ano dans une db
#' 
#' Les tables sont importées dans R puis copiées dans la db.
#' Le tra est ajouté aux tables.
#' 
#' @param con la connexion a la base de donnees (src_..)
#' @param p le noyau pmeasyr
#' @param remove a TRUE, les tables precedentes rha de l'annee sont effacees avant
#' @param zip a TRUE les fichiers des archives sont dezippes et effaces apres integration dans la db
#' @param indexes index a ajouter a la table dans la base (voir \code{\link[dplyr]{copy_to}})
#' @return nothing
#' @export
#'
#' @usage db_ssr_out(con, p, remove = T, zip = T, indexes = list(), ...)
#' @examples
#' \dontrun{
#' purrr::quietly(db_ssr_out)(con, p) -> statuts ; gc(); #ok
#' purrr::quietly(db_ssr_out)(con, p, annee = 2017, mois = 7) -> statuts ; gc(); #..
#' }
db_ssr_out <- function (con, p, remove = T, zip = T, indexes = list(), ...){
  p <- utils::modifyList(p, list(...))
  an <- substr(as.character(p$annee), 3, 4)
  if (remove == T) {
    #u <- DBI::dbListTables(con)
    u <- dplyr::src_tbls(con)
    lr <- u[grepl("ssr", u) & grepl(an, u)]
    lapply(lr, function(x) {
      DBI::dbRemoveTable(con$con, x)
    })
  }
  if (zip == T) {
    pmeasyr::adezip(p, type = "out", liste = c("rha", "ano",
                                               "sha", "tra"))
  }
  rha <- pmeasyr::irha(p)
  ssrha <- pmeasyr::issrha(p)
  rha_ano <- pmeasyr::iano_ssr(p)
  tra <- pmeasyr::itra(p, champ = "ssr")
  rha$rha <- pmeasyr::inner_tra(rha$rha, tra, champ = "ssr")
  rha$acdi <- pmeasyr::inner_tra(rha$acdi, tra, champ = "ssr")
  if (p$annee > 2016) {
    ssrha$ssrha <- pmeasyr::inner_tra(ssrha$ssrha, tra, champ = "ssr")
    ssrha$gme <- pmeasyr::inner_tra(ssrha$gme, tra, champ = "ssr")
  }
  else {
    ssrha <- pmeasyr::inner_tra(ssrha, tra, champ = "ssr")
  }
  rha_ano <- pmeasyr::inner_tra(rha_ano, tra, champ = "ssr")
  
  
  purrr::flatten_int(purrr::map(indexes, length)) -> t_1
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rha$rha))))) -> t_2
  
  dplyr::copy_to(con, dplyr::as_data_frame(rha$rha),   "ssr_" %+% an %+% "_rha_rha",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rha$acdi))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rha$acdi),   "ssr_" %+% an %+% "_rha_acdi",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rha_ano))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rha_ano),    "ssr_" %+% an %+% "_rha_ano",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  
  if (p$annee > 2016) {
    purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(ssrha$ssrha))))) -> t_2
    dplyr::copy_to(con, dplyr::as_data_frame(ssrha$ssrha),    "ssr_" %+% an %+% "_rha_ssrha",    temporary = FALSE, overwrite = TRUE,
                   indexes = indexes[t_1 == t_2])
    purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(ssrha$gme))))) -> t_2
    dplyr::copy_to(con, dplyr::as_data_frame(ssrha$gme),    "ssr_" %+% an %+% "_rha_gme",    temporary = FALSE, overwrite = TRUE,
                   indexes = indexes[t_1 == t_2])
  }
  else {
    purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(ssrha))))) -> t_2
    dplyr::copy_to(con, dplyr::as_data_frame(ssrha),    "ssr_" %+% an %+% "_rha_ssrha",    temporary = FALSE, overwrite = TRUE,
                   indexes = indexes[t_1 == t_2])
  }
  if (zip == T) {
    pmeasyr::adelete(p)
  }
}

#' ~ db - Copier les rapss dans une db
#'
#' Copier les rapss, les actes, les diagnostics et la table ano des rapss dans une db
#' 
#' Les tables sont importées dans R puis copiées dans la db.
#' Le tra est ajouté aux tables.
#' 
#' @param con la connexion a la base de donnees (src_..)
#' @param p le noyau pmeasyr
#' @param remove a TRUE, les tables precedentes rapss de l'annee sont effacees avant
#' @param zip a TRUE les fichiers des archives sont dezippes et effaces apres integration dans la db
#' @param indexes index a ajouter a la table dans la base (voir \code{\link[dplyr]{copy_to}})
#' @return nothing
#' @export
#'
#' @usage db_had_out(con, p, remove = T, zip = T, indexes = list(), ...)
#' @examples
#' \dontrun{
#' purrr::quietly(db_had_out)(con, p) -> statuts ; gc(); #ok
#' purrr::quietly(db_had_out)(con, p, annee = 2017, mois = 7) -> statuts ; gc(); #..
#' }
db_had_out <- function (con, p, remove = T, zip = T, indexes = list(), ...){
  p <- utils::modifyList(p, list(...))
  an <- substr(as.character(p$annee), 3, 4)
  if (remove == T) {
    #u <- DBI::dbListTables(con)
    u <- dplyr::src_tbls(con)
    lr <- u[grepl("_rapss_", u) & grepl(an, u)]
    lapply(lr, function(x) {
      DBI::dbRemoveTable(con$con, x)
    })
  }
  if (zip == T) {
    pmeasyr::adezip(p, type = "out", liste = c("rapss", "ano",
                                               "tra"))
  }
  rapss <- pmeasyr::irapss(p)
  rapss_ano <- pmeasyr::iano_had(p)
  tra <- pmeasyr::itra(p, champ = "had")
  rapss$rapss <- pmeasyr::inner_tra(rapss$rapss, tra, champ = "had")
  rapss$acdi <- pmeasyr::inner_tra(rapss$acdi, tra, champ = "had")
  rapss_ano <- pmeasyr::inner_tra(rapss_ano, tra, champ = "had")
  
  purrr::flatten_int(purrr::map(indexes, length)) -> t_1
  
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rapss$rapss))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rapss$rapss),    "had_" %+% an %+% "_rapss_rapss",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rapss$acdi))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rapss$acdi),    "had_" %+% an %+% "_rapss_acdi",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rapss_ano))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rapss_ano),    "had_" %+% an %+% "_rapss_ano",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  if (zip == T) {
    pmeasyr::adelete(p)
  }
}

#' ~ db - Copier les rpsa dans une db
#'
#' Copier les rpsa, les actes, les diagnostics et la table ano des rpsa dans une db
#' 
#' Les tables sont importées dans R puis copiées dans la db.
#' Le tra est ajouté aux tables.
#' 
#' @param con la connexion a la base de donnees (src_..)
#' @param p le noyau pmeasyr
#' @param remove a TRUE, les tables precedentes rpsa de l'annee sont effacees avant
#' @param zip a TRUE les fichiers des archives sont dezippes et effaces apres integration dans la db
#' @param indexes index a ajouter a la table dans la base (voir \code{\link[dplyr]{copy_to}})
#' @return nothing
#' @export
#'
#' @usage db_psy_out(con, p, remove = T, zip = T, indexes = list(), ...)
#' @examples
#' \dontrun{
#' purrr::quietly(db_psy_out)(con, p) -> statuts ; gc(); #ok
#' purrr::quietly(db_psy_out)(con, p, annee = 2017, mois = 6) -> statuts ; gc(); #..
#' }
db_psy_out <- function (con, p, remove = T, zip = T, indexes = list(), ...){
  p <- utils::modifyList(p, list(...))
  an <- substr(as.character(p$annee), 3, 4)
  if (remove == T) {
    #u <- DBI::dbListTables(con)
    u <- dplyr::src_tbls(con)
    lr <- u[grepl("psy", u) & grepl(an, u)]
    lapply(lr, function(x) {
      DBI::dbRemoveTable(con$con, x)
    })
  }
  if (zip == T) {
    pmeasyr::adezip(p, type = "out", liste = c("rpsa", "ano",
                                               "tra", "tra.raa", "r3a"))
  }
  
  rpsa <- pmeasyr::irpsa(p)
  rpsa_ano <- pmeasyr::iano_psy(p)
  r3a <- pmeasyr::ir3a(p)
  tra_rpsa <- pmeasyr::itra(p, champ = "psy_rpsa")
  tra_r3a <- pmeasyr::itra(p, champ = "psy_r3a")
  
  rpsa$rpsa <- pmeasyr::inner_tra(rpsa$rpsa, tra_rpsa, champ = "psy_rpsa")
  rpsa$das <- pmeasyr::inner_tra(rpsa$das, tra_rpsa, champ = "psy_rpsa")
  if (p$annee > 2016) {
    rpsa$actes <- pmeasyr::inner_tra(rpsa$actes, tra_rpsa)
  }
  rpsa_ano <- pmeasyr::inner_tra(rpsa_ano, tra_rpsa, champ = "psy_rpsa")
  r3a$r3a <- pmeasyr::inner_tra(r3a$r3a, tra_r3a, champ = "psy_r3a")
  r3a$da <- pmeasyr::inner_tra(r3a$da, tra_r3a, champ = "psy_r3a")
  
  purrr::flatten_int(purrr::map(indexes, length)) -> t_1
  
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rpsa$rpsa))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rpsa$rpsa),    "psy_" %+% an %+% "_rpsa_rpsa",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rpsa$das))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rpsa$das),    "psy_" %+% an %+% "_rpsa_das",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  
  if (p$annee > 2016) {
    purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rpsa$actes))))) -> t_2
    dplyr::copy_to(con, dplyr::as_data_frame(rpsa$actes),    "psy_" %+% an %+% "_rpsa_actes",    temporary = FALSE, overwrite = TRUE,
                   indexes = indexes[t_1 == t_2])
  }
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rpsa_ano))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rpsa_ano),    "psy_" %+% an %+% "_rpsa_ano",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])

  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(r3a$r3a))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(r3a$r3a),    "psy_" %+% an %+% "_r3a_r3a",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])

  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(r3a$das))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(r3a$das),    "psy_" %+% an %+% "_r3a_das",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])

  if (zip == T) {
    pmeasyr::adelete(p)
  }
}


#' ~ db - Copier les rum dans une db
#'
#' Copier les rum, les actes et les diagnostics des rums dans une db
#' 
#' Les tables sont importées dans R puis copiées dans la db
#' La table diag est créée et la durée des rum est calculée (DUREESEJPART)
#'
#' @param con la connexion a la base de donnees (src_..)
#' @param p le noyau pmeasyr
#' @param remove a TRUE, les tables precedentes rum de l'annee sont effacees avant
#' @param zip a TRUE les fichiers des archives sont dezippes et effaces apres integration dans la db
#' @param indexes index a ajouter a la table dans la base (voir \code{\link[dplyr]{copy_to}})
#' @return nothing
#' @export
#'
#' @usage db_mco_in(con, p, remove = T, zip = T, indexes = list(), ...)
#' @examples
#' \dontrun{
#' purrr::quietly(db_mco_in)(con, p) -> statuts ; gc(); #ok
#' purrr::quietly(db_mco_in)(con, p, annee = 2015) -> statuts ; gc(); #..
#' }
db_mco_in <- function (con, p, remove = T, zip = T, indexes = list(), ...) {
  p <- utils::modifyList(p, list(...))
  an <- substr(as.character(p$annee), 3, 4)
  if (remove == T) {
    #u <- DBI::dbListTables(con)
    u <- dplyr::src_tbls(con)
    lr <- u[grepl("_rum_", u) & grepl(an, u)]
    lapply(lr, function(x) {
      dbRemoveTable(con$con, x)
    })
  }
  if (zip == T) {
    pmeasyr::adezip(p, type = "in", liste = "rss")
  }
  rum <- pmeasyr::irum(p, typi = 4) %>% pmeasyr::tdiag()
  purrr::flatten_int(purrr::map(indexes, length)) -> t_1
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rum$rum))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rum$rum),   "mco_" %+% an %+% "_rum_rum",    temporary = FALSE, overwrite = TRUE, indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rum$diags))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rum$diags), "mco_" %+% an %+% "_rum_diags",  temporary = FALSE, overwrite = TRUE, indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rum$actes))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rum$actes), "mco_" %+% an %+% "_rum_actes",  temporary = FALSE, overwrite = TRUE, indexes = indexes[t_1 == t_2])
  if (zip == T) {
    pmeasyr::adelete(p)
  }
}

#' ~ db - Copier les rsf dans une db
#'
#' Copier tous les rsf (lettre par lettre, A, B, C, ...), les ano-ace dans une db
#' 
#' Les tables sont importées dans R puis copiées dans la db
#'  
#' @param con la connexion a la base de donnees (src_..)
#' @param p le noyau pmeasyr
#' @param remove a TRUE, les tables precedentes rafael de l'annee sont effacees avant
#' @param zip a TRUE les fichiers des archives sont dezippes et effaces apres integration dans la db
#' @param indexes index a ajouter a la table dans la base (voir \code{\link[dplyr]{copy_to}})
#' @return nothing
#' 
#' @import DBI
#' @export
#'
#' @usage db_rsf_out(con, p, remove = T, zip = T, indexes = list(), ...)
#' @examples
#' \dontrun{
#' purrr::quietly(db_rsf_out)(con, p) -> statuts ; gc(); #ok
#' purrr::quietly(db_rsf_out)(con, p, annee = 2014) -> statuts ; gc(); #ok
#' }
db_rsf_out <- function (con, p, remove = T, zip = T, indexes = list(), ...){
  p <- utils::modifyList(p, list(...))
  an <- substr(as.character(p$annee), 3, 4)
  if (remove == T) {
    u <- dplyr::src_tbls(con)
    lr <- u[grepl("_rafael_", u) & grepl(an, u)]
    lapply(lr, function(x) {
      DBI::dbRemoveTable(con$con, x)
    })
  }
  if (zip == T) {
    pmeasyr::adezip(p, type = "out", liste = c("rsfa", "ano-ace"))
  }
  
  rsf <- pmeasyr::irafael(p)
  rsf_ano <- pmeasyr::iano_rafael(p)
  
  purrr::flatten_int(purrr::map(indexes, length)) -> t_1
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf$A))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf$A),   "rsf_" %+% an %+% "_rafael_a",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf$B))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf$B),   "rsf_" %+% an %+% "_rafael_b",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf$C))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf$C),   "rsf_" %+% an %+% "_rafael_c",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf$H))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf$H),   "rsf_" %+% an %+% "_rafael_h",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf$L))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf$L),   "rsf_" %+% an %+% "_rafael_l",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf$M))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf$M),   "rsf_" %+% an %+% "_rafael_m",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf$P))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf$P),   "rsf_" %+% an %+% "_rafael_p",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(rsf_ano))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(rsf_ano),   "rsf_" %+% an %+% "_rafael_ano",    temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
  
  if (zip == T) {
    pmeasyr::adelete(p)
  }
}


#' ~ db - Lister les tables d'une db en tableau
#'
#' @param con la connexion a la base de donnees (src_..)
#' @param nb le nombre de lignes du tableau
#'
#' @return nothing
#' @export
#'
#' @usage db_liste_tables(con, nb = 15)
#' @examples
#' \dontrun{
#' db_liste_tables(con)
#' }
#' @export
db_liste_tables <- function(con, nb = 15){
  DBI::dbListTables(con) -> liste
  
  suppressWarnings(matrix(c(sort(liste), rep('',nb - length(liste) %% nb)),
                          nrow = nb)) %>% tibble::as_tibble() %>%
    knitr::kable(col.names = paste0(nb %+% ' tables, n°',
                                    1:(floor((length(liste)/ nb) )+1)))
}

#' ~ db - remote access aux tables mco
#'
#' @param con Connexion à la base de données
#' @param an Année pmsi (ex: 16)
#' @param table Table à requêter
#' @return remote table
#'
#' @usage tbl_mco(con, an, table)
#' @examples
#' \dontrun{
#' tbl_mco(con, 16, 'rsa_rsa')
#' }
#' @export
tbl_mco <- function(con, an, table){
  dplyr::tbl(con, 'mco_' %+% an %+% '_' %+% table)  
}

#' ~ db - remote access aux tables rsf
#'
#' @param con Connexion à la base de données
#' @param an Année pmsi (ex: 16)
#' @param table Table à requêter
#' @return remote table
#'
#' @usage tbl_rsf(con, an, table)
#' @examples
#' \dontrun{
#' tbl_rsf(con, 16, 'rsf_rafael_ano')
#' }
#' @export
tbl_rsf <- function(con, an, table){
  dplyr::tbl(con, 'rsf_' %+% an %+% '_' %+% table)  
}

#' ~ db - remote access aux tables ssr
#'
#' @param con Connexion à la base de données
#' @param an Année pmsi (ex: 16)
#' @param table Table à requêter
#' @return remote table
#'
#' @usage tbl_ssr(con, an, table)
#' @examples
#' \dontrun{
#' tbl_ssr(con, 16, 'rha_rha')
#' }
#' @export
tbl_ssr <- function(con, an, table){
  dplyr::tbl(con, 'ssr_' %+% an %+% '_' %+% table)  
}

#' ~ db - remote access aux tables had
#'
#' @param con Connexion à la base de données
#' @param an Année pmsi (ex: 16)
#' @param table Table à requêter
#' @return remote table
#' @return tibble
#'
#' @usage tbl_had(con, an, table)
#' @examples
#' \dontrun{
#' tbl_had(con, 16, 'rapss_rapss')
#' }
#' @export
tbl_had <- function(con, an, table){
  dplyr::tbl(con, 'had_' %+% an %+% '_' %+% table)  
}

#' ~ db - remote access aux tables psy
#'
#' @param con Connexion à la base de données
#' @param an Année pmsi (ex: 16)
#' @param table Table à requêter
#' @return remote table
#' @return tibble
#'
#' @usage tbl_psy(con, an, table)
#' @examples
#' \dontrun{
#' tbl_psy(con, 16, 'rpsa_rpsa')
#' }
#' @export
tbl_psy <- function(con, an, table){
  dplyr::tbl(con, 'psy_' %+% an %+% '_' %+% table)  
}

#' ~ db - Copier un tibble dans une db
#'
#' Copier une table R dans une db
#' 
#' La tables déjà importée dans R est copiée dans la db
#' 
#' @param con la connexion a la base de donnees (src_..)
#' @param an l'annee pmsi
#' @param table La table R (tibble) a copier dans la db
#' @param prefix prefixe de la table dans la db (ex : mco, rsf, ssr, ...)
#' @param suffix suffixe de la table dans la db (ex : rum_rum, rha_actes, rapss_rapss, ...)
#' @param indexes index a ajouter a la table dans la base (voir \code{\link[dplyr]{copy_to}})
#' 
#' @return nothing
#' 
#'
#' @usage db_generique(con, an, table, prefix, suffix, indexes = list(), remove = T)
#' @examples
#' \dontrun{
#' purrr::quietly(db_generique)(con, 16, ma_table, 'had', 'rapss_ano') -> statuts ; gc(); #
#' # Result in db : had_16_rapss_ano
#' }
#' @export 
db_generique <- function(con,  an, table, prefix, suffix, indexes = list(), remove = T){
  nom <- prefix %+% "_" %+% an %+% "_" %+% suffix
  if (remove == T){
    #DBI::dbListTables(con) -> u
    dplyr::src_tbls(con) -> u
    if (length(u[u == nom])>0){
    DBI::dbRemoveTable(con$con, nom)}
  }
  
  purrr::flatten_int(purrr::map(indexes, length)) -> t_1
  purrr::flatten_int(purrr::map(indexes, function(x)(sum(x %in% names(table))))) -> t_2
  dplyr::copy_to(con, dplyr::as_data_frame(table),    nom,   temporary = FALSE, overwrite = TRUE,
                 indexes = indexes[t_1 == t_2])
}

##############################################
################## REQUETES ##################
##############################################

#' ~ req : mise en forme d'une liste de codes
#'
#'
#' @examples
#' \dontrun{
#' li <- c('QEFA003', 'QEFA005', 'QEFA010', 'QEFA013', 'QEFA015', 'QEFA019', 'QEFA020')
#'
#' enrobeur(li, robe="", interstice="|") %>% cat()
#'
#' enrobeur(li, robe="\'", interstice=",") %>% cat()
#'
#' enrobeur(li, robe="\'%", interstice="\n", symetrique = T) %>% cat()
#' }
#'
#' @author G. Pressiat
#' voir \url{https://guillaumepressiat.shinyapps.io/transcodeur/} pour son utilisation interactive hors AP-HP
#' @export
enrobeur <- function(a, robe = "\'", colonne = F, interstice = ", ", symetrique = F){
  strReverse <- function(x) sapply(lapply(strsplit(x, NULL), rev), paste,
                                   collapse="")
  
  b <- paste0(robe,a,ifelse(symetrique == F,robe, strReverse(robe)))
  if (colonne == F){
    return(paste0(b, collapse = interstice))
  }
  else { return(b)}
  
}

#' ~ req : requeter les rsa avec une liste
#'
#'
#' @examples
#' \dontrun{
#' liste = list(actes = c('EBLA003', 'EQLF002'))
#' requete(rsa, liste)
#'
#' liste = list(actes = c('EBLA003', 'EQLF002'), dureemax = 0)
#' requete(rsa, liste)
#' }
#'
#' @return un tibble contenant les rsa respectant la requete : les rsa qui ont un acte de la liste, un diag, une duree correspondante, etc.
#'
#' @author G. Pressiat
#' @importFrom dplyr mutate inner_join select filter_ distinct tibble tbl_df data_frame
#' @importFrom purrr flatten_chr
#' @importFrom sqldf sqldf
#' @export
requete <- function (tables, elements, vars = NULL) {
  chaine = list()
  if (length(elements[["ghm"]]) > 0) {
    chaine$ghm <- paste0("grepl('", enrobeur(elements$ghm, 
                                             robe = "", colonne = F, interstice = "|"), "', ghm) ")
  }
  if (length(elements[["ghm_exclus"]]) > 0) {
    chaine$ghm_exclus <- paste0("!grepl('", enrobeur(elements$ghm_exclus, 
                                                     robe = "", colonne = F, interstice = "|"), "', ghm) ")
  }
  if (length(elements[["diags_exclus"]]) > 0) {
    chaine$diags_exclus = paste0("!grepl('", enrobeur(elements$diags_exclus, 
                                                      robe = "", colonne = F, interstice = "|"), "', diags) ")
  }
  if (length(elements[["agemin"]]) > 0) {
    chaine$agemin = paste0("agean >= ", elements$agemin)
  }
  if (length(elements[["agemax"]]) > 0) {
    chaine$agemax = paste0("agean <= ", elements$agemax)
  }
  if ((length(elements[["agejrmin"]]) > 0) & ((length(elements[["agemmax"]]) > 
                                               0) | (length(elements[["agemin"]]) > 0))) {
    chaine$agejrmin = paste0("agean >= ", elements$agejrmin/365.25)
  }
  if ((length(elements[["agejrmax"]]) > 0) & ((length(elements[["agemmax"]]) > 
                                               0) | (length(elements[["agemin"]]) > 0))) {
    chaine$agejrmax = paste0("agean <= ", elements$agejrmax/365.25)
  }
  if (length(elements[["agejrmin"]]) > 0 & (length(elements[["agemmax"]]) == 
                                            0) & (length(elements[["agemin"]]) == 0)) {
    chaine$agejrmin = paste0("!is.na(agejr) & agejr >= ", 
                             elements$agejrmin)
  }
  if (length(elements[["agejrmax"]]) > 0 & (length(elements[["agemmax"]]) == 
                                            0) & (length(elements[["agemin"]]) == 0)) {
    chaine$agejrmin = paste0("!is.na(agejr) & agejr <= ", 
                             elements$agejrmax)
  }
  if (length(elements[["dureemax"]]) > 0) {
    chaine$dureemax = paste0("duree <= ", elements$dureemax)
  }
  if (length(elements[["dureemin"]]) > 0) {
    chaine$dureemin <- paste0("duree >= ", elements$dureemin)
  }
  if (length(elements[["poidsmin"]]) > 0) {
    chaine$poidsmin <- paste0("poids >= ", elements$poidsmin)
  }
  if (length(elements[["poidsmax"]]) > 0) {
    chaine$podsmax <- paste0("poids <= ", elements$poidsmax)
  }
  if (length(elements[["autres"]]) > 0) {
    if (!identical(chaine, character())) {
      chaine$autres <- paste0(elements$autres)
    }
    else {
      chaine <- paste0(elements$autres)
    }
  }
  if (length(chaine) == 0) {
    chaine = list("TRUE")
  }
  rsa_filtre <- tables$rsa %>% filter_(paste0(purrr::flatten_chr(chaine), 
                                              collapse = " & ")) %>% select(cle_rsa)
  if (length(elements[["diags"]]) > 0) {
    if (elements[["positions_diags"]][1] == "toutes") {
      d <- tables$diags
      s <- paste0("select distinct cle_rsa from d where ",
                  paste0("diag like '", elements$diags, "%'",
                         collapse = " or "))
      diags <- sqldf::sqldf(s) %>% dplyr::tbl_df()
    }
    else if (elements[["positions_diags"]][1] == "dp") {
      d <- tables$rsa
      s <- paste0("select cle_rsa from d where (",
                  paste0("dp like '", elements$diags, "%'",
                         collapse = " or "), ")")
      diags <- sqldf::sqldf(s) %>% dplyr::tbl_df() %>%
        dplyr::select(cle_rsa)
    }
    else {
      d <- tables$diags
      s <- paste0("select distinct cle_rsa from d where position in (",
                  paste0(elements$positions_diags, collapse = ", "),
                  ") and ( ", paste0("diag like '", elements$diags,
                                     "%'", collapse = " or "), " )")
      diags <- sqldf::sqldf(s) %>% dplyr::tbl_df()    
    }}
  else {
    diags = dplyr::tibble()
    
  }
  if (length(elements[["actes"]]) > 0) {
    liste_actes = dplyr::data_frame(cdccam = elements$actes) %>% 
      dplyr::tbl_df()
    if (length(elements[["activite_actes"]]) > 0) {
      actes = dplyr::inner_join(tables$actes %>% filter(act %in% 
                                                          elements[["activite_actes"]]), liste_actes, 
                                by = c(cdccam = "cdccam")) %>% dplyr::distinct(cle_rsa)
    }
    else {
      actes = dplyr::inner_join(tables$actes, liste_actes, 
                                by = c(cdccam = "cdccam")) %>% dplyr::distinct(cle_rsa)
    }
  }
  else {
    actes = dplyr::tibble()
  }
  resultat <- tables$rsa %>% dplyr::inner_join(rsa_filtre, 
                                               by = "cle_rsa")
  if (length(elements[["actes"]]) > 0) {
    resultat <- resultat %>% dplyr::inner_join(actes, by = "cle_rsa")
  }
  if (length(elements[["diags"]]) > 0) {
    resultat <- resultat %>% dplyr::inner_join(diags, by = "cle_rsa")
  }
  resultat
  if (is.null(vars)) {
    return(dplyr::distinct(resultat, cle_rsa))
  }
  else {
    return(resultat %>% dplyr::select(cle_rsa, vars))
  }
}

#' ~ req : requeter les rsa dans une db avec une liste
#'
#' @examples
#' \dontrun{
#' liste = list(actes = c('EBLA003', 'EQLF002'))
#' requete_db(con, an, liste)
#'
#' liste = list(actes = c('EBLA003', 'EQLF002'), dureemax = 0)
#' requete_db(con, 16, liste)
#' }
#'
#' @return un tibble contenant les rsa respectant la requete : les rsa qui ont un acte de la liste, un diag, une duree correspondante, etc.
#'
#' @author G. Pressiat
#' @importFrom dplyr mutate inner_join select filter_ distinct tibble tbl_df data_frame
#' @export
requete_db <- function (con, an, elements, vars = NULL)
{
  chaine = list()
  if (length(elements[["ghm"]]) > 0) {
    chaine$ghm <- paste0("(", paste0("ghm %like% '", elements$ghm,
                                     "%'", collapse = " | "), ")")
  }
  if (length(elements[["ghm_exclus"]]) > 0) {
    chaine$ghm_exclus <- paste0("(", paste0("!(ghm %like% '",
                                            elements$ghm_exclus, "%')", collapse = " & "), ")")
  }
  
  if (length(elements[["agemin"]]) > 0) {
    chaine$agemin = paste0("agean >= ", elements$agemin)
  }
  if (length(elements[["agemax"]]) > 0) {
    chaine$agemax = paste0("agean <= ", elements$agemax)
  }
  if ((length(elements[["agejrmin"]]) > 0) & ((length(elements[["agemmax"]]) >
                                               0) | (length(elements[["agemin"]]) > 0))) {
    chaine$agejrmin = paste0("agean >= ", elements$agejrmin/365.25)
  }
  if ((length(elements[["agejrmax"]]) > 0) & ((length(elements[["agemmax"]]) >
                                               0) | (length(elements[["agemin"]]) > 0))) {
    chaine$agejrmax = paste0("agean <= ", elements$agejrmax/365.25)
  }
  if (length(elements[["agejrmin"]]) > 0 & (length(elements[["agemmax"]]) ==
                                            0) & (length(elements[["agemin"]]) == 0)) {
    chaine$agejrmin = paste0("!is.na(agejr) & agejr >= ",
                             elements$agejrmin)
  }
  if (length(elements[["agejrmax"]]) > 0 & (length(elements[["agemmax"]]) ==
                                            0) & (length(elements[["agemin"]]) == 0)) {
    chaine$agejrmin = paste0("!is.na(agejr) & agejr <= ",
                             elements$agejrmax)
  }
  if (length(elements[["dureemax"]]) > 0) {
    chaine$dureemax = paste0("duree <= ", elements$dureemax)
  }
  if (length(elements[["dureemin"]]) > 0) {
    chaine$dureemin <- paste0("duree >= ", elements$dureemin)
  }
  if (length(elements[["poidsmin"]]) > 0) {
    chaine$poidsmin <- paste0("poids >= ", elements$poidsmin)
  }
  if (length(elements[["poidsmax"]]) > 0) {
    chaine$podsmax <- paste0("poids <= ", elements$poidsmax)
  }
  if (length(elements[["autres"]]) > 0) {
    if (!identical(chaine, character())) {
      chaine$autres <- paste0(elements$autres)
    }
    else {
      chaine <- paste0(elements$autres)
    }
  }
  if (length(chaine) == 0) {
    chaine = list("TRUE")
  }
  rsa_filtre <- tbl_mco(con, an, "rsa_rsa") %>% dplyr::filter_(paste0(purrr::flatten_chr(chaine),
                                                                               collapse = " & ")) %>% dplyr::select(cle_rsa)
  if (length(elements[["diags"]]) > 0) {
    if (elements[["positions_diags"]][1] == "toutes"){
      chaine_d <- paste0("(", paste0("diag %like% '",elements$diags, "%'", collapse = " | "), ' )')
    } else
      if (elements[["positions_diags"]][1] == "dp"){
        chaine_d <- paste0("position == '1' & ( ", paste0("diag %like% '",elements$diags, "%'", collapse = " | "), ' )')
      } else {
        chaine_d <- paste0("position %in% c(", paste0(elements$positions_diags,
                                                      collapse = ", "), ") & (", paste0("diag %like% '",
                                                                                        elements$diags, "%'", collapse = " | "), ' )')
      }
    
    diags_filtre <- tbl_mco(con, an, "rsa_diags") %>%
      dplyr::filter_(chaine_d) %>% dplyr::distinct(cle_rsa)
  }
  else {
    diags_filtre <- NULL
  }
  if (length(elements[["diags_exclus"]]) > 0) {
    
    chaine_d <- paste0('( ', paste0("diag %like% '", elements$diags_exclus, "%'", collapse = " | "), ' )')
    
    antidiags_filtre <- tbl_mco(con, an, "rsa_diags") %>%
      dplyr::filter_(chaine_d) %>% dplyr::distinct(cle_rsa)
  }
  else {
    antidiags_filtre <- NULL
  }
  if (length(elements[["actes"]]) > 0) {
    if (length(elements[["activite_actes"]]) > 0){
      liste_actes = dplyr::data_frame(cdccam = elements$actes) %>%
        dplyr::tbl_df() %>% dplyr::copy_to(con, ., "acc",
                                           overwrite = T)
      actes_filtre = dplyr::inner_join(tbl_mco(con, an, "rsa_actes") %>% filter(act %in% elements[['activite_actes']]),
                                       dplyr::tbl(con, "acc"), by = c(cdccam = "cdccam")) %>%
        dplyr::distinct(cle_rsa)
    } else {
      liste_actes = dplyr::data_frame(cdccam = elements$actes) %>%
        dplyr::tbl_df() %>% dplyr::copy_to(con, ., "acc",
                                           overwrite = T)
      actes_filtre = dplyr::inner_join(tbl_mco(con, an, "rsa_actes"),
                                       dplyr::tbl(con, "acc"), by = c(cdccam = "cdccam")) %>%
        dplyr::distinct(cle_rsa)
    }
  }
  else {
    actes_filtre = NULL
  }
  resultat <- rsa_filtre
  if (length(elements[["diags"]]) > 0) {
    resultat <- resultat %>% dplyr::inner_join(diags_filtre,
                                               by = "cle_rsa")
  }
  if (length(elements[["diags_exclus"]]) > 0) {
    resultat <- resultat %>% dplyr::anti_join(antidiags_filtre,
                                              by = "cle_rsa")
  }
  
  if (length(elements[["actes"]]) > 0) {
    resultat <- resultat %>% dplyr::inner_join(actes_filtre,
                                               by = "cle_rsa")
  }
  if (is.null(vars)) {
    return(resultat %>% dplyr::collect())
  }
  else {
    return(resultat %>% dplyr::inner_join(tbl_mco(con,
                                                           an, "rsa_rsa") %>% dplyr::select(cle_rsa, vars),
                                          by = "cle_rsa") %>% dplyr::collect())
  }
}

#' ~ req : lancer une ou plusieurs requetes dans une db avec une ou des listes
#'
#'
#' @examples
#' \dontrun{
#' get_all_listes('Recours Exceptionnel') -> listes_re
#' lancer_requete_db(con, an, listes_re)
#'
#' get_liste('chir_bariatrique_total') -> liste_bari
#' lancer_requete_db(con, an, liste_bari)
#' }
#'
#' @return un tibble concatenant les resultats de toutes les requetes : les rsa qui ont un acte de la liste, un diag, une duree correspondante, etc.
#' @author G. Pressiat
#' @importFrom dplyr mutate bind_rows
#' @export
lancer_requete_db <- function(con, an, elements, vars = NULL){
  if (length(elements$nom) == 1){
    cat(elements$nom, "\n")
    return(elements %>%
             requete_db(con, an, ., vars) %>%
             dplyr::mutate(Requete = elements$nom,
                           Thematique = elements$thematique))}
  else {
    return(dplyr::bind_rows(lapply(elements,
                                   function(elements1){
                                     cat(elements1$nom, "\n")
                                     elements1 %>%
                                       requete_db(con, an, ., vars) %>%
                                       dplyr::mutate(Requete = elements1$nom,
                                                     Thematique = elements1$thematique)})))}
}

#' ~ req : lancer une ou plusieurs requetes avec une ou des listes
#'
#'
#' @examples
#' \dontrun{
#' prepare_rsa(rsa) -> rsa
#' get_all_listes('Recours Exceptionnel') -> listes_re
#' lancer_requete(rsa, listes_re)
#'
#' get_liste('chir_bariatrique_total') -> liste_bari
#' lancer_requete(rsa, liste_bari)
#' }
#'
#' @return un tibble concatenant les resultats de toutes les requetes : les rsa qui ont un acte de la liste, un diag, une duree correspondante, etc.
#' @author G. Pressiat
#' @importFrom dplyr mutate bind_rows
#' @export
lancer_requete <- function(tables, elements, vars = NULL){
  if (length(elements$nom) == 1){
    cat(elements$nom, "\n")
    return(elements %>%
             requete(tables, ., vars) %>%
             dplyr::mutate(Requete = elements$nom,
                           Thematique = elements$thematique))}
  else {
    return(dplyr::bind_rows(lapply(elements,
                                   function(elements1){
                                     cat(elements1$nom, "\n")
                                     elements1 %>%
                                       requete(tables, ., vars) %>%
                                       dplyr::mutate(Requete = elements1$nom,
                                                     Thematique = elements1$thematique)})))}
}

#' ~ req : preparer les rsa pour la requete
#'
#' On selectionne certaines variables et on en cree d'autres utiles lors de l'execution de la requete
#'
#' @examples
#' \dontrun{
#' tab <- irsa(typi = 6)
#' prepare_rsa(tab)
#' }
#'
#' @return un objet de classe rsa de pmeasyr
#' @author G. Pressiat
#' @importFrom dplyr select mutate
#' @importFrom tidyr unite
#' @export
prepare_rsa <- function(rsa){
  
  
  rsa$rsa <- rsa$rsa %>%
    # tidyr::unite(ghm, RSACMD, RSATYPE, RSANUM, RSACOMPX, sep = "") %>%
    #dplyr::select(NOFINESS, CLE_RSA, NOSEQRUM, MOISSOR, ghm, DP, DR, ECHPMSI, PROV, DEST, SCHPMSI, AGEAN,POIDS, AGEJR, dpdrum, DUREE, das, actes, um) %>%
    dplyr::mutate(diags = paste(dpdrum, das),
                  agean = dplyr::if_else(is.na(agean), as.numeric(agejr)/365.25, as.numeric(agean))) %>%
    dplyr::mutate(rsatype = substr(ghm, 3,3))
  
  rsa <- tdiag(rsa)
  # sjlabelled::remove_all_labels(rsa$rsa) -> rsa$rsa
  # sjlabelled::remove_all_labels(rsa$actes) -> rsa$actes
  # sjlabelled::remove_all_labels(rsa$diags) -> rsa$diags
  # sjlabelled::remove_all_labels(rsa$rsa_um) -> rsa$rsa_um
  return(rsa)
}


#' ~ req : collecter les rsa présents dans une db
#'
#' On selectionne certaines variables et on en cree d'autres utiles lors de l'execution de la requete
#'
#' @param con Connexion à la base de données
#' @param an année des rsa (ex: 17)
#' @param n Nombre de lignes à importer (équivalent `head`)
#' @examples
#' \dontrun{
#' rsa <- collect_rsa_from_db(con, 16, n = 1e5)
#' }
#'
#' @return un liste rsa similaire à un import de pmeasyr
#' @author G. Pressiat
#' @export
collect_rsa_from_db <- function(con, an, n = Inf){
  rsa <- list()
  to_up <- function(table){
    names(table) <- toupper(names(table))
    table
  }
  # %>% to_up() %>% rename(ghm = GHM,
  # dpdrum = DPDRUM,
  # das = DAS,
  # actes = ACTES, um = UM)
  
  tbl_mco(con, an, 'rsa_rsa') %>% dplyr::collect(n = n)   -> rsa$rsa
  
  tbl_mco(con, an, 'rsa_actes') %>% dplyr::collect(n = n)   -> rsa$actes
  tbl_mco(con, an, 'rsa_diags') %>% dplyr::filter(position == 5) %>% dplyr::collect(n = n)   %>% dplyr::rename(DAS = DIAG) -> rsa$das
  tbl_mco(con, an, 'rsa_um') %>% dplyr::collect(n = n)  -> rsa$rsa_um
  
  rsa
}

#' ~ req : creer un json pour partager une liste
#'
#' On selectionne certaines variables et on en cree d'autres utiles lors de l'execution de la requete
#'
#' @param requete `list` qui contient la requête
#' @param chemin Nom du fichier json qui sera créé
#' @examples
#' \dontrun{
#' liste = list(nom_abrege = "pac",
#'              thematique = "test",
#'              actes = c('EBLA003', 'EQLF002'),
#'              dureemax = 0,
#'              auteur = "John Doe",
#'              date_saisie = "1960-01-01")
#' creer_json(liste, paste0('~/Documents/listes/', liste$nom_abrege, '.json'))
#' }
#'
#' @return un objet de classe rsa de pmeasyr
#' @author G. Pressiat
#' @importFrom jsonlite write_json
#' @export
creer_json <- function(requete, chemin){
  jsonlite::write_json(requete, chemin, method='C', pretty = T)
}
