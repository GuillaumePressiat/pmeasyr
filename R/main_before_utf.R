
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param typi Type d'import, par defaut a 3, a 0 : propose a l'utilisateur de choisir au lancement
#' @param ~...   parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une classe S3 contenant les tables (data.frame, tibble) importées (rum, actes, das et dad si import 3 et 4)
#'
#' @examples
#' \dontrun{
#'    irum(750712184,2015,12,'~/Documents/data/mco', typi = 1) -> rum15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}}, \code{\link{ileg_mco}}, \code{\link{iano_mco}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @importFrom utils View data unzip modifyList
#' @importFrom magrittr '%>%'
#' @export irum
#' @usage irum(finess, annee, mois, path, lib = T, typi = 3, ...)
#' @export
irum <- function(...){
  UseMethod('irum')
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
irum.default <- function(finess, annee, mois, path, lib = T, typi = 3, ...){
  if (annee<2011|annee>2017){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  if (!(typi %in% 0:4)){
    stop("Type d'import incorrect : 0 ou 1, 2, 3 et 4\n")
  }
  
  #op <- options(digits.secs = 6)
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
           e = readr::col_euro_double(),
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
                               na=character(), ...) %>%
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
          AGEGEST       = stringr::str_sub(RUM,110,111),
          DDR2          = ifelse(NOVERG=='115', ""                            , stringr::str_sub(RUM,112,119)),
          NBSEAN        = ifelse(NOVERG=='115', stringr::str_sub(RUM,112,113) , stringr::str_sub(RUM,120,121)),
          NBDAS         = ifelse(NOVERG=='115', stringr::str_sub(RUM,114,115) , stringr::str_sub(RUM,122,123)) %>% as.integer(),
          NBDAD         = ifelse(NOVERG=='115', stringr::str_sub(RUM,116,117) , stringr::str_sub(RUM,124,125)) %>% as.integer(),
          NBACTE        = ifelse(NOVERG=='115', stringr::str_sub(RUM,118,120) , stringr::str_sub(RUM,126,128)) %>% as.integer(),
          DP            = ifelse(NOVERG=='115', stringr::str_sub(RUM,121,128) , stringr::str_sub(RUM,129,137)),
          DR            = ifelse(NOVERG=='115', stringr::str_sub(RUM,129,136) , stringr::str_sub(RUM,138,146)),
          IGS           = ifelse(NOVERG=='115', stringr::str_sub(RUM,137,138) , stringr::str_sub(RUM,147,149)),
          CONFCDRSS     = ifelse(NOVERG=='115', stringr::str_sub(RUM,139,140) , stringr::str_sub(RUM,150,150)),
          RDT_TYPMACH   = ifelse(NOVERG=='115', stringr::str_sub(RUM,141,141) , stringr::str_sub(RUM,151,151)),
          RDT_TYPDOSIM  = ifelse(NOVERG=='115', stringr::str_sub(RUM,142,142) , stringr::str_sub(RUM,152,152)),
          NBFAISC       = ifelse(NOVERG=='115', stringr::str_sub(RUM,143,143) , stringr::str_sub(RUM,153,153)),
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
      DR    = stringr::str_trim(DR))
  }
  if (typi== 1){
    
    Fillers <- names(rum_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    
    rum_i <- rum_i[,!(names(rum_i) %in% Fillers)]
    # Libelles
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      rum_i <- rum_i  %>% dplyr::select(-ZAD) %>%  sjmisc::set_label(v)
    }
    
    rum_1 <- list(rum = rum_i )
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
      v <- c(v[!is.na(v)],c("Stream Actes","Stream Das", "Stream Dad"))
      rum_i <- rum_i %>%  sjmisc::set_label(v)
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
      actes %>% sjmisc::set_label(c('N° administratif du séjour','N° du RUM', fa$libelle)) -> actes
      das %>% sjmisc::set_label(c('N° administratif du séjour','N° du RUM', 'Diagnostic associé')) -> das
      dad %>% sjmisc::set_label(c('N° administratif du séjour','N° du RUM', 'Donnée à visée documentaire')) -> dad
    }
    
    
    
    Fillers <- names(rum_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    
    rum_i <- rum_i[,!(names(rum_i) %in% Fillers)]
    # Libelles
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      rum_i <- rum_i  %>% dplyr::select(-ZAD, -ldad, -lactes, -ldas) %>%  sjmisc::set_label(v)
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
      actes %>% sjmisc::set_label(c('N° administratif du séjour','N° du RUM', fa$libelle)) -> actes
      das %>% sjmisc::set_label(c('N° administratif du séjour','N° du RUM', 'Diagnostic associé')) -> das
      dad %>% sjmisc::set_label(c('N° administratif du séjour','N° du RUM', 'Donnée à visée documentaire')) -> dad
    }
    
    
    rum_i <- rum_i %>%
      dplyr::mutate(das = stringr::str_replace_all(das, "\\s{1,},", ","),
                    dad = stringr::str_replace_all(dad, "\\s{1,},", ","))
    
    
    Fillers <- names(rum_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    
    rum_i <- rum_i[,!(names(rum_i) %in% Fillers)]
    # Libelles
    if (lib==T){
      v <- libelles
      v <- c(v[!is.na(v)],c("Stream Actes","Stream Das", "Stream Dad"))
      rum_i <- rum_i  %>% dplyr::select(-ZAD, -ldad, -lactes, -ldas) %>%  sjmisc::set_label(v)
    }
    rum_1 <- list(rum = rum_i, actes = actes, das = das, dad = dad)
    class(rum_1) <- append(class(rum_1),"RUM")
    deux<-Sys.time()
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param typi Type d'import, par defaut a 4, a 0 : propose a l'utilisateur de choisir au lancement
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une classe S3 contenant les tables (data.frame, tbl_df ou tbl) importées  (rsa, rsa_um, actes et das si import > 3)
#'
#' @examples
#' \dontrun{
#'    irsa(750712184,2015,12,'~/Documents/data/mco') -> rsa15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{ileg_mco}}, \code{\link{iano_mco}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irsa(finess, annee, mois, path, lib = T, typi = 4, ...)
#' @export irsa
#' @export
irsa <- function(...){
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
irsa.default <- function(finess, annee, mois, path, lib = T, typi = 4, ...){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
                           readr::fwf_widths(af,an), col_types =at, na=character(), ... ) %>%
      dplyr::mutate(DP = stringr::str_trim(DP),
                    DR = stringr::str_trim(DR))
  }
  
  if (typi== 1){
    deux<-Sys.time()
    #cat(paste("MCO RSA Light",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("(Seule la partie fixe du RSA a été chargée)\n")
    Fillers <- names(rsa_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="FIL"]
    rsa_i <- rsa_i[,!(names(rsa_i) %in% Fillers)]
    # Libelles
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      rsa_i <- rsa_i  %>% dplyr::select(-ZA) %>%  sjmisc::set_label(v)
    }
    
    rsa_1 <- list(rsa = rsa_i)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
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
        v <- c(v[!is.na(v)],c("Supp. Radiothérapies", "Stream Actes", "Stream Das"))
      }
      else {
        v <- c(v[!is.na(v)],c("Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes", "Stream Das"))
      }
      rsa_i <- rsa_i %>%  sjmisc::set_label(v)
    }
    rsa_1 <- list(rsa = rsa_i)
    
    
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    deux<-Sys.time()
   #cat(paste("MCO RSA Light+",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    
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
        v <- c(v[!is.na(v)],c("Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das"))
      }
      else{
        v <-   c(v[!is.na(v)],c("Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das"))
      }
      rsa_i <- rsa_i %>%  sjmisc::set_label(v)
    }
    
    rsa_1 <- list(rsa = rsa_i)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    #cat(paste("MCO RSA Light++",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("La table rsa est dans l'environnement de travail\n")
    
    
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
        rsa_um %>% sjmisc::set_label(c('Clé RSA','N° Séquentiel du RUM', fa$libelle)) -> rsa_um}
      else
        rsa_um %>% sjmisc::set_label(c('Clé RSA', fa$libelle)) -> rsa_um
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
      das %>% sjmisc::set_label(c('Clé RSA', 'N° séquentiel du RUM',  'Diagnostic associé')) -> das
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
      actes %>% sjmisc::set_label(c('Clé RSA', 'N° séquentiel du RUM', fa$libelle)) -> actes
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
        v <- c(v[!is.na(v)],c("Supp. Radiothérapies"))
      }
      else {
        v <- c(v[!is.na(v)],c("Types Aut. à Portée Globale", "Supp. Radiothérapies"))
      }
      rsa_i <- rsa_i %>%  sjmisc::set_label(v)
    }
    
    rsa_1 <- list(rsa = rsa_i,
                  actes = actes,
                  das = das,
                  rsa_um=rsa_um)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    #cat(paste("MCO RSA Standard",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    
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
        rsa_um %>% sjmisc::set_label(c('Clé RSA','N° Séquentiel du RUM', fa$libelle)) -> rsa_um}
      else
        rsa_um %>% sjmisc::set_label(c('Clé RSA', fa$libelle)) -> rsa_um
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
      das %>% sjmisc::set_label(c('Clé RSA', 'N° séquentiel du RUM',  'Diagnostic associé')) -> das
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
      actes %>% sjmisc::set_label(c('Clé RSA', 'N° séquentiel du RUM', fa$libelle)) -> actes
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
        v <- c(v[!is.na(v)],c("Supp. Radiothérapies", "Stream Actes", "Stream Das"))
      }
      else{
        v <-  c(v[!is.na(v)],c("Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes", "Stream Das"))
      }
      
      rsa_i <- rsa_i %>%  sjmisc::set_label(v)
    }
    
    rsa_1 <- list(rsa = rsa_i , actes = actes, das = das, rsa_um=rsa_um)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    #cat(paste("MCO RSA Standard+",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("Les tables rsa, acdi et rsa_um sont dans l'environnement de travail\n")
    
    
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
        rsa_um %>% sjmisc::set_label(c('Clé RSA','N° Séquentiel du RUM', fa$libelle)) -> rsa_um}
      else
        rsa_um %>% sjmisc::set_label(c('Clé RSA', fa$libelle)) -> rsa_um
    }
    deux_i<-Sys.time()
    #cat(round(difftime(deux_i,un_i, units="secs"),0), "secondes\n")
    
    # das
    #cat("Das en ligne : ")
    un_i<-Sys.time() %>% stringr::str_trim()
    das <- purrr::flatten_chr(rsa_i$ldas)
    df <- rsa_um %>% dplyr::select(CLE_RSA,NSEQRUM,NBDIAGAS)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGAS), stringsAsFactors = F) %>% dplyr::tbl_df()
    das <- dplyr::bind_cols(df,data.frame(DAS = das, stringsAsFactors = F) ) %>% dplyr::tbl_df()
    das <- das %>% dplyr::select(-NBDIAGAS)
    if (lib == T){
      das %>% sjmisc::set_label(c('Clé RSA', 'N° séquentiel du RUM',  'Diagnostic associé')) -> das
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
      actes %>% sjmisc::set_label(c('Clé RSA', 'N° séquentiel du RUM', fa$libelle)) -> actes
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
        v <- c(v[!is.na(v)],c("Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das"))
      }
      else{
        v <-  c(v[!is.na(v)],c("Types Aut. à Portée Globale", "Supp. Radiothérapies", "Stream Actes","Parcours Typaut UM","Stream DP/DR des UM","Stream Das"))
      }
      rsa_i <- rsa_i %>%  sjmisc::set_label(v)
      
    }
    
    rsa_1 <- list(rsa = rsa_i , actes = actes, das = das, rsa_um = rsa_um)
    class(rsa_1) <- append(class(rsa_1),"RSA")
    
    #cat(paste("MCO RSA Standard++",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
    #cat("Les tables rsa, acdi et rsa_um sont dans l'environnement de travail\n")
    
    
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param champ Champ PMSI du TRA a integrer ("mco", "ssr", "had", "tra_psy_rpsa", ", "tra_psy_r3a"), par defaut "mco"
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premières lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame ou tbl_df) qui contient : - Clé RSA - NORSS - Numéro de ligne du fichier RSS d'origine (rss.ini) - NAS - Date d'entrée du séjour - GHM groupage du RSS (origine) - Date de sortie du séjour
#'
#' @examples
#' \dontrun{
#'    itra(750712184,2015,12,'~/Documents/data/champ_pmsi') -> tra15
#' }
#'
#' @author G. Pressiat
#'
#' @usage irum(finess, annee, mois, path, lib = T, champ = "mco")
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
itra.default <- function(finess, annee, mois, path, lib = T, champ= "mco",... ){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(DTENT  = lubridate::dmy(DTENT),
                    DTSORT = lubridate::dmy(DTSORT),
                    NOHOP = paste0("000",stringr::str_sub(NAS,1,2)))
  }
  if (champ=="had"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
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
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(NOHOP = paste0("000",stringr::str_sub(NAS,1,2)))
  }
  if (champ=="tra_psy_rpsa"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra.txt"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(NOHOP = paste0("000",stringr::str_sub(NAS,1,2)),
                    DTENT  = lubridate::dmy(DTENT),
                    DTSORT = lubridate::dmy(DTSORT),
                    DT_DEB_SEQ = lubridate::dmy(DT_DEB_SEQ),
                    DT_FIN_SEQ = lubridate::dmy(DT_FIN_SEQ))
  }
  if (champ=="tra_psy_r3a"){
    tra_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".tra.raa.txt"),
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(DTACTE  = lubridate::dmy(DTACTE),
                    DTACTE_2  = lubridate::dmy(DTACTE_2))
  }
  
  if (lib==T & champ !="tra_psy_r3a"){
    v <- c(libelles, 'Établissement')
    return(tra_i  %>%  sjmisc::set_label(v))
  }
  
  if (lib==T & champ =="tra_psy_r3a"){
    v <- libelles
    return(tra_i  %>%  sjmisc::set_label(v))
  }
  return(NULL)
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame ou tbl_df) qui contient les données Anohosp in / out
#'
#' @examples
#' \dontrun{
#'    iano_mco(750712184,2015,12,'~/Documents/data/mco') -> ano_out15
#'    iano_mco(750712184,2015,12,'~/Documents/data/mco', typano = "in") -> ano_in15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @export iano_mco
#' @usage iano_mco(finess, annee, mois, path, lib = T, typano = "out")
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
iano_mco.default <- function(finess, annee, mois, path, typano = c("out", "in"), lib = T, ...){
  if (annee<2011|annee>2017){
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
             e = readr::col_euro_double(),
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
      suppressWarnings(ano_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                                              readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
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
                                       MTMALPAR = MTMALPAR/100) )
      
      
    }
    if (2011<annee & annee<2013){
      suppressWarnings(ano_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                                              readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
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
                                       MTMALPAR = MTMALPAR/100) )
      
    }
    if (annee == 2011){
      suppressWarnings( ano_i<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                                               readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
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
      )
    }
    
    Fillers <- names(ano_i)
    Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
    ano_i <- ano_i[,!(names(ano_i) %in% Fillers)]
    
    if (lib==T){
      v <- c(libelles[!is.na(libelles)], "Chaînage Ok")
      ano_i <- ano_i  %>%  sjmisc::set_label(v)
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
             e = readr::col_euro_double(),
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
                             readr::fwf_widths(af,an), col_types =at, na=character(), ...)  %>%
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
                             readr::fwf_widths(af,an), col_types =at, na=character(), ...)  %>%
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
      ano_i <- ano_i  %>%  sjmisc::set_label(v)
    }
  }
  
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
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
#'    imed_mco(750712184,2015,12,'~/Documents/data/mco') -> med_out15
#'    imed_mco(750712184,2015,12,'~/Documents/data/mco', typmed = "in") -> med_in15
#' }
#'
#' @author G. Pressiat
#'
#' @usage imed_mco(finess, annee, mois, path, lib = T, typmed = c('out', 'in'))
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
imed_mco.default <- function(finess, annee, mois, path, typmed = c("out", "in"), lib = T, ...){
  if (annee<2011|annee>2017){
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
             e = readr::col_euro_double(),
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
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(NBADM = NBADM/1000,
                    PRIX =  PRIX /1000)
    
    
    info = file.info(paste0(path,"/",finess,".",annee,".",mois,".medatu"))
    if (info$size >0 & !is.na(info$size)){
      med_i2<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".medatu"),
                              readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
        dplyr::mutate(NBADM = NBADM/1000,
                      PRIX =  PRIX /1000)
      med_i <- rbind(med_i,med_i2)
    }
    
    info = file.info(paste0(path,"/",finess,".",annee,".",mois,".medthrombo"))
    if (info$size >0 & !is.na(info$size)){
      med_i3<-readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".medthrombo"),
                              readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
        dplyr::mutate(NBADM = NBADM/1000,
                      PRIX =  PRIX /1000)
      med_i <- rbind(med_i,med_i3)
    }
    if (lib==T){
      v <- libelles
      med_i <- med_i  %>%  sjmisc::set_label(v)
    }
    return( med_i  )
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
             e = readr::col_euro_double(),
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
                             readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
        dplyr::mutate(NBADM = NBADM/1000,
                      PRIX =  PRIX /1000)
    }
    if (lib==T){
      v <- libelles
      v<- v[!is.na(v)]
      med_i <- med_i %>% dplyr::select(-Fil1) %>%  sjmisc::set_label(v)
    }
    return(med_i %>% dplyr::mutate(DTDISP = lubridate::dmy(DTDISP)) )
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les dispositifs médicaux implantables In ou Out (T2A, ATU et thrombo selon l'existence des fichiers : si le fichier n'existe pas, pas de donnée importée). Pour discriminer le type de prestation, la colonne TYPEPREST donne l'information : T2A 06 - ATU 09 - THROMBO 10
#'
#' @examples
#' \dontrun{
#'    idmi_mco(750712184,2015,12,'~/Documents/data/mco') -> dmi_out15
#'    idmi_mco(750712184,2015,12,'~/Documents/data/mco', typdmi = "in") -> dmi_in15
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
  do.call(idmi_mcoo.default, param2)
}


#' @export
idmi_mco.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(idmi_mco.default, param2)
}

#' @export
idmi_mco.default <- function(finess, annee, mois, path, typdmi = c("out", "in"), lib = T, ...){
  if (annee<2011|annee>2017){
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
             e = readr::col_euro_double(),
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
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(PRIX   =  PRIX /1000)
    
    
    if (lib==T){
      v <- libelles
      dmi_i <- dmi_i  %>%  sjmisc::set_label(v)
    }
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
             e = readr::col_euro_double(),
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
                           readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(PRIX   =  PRIX /1000,
                    DTPOSE = lubridate::dmy(DTPOSE))
    
    
    if (lib==T){
      v <- libelles
      v <- v[!is.na(v)]
      dmi_i <- dmi_i  %>% dplyr::select(-Fil1,-Fil2) %>%  sjmisc::set_label(v)
    }
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
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les erreurs Out.
#'
#' @examples
#' \dontrun{
#'    ileg_mco(750712184,2015,12,'~/Documents/data/mco') -> leg15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @export ileg_mco
#' @usage ileg_mco(finess, annee, mois, path, reshape = F, ...)
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
ileg_mco.default <- function(finess, annee, mois, path, reshape = F, ...){
  
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
    return(leg_i1)
  }
  
  leg_i1 %>% 
    dplyr::group_by(FINESS, MOIS, ANNEE, CLE_RSA, NBERR) %>%
    dplyr::summarise(EG = paste(EG, collapse = ", ")) -> leg_i1
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
#'    med <- imed_mco(750712184,2015,12,"~/Documents/data/mco","out")
#'    tra <- itra(750712184,2015,12,"~/Documents/data/mco")
#'    med <- inner_tra(med,tra)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}}, \code{\link{imed_mco}}, \code{\link{irpsa}}, \code{\link{irha}}, \code{\link{irapss}}

#' @export
inner_tra <- function(table, tra, sel = 1, champ = "mco"){
  if (champ == "mco"){
    if (sel==1){
      return( suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(-NO_ligne_RSS,-DTENT,-GHM1,-DTSORT))))
    }
    if (sel==2){
      return( suppressMessages( dplyr::inner_join(table, tra )))
    }
  }
  if (champ == "had"){
    if (sel==1){
      return( suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(-NIP,-DTNAI, -DTENT, -DTSORT,-ECHPMSI,
                                                                               -PROV,-SCHPMSI, -DEST, -DT_DEB_SEQ,-DT_FIN_SEQ,
                                                                               -DT_DEB_SS_SEQ,-DT_FIN_SS_SEQ,-DERNIERE_SS_SEQ))))
    }
    if (sel==2){
      return( suppressMessages( dplyr::inner_join(table, tra)))
    }
  }
  if (champ == "ssr"){
    if (sel==1){
      return(suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(-NOSEJ2,-NOSEMAINE, -NOLIGNE_RHS))))
    }
    if (sel==2){
      return(suppressMessages(  dplyr::inner_join(table, tra)))
    }
  }
  if (champ == "psyrpsa"){
    if (sel==1){
      return(suppressMessages( dplyr::inner_join(table, tra %>% dplyr::select(-IPP,-DTENT,-DTENT2,-DTSORT,
                                                                              -DT_DEB_SEQ,-DT_FIN_SEQ),
                                                 by=c('NOSEJPSY'='NOSEQSEJ','NOSEQ'='NOSEQ'))))
    }
    if (sel==2){
      return(suppressMessages(  dplyr::inner_join(table, tra, by=c('NOSEJPSY'='NOSEQSEJ','NOSEQ'='NOSEQ'))))
    }
  }
  if (champ == "psyr3a"){
    if (sel==1){
      return(suppressMessages( dplyr::inner_join(table, tra, by=c('NOORDR'='NOORDR'))))
    }
    if (sel==2){
      return(suppressMessages(  dplyr::inner_join(table, tra, by=c('NOORDR'='NOORDR'))))
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les dialyses péritonéales In ou Out.
#'
#' @examples
#' \dontrun{
#'    idiap <- idiap(750712184,2015,12,"~/Documents/data/mco")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}}
#' @usage idiap(finess, annee, mois, path, typdiap = c("out", "in"), lib = T, ...)
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
idiap.default <- function(finess, annee, mois, path, typdiap = c("out", "in"), lib = T, ...){
  if (annee<2011|annee>2017){
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
             e = readr::col_euro_double(),
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
    
    
    if (lib==T){
      v <- libelles
      diap_i <- diap_i  %>%  sjmisc::set_label(v)
    }
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
             e = readr::col_euro_double(),
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
                            readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
      dplyr::mutate(DTDEBUT = lubridate::dmy(DTDEBUT))
    
    
    if (lib==T){
      
      v <- libelles
      diap_i <- diap_i  %>%  sjmisc::set_label(v)
    }
    return(diap_i)
  }
  
}



#' ~ MCO - Import des donnees UM du Out
#'
#' Imports du fichier IUM
#'
#' Formats depuis 2011 pris en charge
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires à passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les informations structures du Out.
#'
#' @examples
#' \dontrun{
#'    um <- iium(750712184,2015,12,"~/Documents/data/mco")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iium(finess, annee, mois, path, lib = T, ...)
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
iium.default <- function(finess, annee, mois, path, lib = T, ...){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
                         readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
    dplyr::mutate(DTEAUT = lubridate::dmy(DTEAUT))
  
  
  if (lib==T){
    v <- libelles
    ium_i <- ium_i  %>%  sjmisc::set_label(v)
  }
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#' @return Une table (data.frame, tibble) contenant les prélèvements d'organes In ou Out.
#'
#' @examples
#' \dontrun{
#'    po <- ipo(750712184,2015,12,"~/Documents/data/mco")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irum}}, \code{\link{irsa}}, utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ipo(finess, annee, mois, path, typpo = c("out", "in"), lib = T, ...)
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
ipo.default <- function(finess, annee, mois, path, typpo = c("out", "in"), lib = T, ...){
if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
  
  
  if (lib==T){
    v <- libelles
    po_i <- po_i  %>%  sjmisc::set_label(v)
  }
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
           e = readr::col_euro_double(),
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
                        readr::fwf_widths(af,an), col_types =at, na=character(), ...) %>%
    dplyr::mutate(DTDEBUT = lubridate::dmy(DTDEBUT))
  
  
  if (lib==T){
    v <- libelles
    po_i <- po_i  %>%  sjmisc::set_label(v)
  }
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
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une classe S3 contenant les tables (data.frame, tbl_df ou tbl) importées (rapss, acdi, ght).
#'
#' @examples
#' \dontrun{
#'    um <- iium(750712184,2015,12,"~/Documents/data/had")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{iano_had}}, \code{\link{ileg_had}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irapss(finess, annee, mois, path, lib = T, ...)
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
irapss.default <- function(finess, annee, mois, path, lib = T, ...){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
    df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,NBZA)
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
    df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    da <- dplyr::mutate(da, CODE = 'DA') %>% dplyr::select(-NBDA)
    
    dmpp <- purrr::flatten_chr(rapss_i$ldmpp)
    df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,NBDIAGMPP)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGMPP), stringsAsFactors = F) %>% dplyr::tbl_df()
    dmpp <- dplyr::bind_cols(df,data.frame(DMPP = stringr::str_trim(dmpp), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    dmpp <- dplyr::mutate(dmpp, CODE = 'DMPP') %>% dplyr::select(-NBDIAGMPP)
    
    dmpa <- purrr::flatten_chr(rapss_i$ldmpa)
    df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,NBDIAGMPA)
    df <- as.data.frame(lapply(df, rep, df$NBDIAGMPA), stringsAsFactors = F) %>% dplyr::tbl_df()
    dmpa <- dplyr::bind_cols(df,data.frame(DMPA = stringr::str_trim(dmpa), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    dmpa <- dplyr::mutate(dmpa, CODE = 'DMPA') %>% dplyr::select(-NBDIAGMPA)
    
    acdi <- dplyr::bind_rows(actes,da,dmpp,dmpa) %>% dplyr::select(-ZACTES)
    rapss_i <- rapss_i %>% dplyr::select(-c(FILLER,Z,da,za,dmpp,dmpa,lda,ldmpp,ldmpa,lactes))
    
  }
  
  if (annee==2011){
    actes <- purrr::flatten_chr(rapss_i$lactes)
    df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,NBZA)
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
    df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,NBDA)
    df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
    da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df()
    da <- dplyr::mutate(da, CODE = 'DA') %>% dplyr::select(-NBDA)
    
    acdi <- dplyr::bind_rows(actes,da) %>% dplyr::select(-ZACTES)
    rapss_i <- rapss_i %>% dplyr::select(-c(Z,da,za,lda,lactes))
  }
  
  etb_ght <- purrr::flatten_chr(rapss_i$letb_ght)
  df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,NOVRPSS, VCLASS = ETB_VCLASS, CDRETR = ETB_CDRETR,
                                  GHPC = ETB_GHPC, NBGHT = ETB_NBGHT)
  df <- as.data.frame(lapply(df, rep, df$NBGHT), stringsAsFactors = F) %>% dplyr::tbl_df()
  etb_ght <- dplyr::bind_cols(df,data.frame(etb_ght, stringsAsFactors = F) ) %>% dplyr::tbl_df()  %>%
    dplyr::mutate(TYPGHT='ETAB',
                  NUMGHT = stringr::str_sub(etb_ght,1,2),
                  JOURSGHT = stringr::str_sub(etb_ght,3,5) %>% as.numeric() ) %>%
    dplyr::select(-etb_ght)
  
  pap_ght <- purrr::flatten_chr(rapss_i$lpap_ght)
  df <- rapss_i %>% dplyr::select(NOSEJHAD,NOSEQ,NOSOUSSEQ,VCLASS=PAPRICA_VCLASS, CDRETR = PAPRICA_CDRETR, GHPC = PAPRICA_GHPC,
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
    
    ght <- ght %>% sjmisc::set_label(c('N° du séjour HAD', 'N° de la séquence', 'N° de la sous-séquence',
                                       'N° de version du RPSS','Version de classification',
                                       'Codes retours', 'Groupe homogène de prise en charge',
                                       'Nombre de GHT','Type de GHT', 'N° du GHT', 'Nombre de jours du GHT'))
    
    if (annee==2011){
      acdi <- acdi %>% sjmisc::set_label(c('N° du séjour HAD', 'N° de la séquence', 'N° de la sous-séquence',
                                           'Type de code (A : Acte, DA : Diagnostic Associé)',
                                           "Délai depuis la date d'entrée","Code CCAM","Phase", "Activité", "Extension documentaire",
                                           "Nombre d'exécécutions", "Indic Validité de l'acte","Diagnostic Associé"))
    }else{
      acdi <- acdi %>% sjmisc::set_label(c('N° du séjour HAD', 'N° de la séquence', 'N° de la sous-séquence',
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
    rapss_i <- rapss_i  %>% sjmisc::set_label(libelles[!is.na(libelles)])
  }
  rapss_1 <- list(rapss = rapss_i, acdi = acdi, ght = ght)
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données Anohosp HAD du Out.
#'
#' @examples
#' \dontrun{
#'    anoh <- iano_had(750712184,2015,12,"~/Documents/data/had")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iano_had(finess, annee, mois, path, lib = T, ...)
#' @export iano_had
#' @export
iano_had <- function(...){
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
iano_had.default <- function(finess, annee,mois, path, lib=T, ...){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
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
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
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
  ano_i <- ano_i %>% sjmisc::set_label(c(libelles,'Chaînage Ok'))
  ano_i <- ano_i %>% dplyr::select(-dplyr::starts_with("Fill"))
  
  return(ano_i)
}

#' ~ HAD - Import des Med
#'
#' Imports du fichier Med Out
#'
#' Formats depuis 2011 pris en charge
#' Structure du nom du fichier attendu (sortie de Paprica) :
#' \emph{finess.annee.moisc.med}
#'
#' \strong{750712184.2016.2.med}
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données médicaments HAD du Out.
#'
#' @examples
#' \dontrun{
#'    medh <- imed_had(750712184,2015,12,"~/Documents/data/had")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}}
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage imed_had(finess, annee, mois, path, lib = T, ...)
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
imed_had.default <- function(finess, annee, mois, path, lib=T, ...){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
                           readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
    dplyr::mutate(NBADM = NBADM/1000,
                  PRIX  = PRIX /1000) %>% sjmisc::set_label(libelles)
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
#'
#' @return Une table (data.frame, tbl_df) contenant les erreurs Out.
#'
#' @examples
#' \dontrun{
#'    ileg_had(750712184,2015,12,'~/Documents/data/had') -> leg15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irapss}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ileg_had(finess, annee, mois, path, reshape = F, ...)
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
ileg_had.default <- function(finess, annee, mois, path, reshape = F, ...){
  
  leg_i <- readr::read_lines(paste0(path,"/",finess,".",annee,".",mois,".leg"))
  
  extz <- function(x,pat){unlist(lapply(stringr::str_extract_all(x,pat),toString) )}
  
  u <- stringr::str_split(leg_i, "\\;", simplify = T)
  leg_i1 <- dplyr::data_frame(FINESS    = u[,1] %>% as.character(),
                              MOIS      = u[,2] %>% as.character(),
                              ANNEE     = u[,3] %>% as.character(),
                              NOSEJHAD  = u[,4] %>% as.character(),
                              NOSEQ     = u[,5] %>% as.character(),
                              NOSOUSSEQ = u[,6] %>% as.character(),
                              NBERR     = u[,7] %>% as.integer())
  
  leg_i1 <- as.data.frame(lapply(leg_i1, rep, leg_i1$NBERR), stringsAsFactors = F)
  legs <- u[,8:ncol(u)]
  legs<- legs[legs != ""] 
  leg_i1 <- dplyr::bind_cols(leg_i1, data.frame(EG = as.character(legs), stringsAsFactors = F))
  
  if (reshape==T){
    return(leg_i1)
  }
  
  leg_i1 %>% 
    dplyr::group_by(FINESS, MOIS, ANNEE, NOSEJHAD, NOSEQ, NOSOUSSEQ, NBERR) %>%
    dplyr::summarise(EG = paste(EG, collapse = ", ")) -> leg_i1
  
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
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max=10e3} pour lire les 1000 premieres lignes
#'
#' @examples
#' \dontrun{
#'    irha(750712184,2015,12,'pathpath/') -> rha15
#' }
#' @seealso \code{\link{iano_ssr}}, \code{\link{ileg_ssr}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irha(finess, annee, mois, path, lib = T, ...)
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
irha.default <- function(finess, annee, mois, path, lib=T, ...){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
                                            readr::fwf_widths(af,an), col_types = at , na=character(), ...)) %>%
    dplyr::mutate(FPPC = stringr::str_trim(FPPC),
                  MMP = stringr::str_trim(MMP),
                  AE = stringr::str_trim(AE))
  
  if (annee >  2014){
    fzacte <- function(ccam){
      dplyr::mutate(ccam,
                    DELAI  = stringr::str_sub(ccam,1,4),
                    CDCCAM = stringr::str_sub(ccam,5,11),
                    DESCRI = stringr::str_sub(ccam, 12,13),
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
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA)
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CSARR') %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CCAM)","Diagnostic associé",
                   "Code CSARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Nb réel de patients", "Nb d'intervenants","Extension documentaire CSARR", "Code CCAM", "Partie descriptive","Phase CCAM",
                   "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjmisc::set_label(labelacdi)
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
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA)
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CSARR') %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CCAM)","Diagnostic associé",
                   "Code CSARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Nb réel de patients", "Nb d'intervenants","Extension documentaire CSARR", "Code CCAM", "Phase CCAM",
                   "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjmisc::set_label(labelacdi)
    
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
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA)
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCSARR)
    df <- as.data.frame(lapply(df, rep, df$NBCSARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBCSARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CDARR / CCAM)","Diagnostic associé",
                   "Code CSARR","Code CDARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Code CCAM", "Phase CCAM", "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjmisc::set_label(labelacdi)
    
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
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA)
    
    csarr <- purrr::flatten_chr(zad$lcsarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCDARR)
    df <- as.data.frame(lapply(df, rep, df$NBCDARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    csarr <- dplyr::bind_cols(df,data.frame(csarr = csarr, stringsAsFactors = F) )%>% dplyr::tbl_df() %>% dplyr::select(-NBCDARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CSARR / CDARR / CCAM)","Diagnostic associé",
                   "Code CSARR","Code CDARR", "Code supplémentaire appareillage", "Code modulateur de lieu", "Code modulateur patient n°1",
                   "Code modulateur patient n°2", "Code de l'intervenant", "Nb de patients en acte individuel",
                   "Nb de réalisations","Acte compatible avec la semaine", "Délai depuis la date d'entrée dans l'UM",
                   "Code CCAM", "Phase CCAM", "Activité CCAM", "Extension documentaire CCAM")
    acdi <- dplyr::bind_rows(da, fzsarr(csarr), fzacte(ccam))
    acdi <- acdi %>% sjmisc::set_label(labelacdi)
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
      dplyr::select(NOSEQSEJ, NOSEQRHS, CODE, DA)
    
    cdarr <- purrr::flatten_chr(zad$lcdarr)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCDARR)
    df <- as.data.frame(lapply(df, rep, df$NBCDARR), stringsAsFactors = F) %>% dplyr::tbl_df()
    cdarr <- dplyr::bind_cols(df,data.frame(cdarr = cdarr, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CDARR') %>% dplyr::select(-NBCDARR)
    
    
    ccam <- purrr::flatten_chr(zad$lccam)
    
    df <- rha_i %>% dplyr::select(NOSEQSEJ,NOSEQRHS,NBCCAM)
    df <- as.data.frame(lapply(df, rep, df$NBCCAM), stringsAsFactors = F) %>% dplyr::tbl_df()
    ccam <- dplyr::bind_cols(df,data.frame(ccam = ccam, stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::mutate(CODE='CCAM') %>% dplyr::select(-NBCCAM)
    
    acdi <-dplyr::bind_rows(da, fzdarr(cdarr), fzacte(ccam))
    labelacdi <- c('N° Séquentiel du séjour', 'N° Séquentiel du RHS',  "Type de code (DA / CDARR / CCAM)","Diagnostic associé",
                   "Code de l'intervenant", "Code CDARR", "Nb de réalisations","Acte compatible avec la semaine",
                   "Délai depuis la date d'entrée dans l'UM",
                   "Code CCAM", "Phase CCAM", "Activité CCAM", "Extension documentaire CCAM")
    
    acdi <- acdi %>% sjmisc::set_label(labelacdi)
    
  }
  
  
  acdi[is.na(acdi)] <- ""
  acdi$NBEXEC <- acdi$NBEXEC  %>%  as.numeric()
  acdi$DELAI <- acdi$DELAI  %>%  as.numeric()
  if (annee>2014){acdi$NBPATREEL <- acdi$NBPATREEL  %>%  as.numeric()}
  Fillers <- names(rha_i)
  Fillers <- Fillers[stringr::str_sub(Fillers,1,3)=="Fil"]
  
  rha_i <- rha_i[,!(names(rha_i) %in% Fillers)]
  
  rha_i <- rha_i   %>% dplyr::select(-ZAD) %>% sjmisc::set_label(libelles[!is.na(libelles)])
  
  rha_1 = list(rha = rha_i, acdi = acdi)
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... paramètres supplementaires à passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes, \code{progress = F, skip =...}
#'
#' @return Une table (data.frame, tbl_df) contenant les données Anohosp SSR du Out.
#'
#' @examples
#' \dontrun{
#'    anoh <- iano_ssr(750712184,2015,12,"~/Documents/data/ssr")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irha}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' 
#' @usage iano_ssr(finess, annee, mois, path, lib = T, ...)
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
iano_ssr.default <- function(finess, annee, mois, path, lib=T, ...){
  if (annee<2011|annee>2017){
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
           e = readr::col_euro_double(),
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
  
  if (annee>2012){
    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
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
    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano"),
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
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
  ano_i <- ano_i %>% sjmisc::set_label(c(libelles,'Chaînage Ok'))
  ano_i <- ano_i %>% dplyr::select(-dplyr::starts_with("FIL"))
  
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
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données SHA.
#'
#' @examples
#' \dontrun{
#'    sha <- issrha(750712184,2015,12,"~/Documents/data/ssr")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irha}}, \code{\link{ileg_ssr}}, \code{\link{iano_ssr}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage issrha(finess, annee, mois, path, lib = T, ...)
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
issrha.default <- function(finess, annee,mois, path, lib=T, ...){
  if (annee<2011|annee>2016){
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
           e = readr::col_euro_double(),
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
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...) %>%
    sjmisc::set_label(libelles)
  
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
#'
#' @return Une table (data.frame, tbl_df) contenant les erreurs Out.
#'
#' @examples
#' \dontrun{
#'    ileg_had(750712184,2015,12,'~/Documents/data/ssr') -> leg15
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irha}}, \code{\link{issrha}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ileg_ssr(finess, annee, mois, path, reshape = F, ...)
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
ileg_ssr.default <- function(finess, annee, mois, path, reshape = F, ...){
  
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
    return(leg_i1)
  }
  
  leg_i1 %>% 
    dplyr::group_by(FINESS, MOIS, ANNEE, NOSEQSEJ, NOSEQRHS, NBERR) %>%
    dplyr::summarise(EG = paste(EG, collapse = ", ")) -> leg_i1
  
  return(dplyr::ungroup(leg_i1))
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données RPSA.
#'
#' @examples
#' \dontrun{
#'    rpsa <- irpsa(750712184,2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{ir3a}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irpsa(finess, annee, mois, path, lib = T, ...) 
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
irpsa.default <- function(finess, annee, mois, path, lib=T, ...){
  if (annee<2012|annee>2016){
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
           e = readr::col_euro_double(),
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
                                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)) %>%
    dplyr::mutate(DP = stringr::str_trim(DP))
  
  zad <- rpsa_i %>% dplyr::select(NOSEJPSY, NOSEQ,NBDA,ZAD) %>%  dplyr::mutate(da  = ifelse(NBDA>0,ZAD,""),
                                                                               lda = stringr::str_extract_all(da, '.{1,6}'))
  
  da <- purrr::flatten_chr(zad$lda)
  
  df <- zad %>% dplyr::select(NOSEJPSY, NOSEQ,NBDA)
  df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
  da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBDA)
  
  
  rpsa_i$ZAD[is.na(rpsa_i$ZAD)] <- ""
  rpsa_i <- rpsa_i %>% dplyr::mutate(das = extz(ZAD, ".{1,6}")) %>% dplyr::select(-ZAD)
  rpsa_i <- rpsa_i %>% sjmisc::set_label(c(libelles[-length(libelles)], "Stream DA ou facteurs associés"))
  
  da <- da %>% sjmisc::set_label(c('N° séquentiel de séjour','N° séquentiel de séquence au sein du séjour',
                                   'Diagnostics et facteurs associés'))
  rpsa_1 = list(rpsa = rpsa_i, das = da)
  
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données R3A.
#'
#' @examples
#' \dontrun{
#'    r3a <- ir3a(750712184,2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irpsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage ir3a(finess, annee, mois, path, lib = T, ...)
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
ir3a.default <- function(finess, annee, mois, path, lib=T, ...){
  if (annee<2012|annee>2016){
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
           e = readr::col_euro_double(),
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
                                            readr::fwf_widths(af,an), col_types = at , na=character(), ...)) %>%
    dplyr::mutate(DP = stringr::str_trim(DP))
  
  zad <- r3a_i %>% dplyr::select(NOSEJPSY,NOORDR,NBDA,ZAD) %>%  dplyr::mutate(da  = ifelse(NBDA>0,ZAD,""),
                                                                              lda = stringr::str_extract_all(da, '.{1,6}'))
  
  da <- purrr::flatten_chr(zad$lda)
  
  df <- zad %>% dplyr::select(NOSEJPSY,NOORDR,NBDA)
  df <- as.data.frame(lapply(df, rep, df$NBDA), stringsAsFactors = F) %>% dplyr::tbl_df()
  da <- dplyr::bind_cols(df,data.frame(DA = stringr::str_trim(da), stringsAsFactors = F) ) %>% dplyr::tbl_df() %>% dplyr::select(-NBDA)
  
  
  r3a_i$ZAD[is.na(r3a_i$ZAD)] <- ""
  r3a_i <- r3a_i %>% dplyr::mutate(das = extz(ZAD, ".{1,6}")) %>% dplyr::select(-ZAD)
  r3a_i <- r3a_i %>% sjmisc::set_label(c(libelles[-length(libelles)], "Stream DA ou facteurs associés"))
  
  da <- da %>% sjmisc::set_label(c('N° séquentiel de séjour',"N° d'ordre", 'Diagnostics et facteurs associés'))
  r3a_1 = list(r3a = r3a_i, das = da)
  
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
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame, tbl_df) contenant les données Anohosp SSR du Out.
#'
#' @examples
#' \dontrun{
#'    anoh <- iano_psy(750712184,2015,12,"~/Documents/data/psy")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irpsa}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iano_psy(finess, annee, mois, path, lib = T, ...)
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
iano_psy.default <- function(finess, annee, mois, path, lib=T, ...){
  if (annee<2012|annee>2016){
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
           e = readr::col_euro_double(),
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
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
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
                             readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
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
  ano_i <- ano_i %>% sjmisc::set_label(c(libelles,'Chaînage Ok'))
  ano_i <- ano_i %>% dplyr::select(-dplyr::starts_with("Fill"))
  return(ano_i)
}

##############################################
####################### ARCHIVES #############
##############################################

#' ~ *.zip - Liste et volume des fichiers d'une archive PMSI
#'
#' Pour lister sans dezipper les fichiers d'une archive
#'
#'
#' @param path Chemin d'acces  a l'archive
#' @param file Nom du fichier archive
#' @param view par defaut a T : affiche la liste avec View(), a F retourne la table affichee a T
#'
#' @examples
#' \dontrun{
#'    liste <- astat(path = '~/Documents/R/sources/2016/',
#'                   file = "750712184.2016.2.05042016093044.in.zip",
#'                   view = F)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{adezip}}, \code{\link{adezip2}}

#' @export
astat <- function(path,file, view=T){
  
  stat <- unzip(
    zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file), list=T) %>%
    dplyr::mutate(`Taille (Mo)` = round(Length/10e5,6)) %>% dplyr::select(-Length)
  
  if (view==T){View(stat)}
  if (view==F){return(stat)}
}

#' ~ *.zip - Dezippe des fichiers de l'archive PMSI, avec en parametre le nom de l'archive
#'
#' Alternative à la fonction \code{\link{adezip}}, si on connait précisement l'archive que l'on veut utiliser.
#'
#'
#' @param path Chemin d'acces a l'archive
#' @param file Nom de l'archive zip (ex: \code{750712184.2016.2.05042016093044.in.zip})
#' @param liste Liste des fichiers a dezipper parmi l'archive ; si \code{liste = ""}, dezippe la totalite de l'archive
#' @param pathto Chemin ou deposer les fichiers dezippes, par defaut a "", les fichiers sont mis la ou se trouve l'archive
#'
#' @examples
#' \dontrun{
#'    # Fichier ano
#'      adezip2(path = '~/Documents/R/sources/2011/',
#'              file = '750712184.2011.12.27012012141857.in.zip',
#'              liste = 'ano')
#'
#'    # Totalité de l'archive
#'      adezip2(path = '~/Documents/R/sources/2011/',
#'              file = '750712184.2011.12.27012012141857.in.zip',
#'              liste = '')

#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{adezip}}, \code{\link{astat}}, \code{\link{adelete}}

#' @export
adezip2 <- function(path, file, liste = "", pathto=""){
  liste <- unique(liste)
  if (pathto==""){pathto<-ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path)}
  if (liste[1]==""){unzip(zipfile = paste0(path,'/',file), exdir= pathto)}
  else{
    
    pat<- stringr::str_split(file,'\\.')
    cat('Dézippage archive\n',
        'Type     :',pat[[1]][5],'\n',
        'Finess   :',pat[[1]][1],'\n',
        'Période  :',paste(pat[[1]][2],paste0('M',pat[[1]][3])),'\n',
        'Fichiers :', liste,'\n')
    if (pat[[1]][5] == "in") {
      unzip(
        zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
        files = paste0(pat[[1]][1],'.',pat[[1]][2],'.',pat[[1]][3],'.',liste,'.txt'), exdir= pathto)
    }
    if (pat[[1]][5] == "out"){
      if (!(is.integer(grep("tra",liste)) && length(grep("tra",liste)) == 0L)){
        unzip(zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/', file), list=T)$Name -> l
        l[grepl('tra',l)] -> typtra
        if (length(typtra) > 1){
          liste <- c(liste[!grepl('tra', liste)], 'tra.txt', 'tra.raa.txt')
        }else{
          one <- stringr::str_locate(typtra, 'tra')
          stringr::str_sub(typtra, one[1,1], stringr::str_length(typtra)) -> typtra
          liste <- c(liste[!grepl("tra",liste)], typtra)
        }
      }
      unzip(
        zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
        files = paste0(pat[[1]][1],'.',pat[[1]][2],'.',pat[[1]][3],'.',liste),
        exdir= pathto)
    }
  }
}


#' ~ *.zip - Dezippe des fichiers de l'archive PMSI
#'
#' Dezipper une archive PMSI au besoin
#'
#'
#' @param finess Finess du fichier a dezipper
#' @param annee Annee du fichier
#' @param mois Mois du fichier
#' @param path Chemin d'acces au fichier
#' @param liste des fichiers à dezipper ex: ano, rss, rsa, dmi, ... ; si liste = "", dezippe la totalite de l'archive
#' @param type Type de l'archive : in / out
#' @param recent par defaut a T, l'archive la plus recente sera utilisee, sinon propose a l'utilisateur de choisir quelle archive dezipper
#' @param pathto par defaut a "", dezipper la ou est l'archive, sinon preciser le chemin ou dezipper les fichiers (ailleurs)
#'
#' @examples
#' \dontrun{
#'      adezip(750712184,2016,2, path = '~/Documents/R/sources/2016',
#'             liste = 'med',
#'             pathto = "~/Exemple",
#'             type = "out")
#'
#'      adezip(750712184,2016,2, path = '~/Documents/R/sources/2016',
#'             liste = c('med','rapss', 'ano'),
#'             pathto = "~/Exemple",
#'             type = "in")
#'
#'      adezip(750712184,2016,2, path = '~/Documents/R/sources/2016',
#'             liste = c('rss', 'ano'),
#'             pathto = "~/Exemple",
#'             type = "in",
#'             recent = F)
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{adezip2}}, \code{\link{astat}}, \code{\link{adelete}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage adezip(finess, annee, mois, path, liste, pathto = "", type, recent = T)
#' @export adezip
#' @export
adezip <- function(...){
  UseMethod('adezip')
}


#' @export
adezip.pm_param <- function(.params, ...){
  new_par <- list(...)
  noms <- c('finess', 'annee', 'mois', 'path', 'liste', 'type', 'recent', 'pathto')
  param2 <- utils::modifyList(.params, new_par)
  param2 <- param2[noms]
  param2 <- param2[!is.na(names(param2))]
  do.call(adezip.default, param2)
}

#' @export
adezip.list <- function(l, ...){
  .params <- l
  new_par <- list(...) 
  param2 <- utils::modifyList(.params, new_par)
  noms <- c('finess', 'annee', 'mois', 'path', 'liste', 'type', 'recent', 'pathto')
  param2 <- param2[noms]
  param2 <- param2[!is.na(names(param2))]
  do.call(adezip.default, param2)
}

#' @export
adezip.default <- function(finess, annee, mois, path, liste = "", pathto="", type, recent=T){
  
  
  if (pathto==""){pathto<-ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path)}
  
  liste <- unique(liste)
  u <- list.files(path)
  u <- u[grepl(paste0(type,'.zip'),u)]
  u <- u[grepl(paste0(finess,'.',annee,'.',mois),u)]
  lequel <- data.frame(archive = u,
                       type = unlist(lapply(stringr::str_split(u,'\\.'),'[',5) ),
                       Date.du.traitement = unlist(lapply(stringr::str_split(u,'\\.'),'[',4)))
  lequel <- lequel[lequel$type == type,]
  lequel <- lequel[rev(order(lequel$Date.du.traitement)),]
  if (recent==T){
    file <- as.character(lequel[1,1])
    pat<- stringr::str_split(file,'\\.')
    if (liste[1]==""){
      unzip(zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file), exdir= pathto)}
    else{
      cat('Dézippage archive\n',
          'Type     :',pat[[1]][5],'\n',
          'Finess   :',pat[[1]][1],'\n',
          'Période  :',paste(pat[[1]][2],paste0('M',pat[[1]][3])),'\n',
          'Fichiers :', liste,'\n')
      if (type=="out"){
        if (!(is.integer(grep("tra",liste)) && length(grep("tra",liste)) == 0L)){
          unzip(zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/', file), list=T)$Name -> l
          l[grepl('tra',l)] -> typtra
          if (length(typtra) > 1){
            liste <- c(liste[!grepl('tra', liste)], 'tra.txt', 'tra.raa.txt')
          }else{
            one <- stringr::str_locate(typtra, 'tra')
            stringr::str_sub(typtra, one[1,1], stringr::str_length(typtra)) -> typtra
            liste <- c(liste[!grepl("tra",liste)], typtra)
          }
        }
        
        unzip(
          zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
          files = paste0(pat[[1]][1],'.',pat[[1]][2],'.',pat[[1]][3],'.',liste),
          exdir= pathto)
      }
      if (type=="in"){
        unzip(
          zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
          files = paste0(pat[[1]][1],'.',pat[[1]][2],'.',pat[[1]][3],'.',liste,".txt"),
          exdir= pathto)
      }
      
    }}
  
  if (recent==F){
    lequel$Date.du.traitement <- lubridate::parse_date_time(lequel$Date.du.traitement,'%d%m%y%h%m%s')
    lequel$Quelle <- 1:nrow(lequel)
    cat("Quelle archive ?\nEntrez la Quelle dézipper\n")
    print(lequel,row.names = F)
    q <- readline()
    file <- as.character(lequel[lequel$Quelle==q,]$archive)
    pat<- stringr::str_split(file,'\\.')
    if (liste[1]==""){
      unzip(zipfile = paste0(path,'/',file), exdir= pathto)}
    else{
      cat('Dézippage archive\n',
          'Type     :',pat[[1]][5],'\n',
          'Finess   :',pat[[1]][1],'\n',
          'Période  :',paste(pat[[1]][2],paste0('M',pat[[1]][3])),'\n',
          'Fichiers :', liste,'\n')
      if (type=="out"){
        if (!(is.integer(grep("tra",liste)) && length(grep("tra",liste)) == 0L)){
          unzip(zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/', file), list=T)$Name -> l
          l[grepl('tra',l)] -> typtra
          if (length(typtra) > 1){
            liste <- c(liste[!grepl('tra', liste)], 'tra.txt', 'tra.raa.txt')
          }else{
            one <- stringr::str_locate(typtra, 'tra')
            stringr::str_sub(typtra, one[1,1], stringr::str_length(typtra)) -> typtra
            liste <- c(liste[!grepl("tra",liste)], typtra)
          }
        }
        
        
        unzip(
          zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
          files = paste0(pat[[1]][1],'.',pat[[1]][2],'.',pat[[1]][3],'.',liste),
          exdir= pathto)
      }
      if (type=="in"){
        unzip(
          zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
          files = paste0(pat[[1]][1],'.',pat[[1]][2],'.',pat[[1]][3],'.',liste,".txt"),
          exdir= pathto)
      }
    }
  }
}

#' ~ *.zip - Dezippe des fichiers de l'archive PMSI en provenance de l'Intranet AP-HP, avec en parametre le nom de l'archive
#'
#' Version de la fonction \code{\link{adezip2}} pour des archives au format Intranet du DIM Siège de l'AP-HP,
#' \url{http://dime.aphp.fr/}.
#'
#'
#' @param finess Finess du fichier a dezipper
#' @param path Chemin d'acces au fichier
#' @param file Nom de l'archive zip (ex: \samp{MCO_IN_00000_201603.zip})
#' @param liste des fichiers a dezipper ex: ano, rss, rsa, dmi, ... ; si liste = "", dezippe la totalite de l'archive
#' @param pathto Chemin ou deposer les fichiers dezippes, par defaut à "", les fichiers sont mis la ou se trouve l'archive
#'
#' @examples
#' \dontrun{
#'     # Fichier ano
#'     adezip3(path = '~/Downloads',
#'             file = 'MCO_IN_00000_201603.zip',
#'             liste = 'ano')
#'
#'     # Totalité de l'archive
#'     adezip2(path = '~/Downloads',
#'             file = 'MCO_IN_00000_201603.zip',
#'             liste = '')
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{adezip2}}, \code{\link{adezip}}, \code{\link{astat}}, \code{\link{adelete}}

#' @export
adezip3 <- function(finess, path, file, liste = "", pathto=""){
  
  if (pathto==""){pathto<-ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path)}
  liste <- unique(liste)
  if (liste[1]==""){unzip(zipfile = paste0(path,'/',file), exdir = pathto)}
  else{
    
    
    liste[grepl("tra",liste)] <- "tra.txt"
    pat<- stringr::str_split(file,'\\_')
    cat('Dézippage archive\n',
        'Type     :',pat[[1]][2],'\n',
        'Nohop    :',pat[[1]][3],'\n',
        'Période  :',substr(pat[[1]][4],1,6),'\n',
        'Fichiers :', liste,'\n')
    if (pat[[1]][2]=="IN"){
      unzip(
        zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
        files = paste0(finess,'.',substr(pat[[1]][4],1,4),'.',as.numeric(substr(pat[[1]][4],5,7)),'.',liste,'.txt'),
        exdir = pathto)
    }
    if (pat[[1]][2]=="OUT"){
      if (!(is.integer(grep("tra",liste)) && length(grep("tra",liste)) == 0L)){
        unzip(zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/', file), list=T)$Name -> l
        l[grepl('tra',l)] -> typtra
        if (length(typtra) > 1){
          liste <- c(liste[!grepl('tra', liste)], 'tra.txt', 'tra.raa.txt')
        }else{
          one <- stringr::str_locate(typtra, 'tra')
          stringr::str_sub(typtra, one[1,1], stringr::str_length(typtra)) -> typtra
          liste <- c(liste[!grepl("tra",liste)], typtra)
        }
      }
      unzip(
        zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file),
        files = paste0(finess,'.',substr(pat[[1]][4],1,4),'.',as.numeric(substr(pat[[1]][4],5,7)),'.',liste), exdir= pathto)
    }
    
  }
}

#' ~ *.zip - Suppression des fichiers en fin de traitement
#'
#' Supprime les fichiers de l'archive PMSI dezippes en début de traitement
#'
#'
#' @param finess Finess du fichier a supprimer
#' @param annee Annee du fichier
#' @param mois Mois du fichier
#' @param path Chemin d'acces aux fichiers
#' @param liste Liste des fichiers a effacer : par defaut a "", efface tous les \code{fichiers finess.annee.mois.}
#' @param type Type de fichier In / Out : par defaut a "", efface tous les fichiers \code{finess.annee.mois.}
#'
#' @examples
#' \dontrun{
#'    adelete(750712184,2016,2, path = '~/Exemple',  liste = c("rss","ano"), type = "in")
#'    
#'    adelete(750712184,2016,2, path = '~/Exemple')
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{adezip}}, \code{\link{adezip2}}, \code{\link{astat}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage adelete(finess, annee, mois, path, liste, type)
#' @export adelete
#' @export
adelete <- function(...){
  UseMethod('adelete')
}


#' @export
adelete.pm_param <- function(.params, ...){
  .params <- l
  new_par <- list(...)
  noms <- c('finess', 'annee', 'mois', 'path', 'liste', 'type')
  param2 <- utils::modifyList(.params, new_par)
  param2 <- param2[noms]
  param2 <- param2[!is.na(names(param2))]
  do.call(adelete.default, param2)
}

#' @export
adelete.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  noms <- c('finess', 'annee', 'mois', 'path', 'liste', 'type')
  param2 <- utils::modifyList(.params, new_par)
  param2 <- param2[noms]
  param2 <- param2[!is.na(names(param2))]
  do.call(adelete.default, param2)
}

#' @export
adelete.default <- function(finess, annee, mois, path, liste = "", type = ""){
  
  if (type == "" & liste == ""){
    liste <- list.files(paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path)))
    liste <- liste[grepl(paste0(finess,'.',annee,'.',mois,'.'), liste) & !grepl('\\.zip', liste)]
    if (length(liste) == 0){stop('Aucun fichier correspondant.')}
    file.remove(paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',liste))
    return(TRUE)
  }
  
  if ((type != "" & liste == "" )||( type == "" & liste != "")){stop("Type et liste doivent etre vides ensemble ou precises ensemble.")}
  liste[grepl("tra",liste)] <- "tra.txt"
  
  if (type == "in") { file.remove(paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',finess,'.',annee,'.',mois,'.',liste,".txt"))}
  if (type == "out"){ file.remove(paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',finess,'.',annee,'.',mois,'.',liste))}
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
#'    irsa(750712184, 2016, 8, '~/path/path', typi= 1, n_max = 1) -> import
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
    label = sjmisc::get_label(table),
    type  = sapply(table, class)
  ) %>% sjmisc::set_label(c("Nom de la variable","Libellé, de la variable", "Type"))
}

##############################################
####################### Tidy #################
##############################################

#' ~ MCO - Tidy Diagnostics
#'
#' Restructurer les diagnostics
#'
#' On obtient une table contenant tous les diagnostics par séjour, sur le principe suivant :
#' Une variable numérique indique la position des diagnostics
#' - pour les rsa : 1 : DP du rsa, 2 : DR du rsa, 3 : DPUM, 4 : DRUM, 5 : DAS
#' - pour les rum : 1 : DP du rum, 2 : DR du rum, 3 : DAS, 4 : DAD
#'
#' @param d Objet S3 resultat de l'import pmeasyr (irsa, irum)
#' @param include booleen : defaut a T; T : restructure l'objet S3 (agglomere dp, dr, das et dad, par exemple)
#'
#' @examples
#' \dontrun{
#' # avec include = T
#' irum(750712184, 2016, 8, '~/path/path', typi = 3) -> d1
#' tdiag(d1) -> d1
#' d1$diags
#' d1$actes
#' d1$dads
#' irsa(750712184, 2016, 8, '~/path/path', typi = 4) -> d1
#' tdiag(d1, include = F) -> alldiag
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irsa}}, \code{\link{irum}}

#' @export
tdiag <- function (d,  include = T)
{
  if (names(d)[1] == "rsa") {
    temp <- d$rsa %>% dplyr::select(CLE_RSA, NSEQRUM = NOSEQRUM, DP, DR) %>%
      sjmisc::set_label(rep("", 4))
    e <- temp %>% tidyr::gather(position, diag, -CLE_RSA, - NSEQRUM,
                                na.rm = T)
    f <- e %>% dplyr::filter(diag != "")
    g <- d$das  %>% dplyr::select(CLE_RSA,NSEQRUM,diag = DAS) %>% dplyr::mutate(position = "DAS")
    h <- dplyr::bind_rows(f, g)
    temp <- d$rsa_um %>% dplyr::select(CLE_RSA, NSEQRUM, DPUM, DRUM) %>%
      sjmisc::set_label(rep("", 4))
    e <- temp %>% tidyr::gather(position, diag, -CLE_RSA, - NSEQRUM,
                                na.rm = T)
    f <- e %>% dplyr::filter(diag != "")
    h <- dplyr::bind_rows(h, f) %>% dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "DP", `2` = "DR", `3` = "DPUM", `4` = "DRUM",
                                                                                                         `5` = "DAS"))))
    h <- h %>% sjmisc::set_label(c("Clé rsa", "N° du RUM","1:DP, 2:DR, 3:DPUM, 4:DRUM, 5:DAS",
                                   "Diagnostic"))
    if (include == F) {
      return(h)
    }
    else {
      return(list(rsa = d$rsa, rsa_um = d$rsa_um, actes = d$actes, diags = h))
    }
  }
  if (names(d)[1]  == "rum") {
    temp <- d$rum %>% dplyr::select(NAS,NORUM, DP, DR) %>% sjmisc::set_label(rep("",4))
    e <- temp %>% tidyr::gather(position, diag, -NAS,- NORUM, na.rm = T)
    f <- e %>% dplyr::filter(diag != "")
    g <- d$das %>% dplyr::rename(diag = DAS) %>% dplyr::mutate(position = "DAS")
    g2 <- d$dad %>% dplyr::rename(diag = DAD) %>% dplyr::mutate(position = "DAD")
    h <- dplyr::bind_rows(list(f, g, g2)) %>%
      dplyr::mutate(position = as.numeric(as.character(forcats::fct_recode(position,`1` = "DP", `2` = "DR", `3` = "DAS", `4`="DAD"))))
    
    h <- h %>% sjmisc::set_label(c("N° administratif du séjour", "N° du RUM",
                                   "1:DP, 2:DR, 3:DAS, 4:DAD", "Diagnostic"))
    if (include == F) {
      return(h)
    }
    else {
      return(list(rum = d$rum, actes = d$actes, diags = h))
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
#'    irafael(750712184,2015,12,'~/Documents/data/rsf') -> rsfa15
#'    irafael(750712184,2015,12,'~/Documents/data/rsf', lister = 'C', lamda = T) -> rsfa14_lamda
#' }
#'
#' @param finess Finess du Out a importer : dans le nom du fichier
#' @param annee Annee PMSI (nb) des donnees sur 4 caracteres (2016)
#' @param mois Mois PMSI (nb) des donnees (janvier : 1, decembre : 12)
#' @param path Localisation du fichier de donnees
#' @param lib Ajout des libelles de colonnes aux tables, par défaut a TRUE ; necessite le package \code{sjmisc}
#' @param stat avec stat = T, un tableau synthetise le nombre de lignes par type de rafael
#' @param lister Liste des types d'enregistrements a importer
#' @param lamda a TRUE, importe les fichiers \code{rsfa-maj} de reprise de l'annee passee
#' @param ~... Autres parametres a specifier \code{n_max = 1e3}, ...
#' @author G. Pressiat
#'
#' @seealso \code{\link{iano_rafael}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage irafael(finess, annee, mois, path, lib = T, stat = T, lister = c("A", "B",
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
irafael.default <- function(finess, annee, mois, path, lib = T, stat = T, lister = c('A', 'B', 'C', 'H', 'L', 'M',  'P'), lamda = F, ...){
  if (annee<2011|annee>2016){
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
    typi_r <- 9
    
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
    typi_r <- 27
    
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
      one %>% sjmisc::set_label(fa$libelle) -> one
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
  return(list("A" = rafael_A,
              "B" = rafael_B,
              "C" = rafael_C,
              "H" = rafael_H,
              "L" = rafael_L,
              "M" = rafael_M,
              "P" = rafael_P))
  
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
#' @param lib Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjmisc}
#' @param ~... parametres supplementaires a passer
#' dans la fonction \code{\link[readr]{read_fwf}}, par exemple
#' \code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}
#'
#' @return Une table (data.frame ou tbl_df) qui contient les données Anohosp in / out
#'
#' @examples
#' \dontrun{
#'    iano_rafael(750712184, 2015, 12,'~/Documents/data/rsf') -> ano_out15
#'    iano_rafael(750712184, 2015, 12,'~/Documents/data/rsf', lamda = T) -> lamda_maj_ano_out14
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{irafael}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @usage iano_rafael(finess, annee, mois, path, lib = T, lamda = F, ...)
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
iano_rafael.default <- function(finess, annee, mois, path,  lib = T, lamda = F, ...){
  if (annee<2012|annee>2016){
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
           e = readr::col_euro_double(),
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
                                               readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
    dplyr::mutate(DTSORT   = lubridate::dmy(DTSORT),
                  DTENT    = lubridate::dmy(DTENT),
                  cok = ((CRNOSEC=='0')+(CRDNAIS=='0')+ (CRSEXE=='0') + (CRNAS=='0') +
                           (CRDENTR=='0') ==5)) %>% sjmisc::set_label(c(libelles, 'Chaînage Ok'))
  }
  if (lamda == T){    ano_i <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".ano-ace-maj"),
                                               readr::fwf_widths(af,an), col_types = at , na=character(), ...)  %>%
    dplyr::mutate(DTSORT   = lubridate::dmy(DTSORT),
                  DTENT    = lubridate::dmy(DTENT),
                  cok = ((CRNOSEC=='0')+(CRDNAIS=='0')+ (CRSEXE=='0') + (CRNAS=='0') +
                           (CRDENTR=='0') ==5)) %>% sjmisc::set_label(c(libelles, 'Chaînage Ok'))
  }
  
  
  
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
#' path   = '~/Documents/data/mco'
#' )
#' 
#' c(p, type = "out", liste = "") %>% adezip()
#' 
#' p %>% irsa()     -> rsa
#' p %>% iano_mco() -> ano
#' p %>% ipo()      -> po
#' 
#' c(p, type = "in", liste = "") %>% adezip()
#' p %>% irum()     -> rum
#' 
#' # Modifier le type d'import :
#' c(p, typi = 6) %>% irsa() -> rsa
#' 
#' }
#' 
#' @export 
noyau_pmeasyr <- function(...){
  params <- list(...)
  #attr(params, "class") <- "list"
  class(params) <- append(class(params),"pm_param")
  return(params)
}


