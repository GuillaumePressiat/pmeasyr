##############################################
####################### ARCHIVES #############
##############################################

#' ~ *.zip - Liste et volume des fichiers d'une archive PMSI
#'
#' Pour lister sans dezipper les fichiers d'une archive.
#' 
#' @return Une `data.frame` contenant les colonnes :
#' * *Name* Nom du fichier
#' * *Date* Date de modification
#' * *Taille (Mo)*
#'
#' @param path Chemin d'accès au dossier contenant les archives
#' @param file Nom du fichier d'archive
#' @param view par défaut `TRUE` : affiche la liste avec [utils::View()]  Sinon retourne la table dans la console.
#'
#' @examples
#' test_files_dir <- system.file("extdata", "test_data", "test_adezip", package = "pmeasyr")
#' astat(path = test_files_dir, file = "671234567.2016.1.12032016140012.in.zip", view = FALSE)
#'
#' @author G. Pressiat
#'
#' @seealso [adezip()] et [adezip2()] permettent de décompresser les fichiers. Cette fonction est basée sur [utils::unzip()] en utilisant l'argument `list = TRUE`
#' @export
#' @md
astat <- function(path, file, view = TRUE){
  
  stat <- unzip(
    zipfile = file.path(path ,file), 
    list = TRUE
    ) %>%
    dplyr::mutate(`Taille (Mo)` = round(Length / 10^6, 6)) %>% 
    dplyr::select(-Length)
  
  if (view) {
    return(View(stat, title = file))
  } else {
    return(stat)
  }
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


#' ~ *.zip - Identifie et dézippe des fichiers de l'archive PMSI
#'
#' Recherche et dézippe (décompresse) les fichiers contenus dans une archive \emph{*.in} ou \emph{*.out} du PMSI en fonction de paramètres. Il est possible de passer directement les paramètres permettant d'identifier l'archive à dézipper (méthode par défaut) ou à l'aide de paramètres enregistrés dans un noyau de paramètres (voir fonction \code{\link{noyau_pmeasyr}}).
#' @param ... Paramètres supplémentaires. Permet par exemple de changer un des paramètres après avoir passé un noyau de paramètres sans changer le noyau de paramètres.

#' @param .params Un noyau de paramètres définis par la fonction fonction \code{\link{noyau_pmeasyr}}
#' @examples
#' \dontrun{
#'      adezip('750712184',2016,2, path = '~/Documents/R/sources/2016',
#'             liste = 'med',
#'             pathto = "~/Exemple",
#'             type = "out")
#'
#'      adezip('750712184',2016,2, path = '~/Documents/R/sources/2016',
#'             liste = c('med','rapss', 'ano'),
#'             pathto = "~/Exemple",
#'             type = "in")
#'
#'      adezip('750712184',2016,2, path = '~/Documents/R/sources/2016',
#'             liste = c('rss', 'ano'),
#'             pathto = "~/Exemple",
#'             type = "in",
#'             recent = F)
#'             
#'      # Utilisation avec un noyau de paramtères
#'      p <- noyau_pmeasyr(
#'             finess = '750712184',
#'             annee  = 2016,
#'             mois   = 12,
#'             path   = '~/Documents/data/mco',
#'             progress = F
#'             )
#'      
#'      adezip(p, type = "in")
#'      
#'      # Modification d'un paramètre du noyay
#'      adezip(p, mois = 11, type = "in")
#' }
#'
#' @author G. Pressiat
#'
#' @seealso \code{\link{adezip2}}, \code{\link{astat}}, \code{\link{adelete}},
#' utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
#' @export adezip
#' @export
adezip <- function(...){
  UseMethod('adezip')
}


#' @export
#' @rdname adezip
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

#' @param finess Finess du fichier à dézipper
#' @param annee Année du fichier
#' @param mois Mois du fichier
#' @param path Chemin d'accès au répertoire contenant le fichier d'archive
#' @param liste Vecteur de caractère avec le type de fichiers à dézipper (ex: ano, rss, rsa, dmi, ...). Par défaut, \code{liste = ""} dezippe la totalite de l'archive
#' @param type Type de l'archive : in / out
#' @param recent par défaut \code{T}, l'archive la plus recente sera utilisee, sinon propose à l'utilisateur de choisir quelle archive dezipper
#' @param pathto par defaut à \code{""}, dézipper dans le même répertoire que l'archive, sinon préciser le chemin ou dezipper les fichiers dans le répertoire indiqué par \code{pathto}
#' @export
#' @rdname adezip
adezip.default <- function(finess, annee, mois, path, liste = "", 
                           pathto = "", type, recent = TRUE){
  
  # TODO: simplifier cette fonction
  # Si aucun chemin de dossier pour décompresser n'est défini
  # utiliser selui contenant les fichiers
  if (pathto == ""){
    pathto <- ifelse(
      # Si le dernier caractère du chemin est un /
      substr(path,nchar(path),nchar(path))=="/",
      # alors le retirer
      substr(path,1,nchar(path)-1),
      # Sinon utiliser le chemin
      path)
    }
  
  # liste des types de fichiers
  liste <- unique(liste)
  
  # Récuper les .zip
  u <- list.files(path, pattern = "\\.zip$")
  
  # Sélectionner les fichiers d'archives correspondants aux critères
  regex_fichier_selectionne <- paste0(finess, '\\.', annee, '\\.', mois, '\\.')
  u <- u[grepl(regex_fichier_selectionne, u)]
  
  # Créer une data.frame pour disposer de l'information de chaque fichier
  # TODO: créer une fonction spécialisée pour parser les informations du nom
  # de fichier d'archive (en particulier date et heure de traitement)
  lequel <- data.frame(archive = u,
                       type = unlist(lapply(stringr::str_split(u,'\\.'),'[',5) ),
                       Date.du.traitement = unlist(lapply(stringr::str_split(u,'\\.'),'[',4)))
  lequel <- lequel[lequel$type == type,]
  lequel <- lequel[rev(order(lequel$Date.du.traitement)),]
  
  # Si il ne faut prendre que le plus récent
  if (recent) {
    # Sélectionner le premier (en théorie le plus récent)
    file <- as.character(lequel[1,1])
    pat<- stringr::str_split(file,'\\.')
    if (liste[1]==""){
      # TODO: écrire une fonction qui ne fait que écrire le chemin du
      # fichier à dezipper
      unzip(zipfile = paste0(ifelse(substr(path,nchar(path),nchar(path))=="/",substr(path,1,nchar(path)-1),path),'/',file), exdir= pathto)}
    else{
      # TODO: envoyer un message au lieu d'un cat et écrire une fonction
      # pour éviter de répéter ceci 4 fois
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
  
  if (!recent){
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
#'     adezip3(path = '~/Downloads',
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
#'    adelete('750712184',2016,2, path = '~/Exemple',  liste = c("rss","ano"), type = "in")
#'    
#'    adelete('750712184',2016,2, path = '~/Exemple')
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

#' Extraire les informations d'un nom de fichier
#' 
#' Les noms de fichiers des archives PMSI ont toujours le même format, de la forme *finess.annee_envois.mois_envois.* suivi par *horodatage* pour les archives *in* et *ou* et enfin le *type* de fichier. Cette fonction permet d'extraire cette information.
#' @return Une liste avec les informations extraites du nom de fichier :
#' * `nom_fichier` Le nom de fichier passé en arguement
#' * `finess`
#' * `annee`
#' * `mois`
#' * `horodatage_production` L'horodatage de production pour les fichiers *in* et *out* au format POSIXlt
#' * `type` Type de fichier : *in*, *out*, *rss*...
#' @param nom_fichier Une chaine de caractères du fichier à découper
#' @param format_date_archive Format de date d'horodatage pour les fichiers archive avec la notation de [base::strptime()].
#' @examples 
#' parse_nom_fichier("671234567.2016.1.12032016140012.in.zip")
#' @export
#' @md
parse_nom_fichier <- function(nom_fichier, format_date_archive = '%d%m%Y%H%M%S') {
  #TODO : permettre à cette fonction de gérer n noms (vectorisation)
  # Sortira une liste avec un élément par fichier
  # Cette liste pourra être empilée à l'aide de dplyr::bind_row
  if (length(nom_fichier) != 1) stop("nom_fichier doit être de longueur 1")
  
  # Initialiser le vecteur de résultat
  x <- list(nom_fichier = nom_fichier)
  
  champs_separe <- unlist(strsplit(x = nom_fichier, split = ".", fixed = TRUE))
  x$finess <- champs_separe[1]
  x$annee <- champs_separe[2]
  x$mois <- champs_separe[3]
  
  # Si le fichier est une archive, détecter l'horodatage de production et le type
  if (champs_separe[length(champs_separe)] %in% c("zip", "in", "out")) {
    x$horodatage_production <- strptime(champs_separe[4], 
                                          format = format_date_archive) 
    x$type <- champs_separe[5]
  } else {
    x$type <- champs_separe[4]
  }
  
  x
}