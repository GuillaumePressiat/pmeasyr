
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

