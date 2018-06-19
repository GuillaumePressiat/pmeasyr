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
#' astat(path = test_files_dir, file = "123456789.2016.2.15032016152413.in.zip", view = FALSE)
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
#' Alternative à la fonction [adezip()], si on connait precisement l'archive que l'on veut utiliser. 
#' 
#' @details `adezip2` est un simple wrapper autour de la fonction `adzip.default`. Cette fonction est dépréciée. En utilisant le paramètre `nom_archive` avec la fonciton [adezip()] on obtient le même résultat.
#' 
#' @param file Nom de l'archive zip (ex: \code{750712184.2016.2.05042016093044.in.zip})
#' @inheritParams adezip
#' @examples
#' # Chemin vers un dossier temporaire
#' tmp_dir <- tempdir()
#' 
#' # Chemin vers un dossier contenant des archives simulées
#' dossier_archives <- system.file("extdata", "test_data", "test_adezip", package = "pmeasyr")
#' 
#' # Décompresser en fonction du finess, année et mois du 
#' # fichier med d'une archive out
#'  adezip2(path = dossier_archives,
#'          file = "123456789.2017.7.21082017091715.out.zip",
#'          liste = 'med',
#'          pathto = tmp_dir)
#'  
#' dir(tmp_dir, pattern = "2017\\.7.*med")
#' @author G. Pressiat
#'
#' @seealso \code{\link{adezip}}, \code{\link{astat}}, \code{\link{adelete}}
#' @md

#' @export
adezip2 <- function(path, file, liste = "", pathto=""){
  adezip.default(path = path, nom_archive = file,
                 liste = liste, pathto = pathto)
}


#' ~ *.zip - Identifie et dezippe des fichiers de l'archive PMSI
#'
#' Recherche et dezippe (décompresse) les fichiers contenus dans une archive \emph{*.in} ou \emph{*.out} du PMSI en fonction de parametres. 
#' 
#' 
#' Il est possible de passer directement les paramètres permettant d'identifier l'archive à dézipper (méthode par défaut) ou à l'aide de paramètres enregistrés dans un noyau de paramètres (voir fonction \code{\link{noyau_pmeasyr}}).
#' 
#' @param ... Paramètres supplémentaires. Permet par exemple de changer un des paramètres après avoir passé un noyau de paramètres sans changer le noyau de paramètres.

#' @examples
#' # Chemin vers un dossier temporaire
#' tmp_dir <- tempdir()
#' 
#' # Chemin vers un dossier contenant des archives simulées
#' dossier_archives <- system.file("extdata", "test_data", "test_adezip", package = "pmeasyr")
#' 
#' # Décompresser en fonction du finess, année et mois du 
#' # fichier med d'une archive out
#'  adezip('123456789', 2016, 2, 
#'         path = dossier_archives,
#'         liste = 'med',
#'         pathto = tmp_dir,
#'         type = "out")
#'  
#' dir(tmp_dir)
#'
#' # Décompresser plusieurs types de fichiers d'une archive in
#'  adezip('123456789', 2016, 2, 
#'         path = dossier_archives,
#'         liste = c('med','rapss', 'ano'),
#'         pathto = tmp_dir,
#'         type = "in")
#'         
#' dir(tmp_dir)
#' 
#' # Utilisation avec un noyau de paramtères
#' p <- noyau_pmeasyr(
#'        finess = '123456789',
#'        annee  = 2016,
#'        mois   = 12,
#'        path   = dossier_archives,
#'        progress = FALSE
#'        )
#' 
#' adezip(p, type = "in", pathto = tmp_dir)
#' dir(tmp_dir)
#' # Modification d'un paramètre du noyau
#' adezip(p, mois = 11, type = "in",  pathto = tmp_dir)
#' dir(tmp_dir)
#' 
#' # Pour une même période (année/mois), il peut y avoir plusieurs archives si 
#' # il y a eu plusieurs envois. Par exemple il y a deux version de l'archive
#' # out pour la période 2017.10 dans notre exemple simulé
#' dir(dossier_archives, pattern = "2017\\.10.*out\\.zip")
#' 
#'  # Lorsque l'arguement `recent` est `TRUE` alors la fonction adezip 
#'  # sélectionne automatiquement l'archive la plus récente
#'  adezip(123456789, 2017, 10,
#'         path = dossier_archives,
#'         liste = 'med',
#'         pathto = tmp_dir,
#'         type = "out") 
#'  dir(tmp_dir, pattern = "med")
#'  
#'  # Si l'arguement `recent` est `FALSE` alors l'utilisateur est invité
#'  # à choisir
#'  \dontrun{
#'  adezip(123456789, 2017, 10,
#'         path = dossier_archives,
#'         liste = 'rsa',
#'         pathto = tmp_dir,
#'         recent = FALSE,
#'         type = "out")
#'  dir(tmp_dir, pattern = "rsa")
#'  }
#' 
#' # Dézipper les logs
#' \dontrun{
#' adezip(p, type = "out", liste = c('chainage.log', 'comp.log', 'log'))
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
#' @param .params Un noyau de paramètres définis par la fonction fonction \code{\link{noyau_pmeasyr}}
#' @rdname adezip
adezip.pm_param <- function(.params, ...){
  new_par <- list(...)
  noms <- c('finess', 'annee', 'mois', 'path', 'liste', 'type', 'recent', 'pathto', 'quiet')
  param2 <- utils::modifyList(.params, new_par)
  param2 <- param2[noms]
  param2 <- param2[!is.na(names(param2))]
  do.call(adezip.default, param2)
}

#' @export
#TODO : a déprécier puis supprimer ?
adezip.list <- function(l, ...){
  .params <- l
  new_par <- list(...) 
  param2 <- utils::modifyList(.params, new_par)
  noms <- c('finess', 'annee', 'mois', 'path', 'liste', 'type', 'recent', 'pathto', 'quiet')
  param2 <- param2[noms]
  param2 <- param2[!is.na(names(param2))]
  do.call(adezip.default, param2)
}

#' Dezipper les fichiers selectionnes d'une archive
#' @param finess Finess de l'archive.
#' @param annee Année de l'archive.
#' @param mois Mois de l'archive.
#' @param path Chemin d'accès au répertoire contenant l'archive à décompresser
#' @param liste Vecteur de caractère avec le type de fichiers à dézipper (ex: ano, rss, rsa, dmi, ...). Par défaut, `liste = ""` dezippe la totalite de l'archive.
#' @param type Type de l'archive : *in* ou *out*.
#' @param recent par défaut `TRUE`, l'archive la plus recente sera utilisee, sinon propose à l'utilisateur de choisir quelle archive dezipper
#' @param pathto Par defaut la même valeur que `path`, dézipper dans le même répertoire que l'archive, sinon préciser le chemin ou dezipper les fichiers dans le répertoire indiqué par `pathto`.
#' @param nom_archive Nom de l'archive à décompresser dans le dossier `path`. Par défaut, `NULL`, n'utilise pas ce paramètre. Si le chemin est spécifié, alors les paramètres `finess`, `annee`, `mois` et `recent` ne sont pas utilisés.
#' @param quiet Affichage d'un message au dézippage `TRUE` / `FALSE`
#' @return Les chemins d'accès des fichiers décompressés, de manière invisible.
#' 
#' @seealso [utils::unzip()]
#' @export
#' @importFrom magrittr %>%
#' @importFrom dplyr filter arrange mutate select
#' @rdname adezip
#' @md
adezip.default <- function(finess, annee, mois, 
                           #TODO: renomme l'argument `path` par un terme plus
                           # explicite (ex. : chemin_dossier_archives) tout en
                           # prenant garde aux effets de bord sur les
                           # autres fonctions
                           path, 
                           liste = NULL, 
                           #TODO: renommer l'argument `pathto` en un terme
                           # en Français afin d'avoir une constance dans les
                           # règles de nommage des arguments
                           pathto = path, 
                           type = "in", recent = TRUE, nom_archive = NULL,
                           quiet = FALSE, ...){
  
  # Si le nom de l'archive n'est pas donné, alors rechercher l'archive
  # qui correspond aux arguments finess, annee et mois
  if (is.null(nom_archive)) {
    info_archive <- selectionne_archive(finess = finess, annee = annee, 
                                        mois = mois, dossier_archives = path,
                                        type_archive = type, recent = recent)
    
    nom_archive <- info_archive$nom_fichier
  } else {
    info_archive <- parse_nom_fichier(nom_archive)
  }
  
  # Le chemin d'accès de l'archive est la concatenation du chemin d'accès au
  # dossier contenant les archives avec le nom de l'archive
  chemin_archive <- file.path(path, nom_archive)
  
  info_archive$taille_mo <- signif(file.size(chemin_archive)/10^6, 2)
  
  # Selection des fichiers à extraire
  if (is.null(liste) || liste == "") {
    selection_fichiers_a_extraire <- NULL
  } else {
    selection_fichiers_a_extraire <- selectionne_fichiers(
      chemin_archive = chemin_archive, 
      types_fichier = liste
    )
  }
  
  # Extraire les types de fichiers courts pour l'affichage
  l_f <- extraire_types_fichiers(selection_fichiers_a_extraire, info_archive)
  liste_fichiers <- ifelse(is.null(selection_fichiers_a_extraire), "Tous", toString(l_f))
  
  if (liste_fichiers == ""){
    warning("Aucun fichier dézippé : vérifier le paramètre liste")
  }
  
  if (!quiet){
    message(
      "\n",
      "Dézippage de l'archive ", info_archive$nom_fichier, '\n',
      'Taille    : ', info_archive$taille_mo, " Mo\n",
      'Type      : ', info_archive$type, '\n',
      'Finess    : ', info_archive$finess, '\n',
      'Période   : ', info_archive$annee, ' M', info_archive$mois, '\n',
      'Date prod : ', info_archive$horodatage_production, '\n',
      'Fichiers  : ', liste_fichiers,'\n'
    )
  }
  unzip(zipfile = chemin_archive,
        files = selection_fichiers_a_extraire,
        exdir = pathto)
}

#' Sélectionne une archive in/out
#' @param dossier_archives Chemin vers le dossier contenant les archives. Vecteur de caractère de longueur 1.
#' @param type_archive Type d'archive : *in* (par défaut) ou *out*
#' @inherit adezip.default params
#' @inherit parse_nom_fichier return
#' @seealso [adezip.default()] et [parse_nom_fichier()]
#' @export
#' @md
selectionne_archive <- function(finess, mois, annee, dossier_archives, 
                                type_archive = "in", recent = TRUE) {
  # Récupérer la liste de fichiers .zip
  path_archives <- list.files(dossier_archives, pattern = "\\.zip$")
  
  # Créer une table avec les caractéristiques des fichiers ZIP
  df_zip <- parse_noms_fichiers(path_archives)
  
  # Sélectionner les fichiers d'archives correspondants aux critères
  df_zip_selectionne <- df_zip %>%
    filter(finess == !!finess, annee == !!annee, 
           mois == !!mois, type == !!type_archive) %>%
    # Trier du plus récent au plus ancien
    arrange(desc(horodatage_production))
  
  # Si il ne faut prendre que le plus récent
  if (recent) {
    ligne_zip_a_selectionner <- 1
  } else {
    # Si il y a plusieurs fichiers et que l'utilisateur souhaite
    # choisir lui même
    cat("Liste des archives correspondants aux critières :\n")
    df_zip_selectionne %>%
      mutate(`N°` = seq_len(n())) %>%
      select(`N°`, 
             "Nom de l'archive" = nom_fichier, 
             "Horodatage production" = horodatage_production) %>%
      as.data.frame %>%
      print(row.names = F)
    ligne_zip_a_selectionner <- readline(
      prompt = "Quelle numéro d'archive souhaitez vous dézipper ?\n")
  }
  
  as.list(df_zip_selectionne[ligne_zip_a_selectionner, ])
}


#' Selectionne les fichiers dans une archive
#' 
#' @return Le noms dans fichiers dans l'archive
#' @export
selectionne_fichiers <- function(chemin_archive, types_fichier) {
  
  fichiers_dans_archive <- unzip(zipfile = chemin_archive, list = TRUE)$Name
  
  tableau_fichiers <- parse_noms_fichiers(fichiers_dans_archive) %>% 
    dplyr::mutate(type = gsub("\\.txt", "", type))

  fichiers_selectionne <- dplyr::filter(
    tableau_fichiers, type %in% types_fichier)

  fichiers_selectionne$nom_fichier
}

#' ~ *.zip - Dezippe des fichiers de l'archive PMSI en provenance de l'Intranet AP-HP, avec en parametre le nom de l'archive
#'
#' Version de la fonction \code{\link{adezip2}} pour des archives au format Intranet du DIM Siege de l'AP-HP,
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
        'Type     : ',pat[[1]][2],'\n',
        'Nohop    : ',pat[[1]][3],'\n',
        'Période  : ',substr(pat[[1]][4],1,6),'\n',
        'Fichiers : ', liste,'\n')
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
#' Supprime les fichiers de l'archive PMSI dezippes en debut de traitement
#'
#'
#' @param finess Finess du fichier a supprimer
#' @param annee Annee du fichier
#' @param mois Mois du fichier
#' @param path Chemin d'acces aux fichiers
#' @param liste Liste des fichiers a effacer : par defaut a "", efface tous les \code{fichiers finess.annee.mois.}
#' @param type Type de fichier In / Out : par defaut a "", efface tous les fichiers \code{finess.annee.mois.}
#' @usage adelete(finess, annee, mois, path, liste, type, ...)
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
#'
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
adelete.default <- function(finess, annee, mois, path, liste = "", type = "", ...){
  
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
#' @param noms_fichiers Des noms de fichiers d'archive sous la forme d'un vecteur de caractères.
#' @param return_tible Retourne une table si `TRUE`, sinon retourne une liste.
#' @return Une liste ou une table avec les informations extraites du nom de fichier :
#' - `nom_fichier` Le nom de fichier passé en arguement
#' - `finess`
#' - `annee`
#' - `mois`
#' - `horodatage_production` L'horodatage de production pour les fichiers *in* et *out* au format POSIXlt
#' Si il y a plusieurs fichiers, la liste peut être facilement transformée en table avec [dplyr::bind_rows()].
#' - `type` Type de fichier : *in*, *out*, *rss*...
#' @examples 
#' noms_de_fichiers <- c("671234567.2016.1.12032016140012.in.zip",
#'                       "671234567.2016.1.rum.txt")
#'  x <- parse_noms_fichiers(noms_de_fichiers)
#' dplyr::bind_rows(x)
#' @export
#' @md
parse_noms_fichiers <- function(noms_fichiers, return_tibble = TRUE) {
  liste_fichiers <- lapply(X = noms_fichiers, FUN = parse_nom_fichier)
  
  if (return_tibble) {
    dplyr::bind_rows(liste_fichiers)
  } else {
    liste_fichiers
  }
}

#' Découpe un seul nom de fichier
#' @param nom_fichier Le nom du fichier à découper. Chaine de caractère de longueur 1.
#' @param format_date_archive Format de date d'horodatage pour les fichiers archive avec la notation de [base::strptime()].
#' @seealso [parse_noms_fichiers()] pour traiter plusieurs noms de fichiers.
#' @inherit parse_noms_fichiers
#' @md
parse_nom_fichier <- function(
  nom_fichier, 
  format_date_archive = '%d%m%Y%H%M%S'
) {
  
  # Vérifier la longueur de l'argument
  if (length(nom_fichier) != 1) stop("nom_fichier doit être de longueur 1")
  
  # Initialiser le vecteur de résultat
  x <- list(nom_fichier = nom_fichier)
  
  # Découper avec les points
  champs_separe <- unlist(strsplit(x = nom_fichier, split = ".", fixed = TRUE))
  
  # Récupérer chaque champ
  x$finess <- champs_separe[1]
  x$annee <- champs_separe[2]
  x$mois <- champs_separe[3]
  
  # Si le fichier est une archive, détecter l'horodatage de production et le type
  extension_fichier <- champs_separe[length(champs_separe)]
  
  if (extension_fichier  %in% c("zip", "in", "out")) {
    x$horodatage_production <- as.POSIXct(champs_separe[4], 
                                          format = format_date_archive) 
    x$type <- champs_separe[5]
  } else {
    # Si ce n'est pas une archive in/out
    # Ajout condition sur la longueur de champs_separe pour ajouter le champ 5 auquel cas
    x$type <- paste(champs_separe[4:length(champs_separe)], collapse = ".")
  }
  
  # Si le type est NA alors probablement pas un fichier avec bon format
  # Ne retourner que le nom de fichier
  if (is.na(x$type)) x <- x[1]
  
  x
}

#' Créer des archives simulées à partir de plusieurs archives réelles
#' @inherit creer_archive_vide
#' @param chemins_archives Chemin vers les archives qui servira de modèle pour l'archive simulée
#' @seealso [creer_archive_vide()]
#' @md
creer_archives_vides <- function(chemins_archives) {
  lapply(dir(chemins_archives, full.names = TRUE, pattern = "zip$"), FUN = creer_archive_vide)
  
  invisible(NULL)
}

#' Créer une archive vide à partir d'une archive réelle
#' @param chemin_archive Chemin vers une archive réelle qui servira de modèle pour l'archive simulée
#' @param dossier_cible Chemin vers le dossier où l'archive simulée sera stockée
#' @param finess_simul Un numéro finess pour le fichier simulé
#' @return `NULL`
#' @importFrom utils zip
#' @md
creer_archive_vide <- function(chemin_archive, 
                               dossier_cible = "archive_simul/",
                               finess_simul = "123456789"
) {
  # Créer le dossier simulé
  nom_archive <- basename(chemin_archive)
  finess_reel <- substr(nom_archive, 1, 9)
  
  info_archive <- parse_nom_fichier(nom_archive)
  
  
  nom_archive_simule <- creer_nom_archive(finess = finess_simul,
                                          annee = info_archive$annee,
                                          mois = info_archive$mois,
                                          type = info_archive$type)
  
  # Modifier l'horodatage de production pour que ne soit pas reconnaissable
  
  # Créer le dossier
  chemin_archive_simule <- file.path(dossier_cible, sub("\\.zip", "", nom_archive_simule))
  dir.create(chemin_archive_simule)
  
  # Lister les fichiers originaux
  noms_fichiers <- unzip(chemin_archive, list = T)$Name
  # Modifier les fichiers
  noms_fichiers_simul <-  sub(pattern = finess_reel, replacement = finess_simul, x = noms_fichiers)
  file.create(file.path(chemin_archive_simule, noms_fichiers_simul))
  
  # Zipper
  utils::zip(zipfile = paste0(chemin_archive_simule, ".zip"), 
             files = dir(chemin_archive_simule, full.names = T),
             # Sauvegarder uniquement les fichiers, pas le chemin complet
             flags = "-j")
  
  # Supprimer le répertoire temporaire
  unlink(chemin_archive_simule, recursive = TRUE)
  
  invisible(NULL)
}


#' Créer le nom d'une archive
#' Permet de simuler ou de créer des noms d'archives PMSI
#' @param horodatage Une date au format *dmYHMS*
#' @inherit adezip.default params
#' @return Un nom d'archive compatible
#' @importFrom stats runif
#' @md
creer_nom_archive <- function(
  finess,
  annee,
  mois,
  type = "in",
  horodatage = NULL
) {
  if (is.null(horodatage)) {
    
    rdm <- function(min, max, width = 2)
      formatC(x = as.integer(stats::runif(1, min, max)), width = width, flag = "0")
    # L'envois se fait entre le 20 et le 30 du mois qui suit. Donc faire M+1 entre le 15 et 28
    var_envois <- c(
      jour = rdm(15, 28),
      mois = formatC(as.integer(mois) + 1,width = 2, flag = "0"),
      annee = annee,
      heure = rdm(8, 20),
      minute = rdm(0, 59),
      seconde = rdm(0, 59)
    )
    
    horodatage <- paste(var_envois, collapse = "")
  }
  
  paste(finess, annee, mois, horodatage, type, sep = ".")
}


#' Extraire les types de fichiers
#' Permet d'afficher les types de fichiers d'une archive (ano, rss, rsa, ...)
#' @param selection_fichiers_a_extraire Fichiers sélectionnés dans le zip (résultat de selectionne_fichiers)
#' @param info_archive informatin de l'archive (résultat de selectionne_archive)
#' @return un vecteur
extraire_types_fichiers <- function(selection_fichiers_a_extraire, info_archive){
  # l_f <- stringr::str_replace_all(selection_fichiers_a_extraire,
  #                                 paste(info_archive$finess,info_archive$annee, info_archive$mois, "txt", sep = "|"), "")
  l_f <- stringr::str_replace_all(selection_fichiers_a_extraire, paste(info_archive$finess, "txt", sep = "|"), "")
  l_f <- stringr::str_replace_all(l_f, "[0-9]+|\\.$", "")
  l_f <- stringr::str_replace_all(l_f, "\\.{2,}", "")
  l_f
}

