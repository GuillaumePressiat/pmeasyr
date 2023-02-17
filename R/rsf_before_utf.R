
##############################################
####################### RSF ##################
##############################################
#' ~ RSF - Import des RSF
#'
#' Import des RSF en année courante 
#'
#' Formats depuis 2019 pour les rsf
#' 
#' @return Une classe S3 contenant les tables (data.frame, tbl_df ou tbl) importées  (rafaels)
#'
#' @examples
#' \dontrun{
#'    irsf('750712184',2019,12,'~/Documents/data/rsf') -> rsf19
#' }
#'
#' @param finess Finess du In a importer : dans le nom du fichier
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
irsf <- function(...){
  UseMethod('irsf')
}

#' @export
irsf.pm_param <- function(params, ...){
  new_par <- list(...)
  param2 <- utils::modifyList(params, new_par)
  do.call(irsf.default, param2)
}

#' @export
irsf.list <- function(l, ...){
  .params <- l
  new_par <- list(...)
  param2 <- utils::modifyList(.params, new_par)
  do.call(irsf.default, param2)
}

#' @export
irsf.default <- function(finess, annee, mois, path, lib = T, stat = T, 
                         lister = c('A', 'B', 'C', 'H', 'L', 'M',  'P'), 
                            lamda = F, tolower_names = F, ...){
  if (annee<2011|annee > 2023){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois<1|mois>12){
    stop('Mois incorrect\n')
  }
  
  op <- options(digits.secs = 6)
  un<-Sys.time()
  
  if (lamda == F){
    # cat(paste("Import des RSF", annee, paste0("M",mois),"\n"))
    # cat(paste("L'objet retourné prendra la forme d'une classe S3.
    #           $A pour les RSF A, et B, C, ...\n"))
    
    
    formats <- pmeasyr::formats %>% dplyr::filter(champ == "rsf", table == "rsf", an == substr(annee, 3, 4))
    # formats <- formats %>% dplyr::filter(champ == "rsf", table == "rsf", an == substr(annee, 3, 4))
    
    r <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rsf.txt"),
                         readr::fwf_widths(NA, 'lon'),
                         col_types = readr::cols('c'),  ...)
    readr::problems(r) -> synthese_import
    
    typi_r <- 1
    # avec les formats 2019-20, le test n'est pas nécessaire 
    # if (annee > 2016){typi_r <- 1}
  }
  # Lecture des lamdas RSF non prise en charge 
  #if (lamda == T){
  # cat(paste("Import des rsfa-maj", annee, paste0("M",mois),"\n"))
  # cat(paste("L'objet retourné prendra la forme d'une classe S3.
  #           $A pour les Rafael A, et B, C, ...\n"))
  # 
    
  # formats <- pmeasyr::formats %>% dplyr::filter(champ == "rsf", table == "rafael-maj", an == substr(annee,3,4))
    
  # r <- readr::read_fwf(paste0(path,"/",finess,".",annee,".",mois,".rsfa-maj"),
  #                       readr::fwf_widths(NA, 'lon'),
  #                       col_types = readr::cols('c'),  ...)
  #  readr::problems(r) -> synthese_import
  #  typi_r <- 27
  #  if (annee > 2017){typi_r <- 29}
  #}
  
  former <- function(cla, col1){
    switch(cla,
           'c' = col1,
           'trim' = col1 %>% stringr::str_trim(),
           'i' = col1 %>% as.integer(),
           'n' = (col1 %>% as.numeric() )/100,
           'dmy' = lubridate::dmy(col1))
  }
  
  cutt <- function(typs, lib){
    fa <- formats %>% dplyr::filter(Typer == typs)
    
    deb <- fa$position
    fin <- fa$fin
    u <- function(x, i){stringr::str_sub(x, deb[i], fin[i])}
    # sélection des lignes avec en-tête égale à typs
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
  
  
  if ('A' %in% lister){rsf_A <- suppressWarnings(cutt('A', lib))}else{rsf_A <- data.frame()}
  r %>% dplyr::filter(substr(lon,typi_r,typi_r) != 'A') -> r
  if ('B' %in% lister){rsf_B <- suppressWarnings(cutt('B', lib))}else{rsf_B <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'B') -> r
  if ('C' %in% lister){rsf_C <- suppressWarnings(cutt('C', lib))}else{rsf_C <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'C') -> r
  if ('M' %in% lister){rsf_M <- suppressWarnings(cutt('M', lib))}else{rsf_M <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'M') -> r
  if ('L' %in% lister){rsf_L <- suppressWarnings(cutt('L', lib))}else{rsf_L <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'L') -> r
  if ('P' %in% lister){rsf_P <- suppressWarnings(cutt('P', lib))}else{rsf_P <- data.frame()}
  r %>%  dplyr::filter(substr(lon,typi_r,typi_r) != 'P') -> r
  if ('H' %in% lister){rsf_H <- suppressWarnings(cutt('H', lib))}else{rsf_H <- data.frame()}
  rm(r)
  
  deux<-Sys.time()
  # at(paste("RSF",annee, paste0("M",mois),"chargés en : ",round(difftime(deux,un, units="secs"),0), "secondes\n"))
  
  if (stat == T){
    print(
      knitr::kable(dplyr::data_frame(RSF = c('A', 'B', 'C', 'H', 'L', 'M',  'P'),
                                     Lignes = c(nrow(rsf_A),
                                                nrow(rsf_B),
                                                nrow(rsf_C),
                                                nrow(rsf_H),
                                                nrow(rsf_L),
                                                nrow(rsf_M),
                                                nrow(rsf_P)))))
  }
  if (tolower_names){
    names(rsf_A) <- tolower(names(rsf_A))
    names(rsf_B) <- tolower(names(rsf_B))
    names(rsf_C) <- tolower(names(rsf_C))
    names(rsf_H) <- tolower(names(rsf_H))
    names(rsf_L) <- tolower(names(rsf_L))
    names(rsf_M) <- tolower(names(rsf_M))
    names(rsf_P) <- tolower(names(rsf_P))
  }
  
  r_ii <- list("A" = rsf_A,
               "B" = rsf_B,
               "C" = rsf_C,
               "H" = rsf_H,
               "L" = rsf_L,
               "M" = rsf_M,
               "P" = rsf_P)
  
  attr(r_ii,"problems") <- synthese_import
  return(r_ii)
  
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
