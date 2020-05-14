
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
  if (annee<2011|annee > 2020){
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
