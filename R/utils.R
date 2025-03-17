

atih_mappings <-   tibble::tribble(
  ~debut, ~fin, ~champ_,~outil_atih, ~pmsi_formatter, 
  "201101", "202312", "mco", "genrsa",  "{finess}.{annee}.{mois}",
  "202401", "209912", "mco", "druides", "{finess}.{annee}.{mois}",
  
  "200000", "202412", "psy", "pivoine", "{finess}.{annee}.{mois}",
  "202501", "209912", "psy", "druides", "{finess}.{annee}.{mois2}",
  
  "201101", "202407", "ssr", "genrha",  "{finess}.{annee}.{mois}",
  "202408", "209912", "ssr", "druides", "{finess}.{annee}.{mois2}",
  
  "201101", "209912", "had", "paprica", "{finess}.{annee}.{mois}"
)

pmsi_check_periode <- function(annee, mois, champ = "mco"){
  
  if (annee < 2011 | annee > 2025){
    stop('Année PMSI non prise en charge\n')
  }
  if (mois < 0 | mois > 12){
    stop('Mois incorrect\n')
  }
  
  mois2 <- stringr::str_pad(mois, 2, "left", "0")
  an_mois <- glue::glue("{annee}{mois2}")
  
  atih_mappings %>% 
    dplyr::mutate(input = an_mois, 
                  mois2 = mois2,
                  mois = mois,
                  annee = annee) %>% 
    dplyr::filter(dplyr::between(input, debut, fin),
                  champ_ == champ)
  
}

# 
# pmsi_check_periode(2023L, 2L, 'mco') 
# 
# pmsi_check_periode(2025L, 2L, 'mco')
# 
# pmsi_check_periode(2024L, 2L, 'ssr') 
# 
# pmsi_check_periode(2025L, 2L, 'ssr') 
# 

pmsi_glue_fullname <- function(finess, annee, mois, champ, pmsi_extension){
  pmsi_check_periode(annee, mois, champ) %>%
    dplyr::mutate(pmsi_basename = glue::glue(pmsi_formatter)) %>%
    glue::glue_data('{pmsi_basename}.{pmsi_extension}') %>%
    glue::glue()
}

# 
# pmsi_glue_fullname('290000017', 2025L, 2L, 'mco', 'rss.ini.txt')
# 
# pmsi_glue_fullname('750712184', 2025L, 2L, 'ssr', 'rhs.ini.txt')
# 
