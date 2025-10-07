

atih_mappings <-   tibble::tribble(
  ~debut, ~fin, ~champ_,~outil_atih, ~pmsi_formatter, ~zip_formatter,
  "201101", "202304", "mco", "genrsa",  "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipfratime}.{ziptype}.zip",
  "202305", "202312", "mco", "genrsa",  "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipisotime}.{ziptype}.zip",
  "202401", "202507", "mco", "druides", "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipisotime}.{ziptype}.zip",
  "202508", "209912", "mco", "druides", "{finess}.{annee}.{mois2}", "{finess}.{annee}.{mois2}.MCO.SEJOURS.SEJOURS.{zipisotime}.{ziptype}.zip",
  
  "201101", "202304", "mco.rsface", "preface",  "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipfratime}.{ziptype}.zip",
  "202305", "202312", "mco.rsface", "preface",  "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipisotime}.{ziptype}.zip",
  "202401", "202507", "mco.rsface", "druides", "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipisotime}.{ziptype}.zip",
  "202508", "209912", "mco.rsface", "druides", "{finess}.{annee}.{mois2}", "{finess}.{annee}.{mois2}.MCO.RSFACE.RSFACE.{zipisotime}.{ziptype}.zip",
  
  "200000", "202412", "psy", "pivoine", "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipisotime}.{ziptype}.zip",
  "202501", "209912", "psy", "druides", "{finess}.{annee}.{mois2}", "{finess}.{annee}.{mois2}.PSY.SEJOURS.SEJOURS.{zipisotime}.{ziptype}.zip",
  
  "201101", "202407", "ssr", "genrha",  "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipfratime}.{ziptype}.zip",
  "202408", "209912", "ssr", "druides", "{finess}.{annee}.{mois2}", "{finess}.{annee}.{mois2}.SMR.SEJOURS.SEJOURS.{zipisotime}.{ziptype}.zip",
  
  "201101", "202507", "had", "paprica", "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois}.{zipfratime}.{ziptype}.zip",
  "202508", "209912", "had", "druides", "{finess}.{annee}.{mois}", "{finess}.{annee}.{mois2}.HAD.SEJOURS.SEJOURS.{zipisotime}.{ziptype}.zip",
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
                  champ_ == champ) %>% 
    dplyr::mutate_all(as.character)
  
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


pmsi_check_archive_name <- function(archive_name, zip_formatter){

  regex_test <- glue::glue(zip_formatter, 
                           finess = '[0-9]{9}', 
                           annee = '20[0-9]{2}', 
                           mois2 = '[0-9]{1,2}', mois = '[0-9]{1,2}',  
                           zipfratime = "[0-9]{14}", 
                           ziptype = "(in|out)", 
                           zipisotime = "[0-9]{14}") %>% 
    stringr::str_replace_all("\\.", "\\\\.")
  
  stringr::str_detect(archive_name, regex_test)
}

pmsi_strip_month <- function(file_name){
  file_name %>% 
    stringr::str_replace('\\.0([0-9])\\.', ".\\1.")
}

