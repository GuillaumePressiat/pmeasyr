library(testthat)
context("Dezippage")

dossier_archives <- system.file("extdata", "test_data", "test_adezip", package = "pmeasyr")

test_that("Sélection de l'archive à traiter",{
  x <- selectionne_archive(finess = 123456789, mois = 2, annee = 2016,
                           dossier_archives = dossier_archives)
  expect_is(x, "list")
  expect_equal(x$nom_fichier, "123456789.2016.2.15032016152413.in.zip")
})

test_that("Sélectionne les fichiers dans une archive", {
  une_archive <- file.path(dossier_archives, "123456789.2016.2.15032016152413.in.zip")
  x <- selectionne_fichiers(une_archive, c("ium", "ano"))
  expect_is(x, "character")
  expect_length(x, 2)
 
})

test_that("adezip sélectionne le bon fichier", {
  tmp_dir <- tempdir()
  test_files_dir <- system.file("extdata", "test_data", "test_adezip", package = "pmeasyr")
  pmeasyr:::adezip.default(finess = "123456789", annee = 2016, mois = 2, path = test_files_dir, pathto = tmp_dir, type = "in")
  expect_equal(dir(tmp_dir, pattern = "\\.dmi\\.txt$"), "123456789.2016.2.dmi.txt")
}
)

test_that("Parse les nom d'un fichier",{
  x <- parse_nom_fichier("671234567.2016.1.12032016140012.in.zip")
  expect_equal(x$finess, "671234567")
  expect_equal(x$annee, "2016")
  expect_equal(x$mois, "1")
  expect_is(x$horodatage_production, "POSIXct")
  expect_equal(x$horodatage_production, as.POSIXlt("2016-03-12 14:00:12 CET"))
  
  # Si un fichier hors format, ne donner que le nom du fichier
  expect_length(parse_nom_fichier("mlskjfq.zip"), 1)
})

test_that("Parse le nom de plusieurs fichiers", {
  noms_de_fichiers <- c("671234567.2016.1.12032016140012.in.zip",
                        "671234567.2016.1.rum.txt")
  x <- parse_noms_fichiers(noms_de_fichiers, return_tibble = FALSE) 
  expect_length(x, 2)
  expect_length(x[[2]], 5)
  expect_equal(x[[2]]$type, "rum")
  x <- parse_noms_fichiers(noms_de_fichiers) 
  expect_is(x, "tbl_df")
})

test_that("astat", {
  
  x <- astat(path = dossier_archives, file = "123456789.2016.2.15032016152413.in.zip", view = FALSE)
  expect_is(x, "data.frame")
})
