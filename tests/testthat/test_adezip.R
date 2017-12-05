library(testthat)
context("Dezippage")

test_that("adezip sélectionne le bon fichier", {
  tmp_dir <- tempdir()
  test_files_dir <- system.file("extdata", "test_data", "test_adezip", package = "pmeasyr")
  archive_test <- "671234567.2016.1.12032016140012.in"
  pmeasyr:::adezip.default(finess = "671234567", annee = 2016, mois = 1, path = test_files_dir, pathto = tmp_dir, type = "in")
  expect_equal(dir(tmp_dir, pattern = "\\.in$"), archive_test)
  expect_equal(dir(file.path(tmp_dir, archive_test)), "671234567.2016.1.12032016140012.rss.txt")
}
)
