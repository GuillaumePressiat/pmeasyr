% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/VVR_MCO.R
\name{epmsi_mco_sv}
\alias{epmsi_mco_sv}
\title{~ VVR - Reproduire le tableau SV}
\usage{
epmsi_mco_sv(valo, knit = FALSE)
}
\arguments{
\item{valo}{Un tibble résultant de \code{\link{vvr_mco}}}

\item{knit}{à TRUE, appliquer une sortie `knitr::kable` au résultat}
}
\value{
Un tibble similaire au tableau RAV epmsi

Il s'agit d'un tableau similaire au tableau "Récapitulation Activité - Valorisation"
}
\description{
Il s'agit d'un tableau similaire au tableau "Séjours Valorisés"
}
\examples{
\dontrun{
epmsi_mco_sv(valo)
}

}
\author{
G. Pressiat
}
