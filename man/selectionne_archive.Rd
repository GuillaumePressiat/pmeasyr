% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/archives.R
\name{selectionne_archive}
\alias{selectionne_archive}
\title{Selectionne une archive in/out}
\usage{
selectionne_archive(
  finess,
  mois,
  annee,
  dossier_archives,
  type_archive = "in",
  recent = TRUE
)
}
\arguments{
\item{finess}{Finess de l'archive.}

\item{mois}{Mois de l'archive.}

\item{annee}{Année de l'archive.}

\item{dossier_archives}{Chemin vers le dossier contenant les archives. Vecteur de caractère de longueur 1.}

\item{type_archive}{Type d'archive : \emph{in} (par défaut) ou \emph{out}}

\item{recent}{par défaut \code{TRUE}, l'archive la plus recente sera utilisee, sinon propose à l'utilisateur de choisir quelle archive dezipper}
}
\description{
Selectionne une archive in/out
}
\seealso{
\code{\link[=adezip.default]{adezip.default()}} et \code{\link[=parse_nom_fichier]{parse_nom_fichier()}}
}
\keyword{internal}
