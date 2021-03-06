% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/raa.R
\name{iraa}
\alias{iraa}
\title{~ PSY - Import des raa}
\usage{
iraa(finess, annee, mois, path, lib = T, tolower_names = F, ...)
}
\arguments{
\item{finess}{Finess du Out a importer : dans le nom du fichier}

\item{annee}{Annee PMSI (nb) des donnees sur 4 caracteres (2016)}

\item{mois}{Mois PMSI (nb) des données (janvier : 1, decembre : 12)}

\item{path}{Localisation du fichier de donnees}

\item{lib}{Ajout des libelles de colonnes aux tables, par defaut a \code{TRUE} ; necessite le package \code{sjlabelled}}

\item{tolower_names}{a TRUE les noms de colonnes sont tous en minuscules}

\item{~...}{parametres supplementaires a passer
dans la fonction \code{\link[readr]{read_fwf}}, par exemple
\code{n_max = 1e3} pour lire les 1000 premieres lignes,  \code{progress = F, skip = 1e3}}
}
\value{
Une table (data.frame, tibble) contenant les données raa.
}
\description{
Import du fichier raa
}
\details{
Formats depuis 2012 pris en charge
Structure du nom du fichier attendu (sortie de Pivoine) :
\emph{finess.annee.moisc.rpa.txt}

\strong{750712184.2016.3.rpa.txt}
}
\examples{
\dontrun{
   raa <- iraa('750712184',2015,12,"~/Documents/data/psy")
}

}
\seealso{
\code{\link{irpsa}},
utiliser un noyau de parametres avec \code{\link{noyau_pmeasyr}}
}
\author{
G. Pressiat
}
