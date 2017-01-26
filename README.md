# pmeasyr : import de données PMSI 

***Guillaume Pressiat***

***SIMAP / DOMU / AP-HP***

*16 janvier 2017*


# Présentation et installation du package

## Contexte

Comment accéder aux données du Programme de Médicalisation des Systèmes d'Informations (PMSI) directement avec R ? Les données sont souvent traitées dans des logiciels propres au PMSI ou des outils statistiques / bases de données du marché. Ces outils ne permettant pas de réaliser des traitements statistiques et des infographies satisfaisantes, les départements d'information médicale sont amenés à retraiter ces données dans R.

Quelles que soient les qualités reconnues de R (libre, évolutif), ses possibilités (visualisations, modèles statistiques, cartographies, ...), l'une des plus importantes de ses évolutions récentes est la possibilité de manipuler des bases de données de taille importante.

Le package *pmeasyr* s'inscrit dans cette veine et permet de réaliser de façon autonome l'ensemble des traitements dans R. 

## Avantages de R

### Un flux de travail unique

Cette évolution de R permet de concilier ses multiples possibilités avec des bases de données de grande taille. En travaillant avec R et seulement avec lui, on peut mettre en place un flux de travail unique : un seul projet, un seul programme, un seul logiciel. La traçabilité, la reproductibilité et la mise à jour d'une étude sont ainsi facilitées.

Habituellement, le travail avec de multiples logiciels oblige à l'export / import de fichiers entre les différents logiciels, et chaque modification du début du flux de travail génère des fichiers exportés v1, v2, ... 

Avec un flux complet dans R, aucun export / import de fichiers n'est nécessaire. De ce fait, toute nouvelle modification est intégrée au processus de travail global. La localisation de toutes les étapes d'une analyse en un seul point évite les erreurs et la confusion lorsque l'on reprend l'étude ultérieurement. 

### R et le PMSI

Utiliser R pour manipuler le recueil de l'activité hospitalière donne au PMSI la liberté offerte par le logiciel : 

   - les requêtes sur des diagnostics, des codes actes peuvent s'écrire de multiples façons et c'est l'utilisateur qui créé ses propres programmes : ainsi le résultat ne dépend pas d'un tiers expert, l'utilisateur est auteur de la méthode (le résultat compte, mais le chemin aussi)
   - les données sont dans R : prêtes pour des modèles linéaires, logistiques, des classifications...
   - la confrontation des données du in (reflet du codage des établissements) aux données du out (reflet de la valorisation accordée à l'établissement) est facilitée par l'import du fichier tra (cf [vignette](vignettes/vignette.Rmd#tra)), cela peut permettre aux équipes DIM d'améliorer leur recueil
   - le reporting de l'activité en excel, pdf, word, html, ou en créant des applications (shiny)
   - l'utilisation des graphiques pour représenter des volumes d'activités et des cartographies interactives pour visualiser la localisation d'activités, de patientèles, et les flux de patients
   - le partage de projets Rstudio facilite et encourage les travaux en équipe.
   
En dernier ressort, R travaillant en mémoire vive, les exécutions de requêtes sont très rapides. 

### Des outils performants

L'engouement autour de R est lié au développement de packages intuitifs et performants : readr, dplyr, tidyr, magrittr, pour n'en citer que quelques-uns. *pmeasyr* s'appuie sur ces packages pour proposer des imports de données rapides sur des fichiers de taille importante (l'entité juridique de l'AP-HP est prise en charge sans problème avec un ordinateur récent).

Dans le cas de *pmeasyr*, l'import de 100 000 rsa (partie fixe, parsing des passages unités médicales, des diagnostics associés et des actes) nécessite en moyenne 5 secondes avec un processeur i7 -- 16Go de ram.


## Contenu du package

Le package contient des fonctions pour la gestion des archives PMSI en entrée / sortie des logiciels de l'ATIH (in / out) : dézippage, suppression des archives, et des fonctions pour l'import des fichiers des champs PMSI MCO, SSR, HAD, PSY et RSF. 

Il est utilisé depuis un an à l'AP-HP pour des analyses d'activité et la description des prises en charge.

## Installation du package

### depuis github avec devtools

```{r echo = T, eval = F}
devtools::install_github('IM-APHP/pmeasyr')
```
Avec cette commande les dépendances du package et le package sont installés.

## Pour commencer

Une fois le package correctement installé, accéder à la vignette introductive du package, celle-ci donne des exemples de commandes pour les traitements du champ PMSI MCO.

```{r echo = T, eval = F}
browseVignettes('pmeasyr')
```

Ou bien consulter la [vignette en ligne](vignettes/vignette.Rmd).

------------


<p align="center">
  SIMAP / DOMU / AP-HP<br>
  <img src="https://github.com/IM-APHP/pmeasyr/blob/master/logo.jpg" alt="AP-HP Logo" align ="center" style="width: 48"/>
</p>
