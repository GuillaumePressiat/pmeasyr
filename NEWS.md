# pmeasyr 0.2.0


### Fonctions d'imports

* Un nouveau parametre `tolower_names` permet de choisir des noms de colonnes en minuscules

* Les colonnes ghm, anseqta desormais presentes en sortie de irsa

* La colonne DUREESEJPART desormais presente en sortie de irum


### Vers la base de donnees

* de nouvelles fonctions `db_mco_in`, `db_mco_out`, `db_rsf_out` et `db_generique` permettent d'integrer les tables de pmeasyr dans une base de donnees `src_`, avec  *dplyr* & *dbplyr*, *DBI*, *MonetDBLite* *RSQLite*, *RPostgreSQL*, etc.

* des fonctions `tbl_mco`, `tbl_rsf`, `tbl_had`, `tbl_ssr`, `tbl_psy` pour se connecter a ses tables

* Bientôt une vignette presentera ces fonctionnalites
