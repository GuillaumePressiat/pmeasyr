# pmeasyr 0.2.4

* Ajout de fonctions pour la valorisation des rsa, voir vignette correspondante
* Ajout d'une fonction pour importer les pie out
* Lien possible vers le package [nomensland](https://guillaumepressiat.github.io/nomensland/index.html)

# pmeasyr 0.2.0


### Fonctions d'imports

* Un nouveau parametre `tolower_names` permet de choisir des noms de colonnes en minuscules

* Les colonnes ghm, anseqta desormais presentes en sortie de irsa

* La colonne DUREESEJPART desormais presente en sortie de irum

* En cas de problème rencontré à la lecture du fichier pmsi, un attribut problems est accessible sur la table importée `obj` en appelant `readr::problems(obj)`. C'est un cas relativement fréquent lorsqu'aucune donnée de facturation n'est disponible pour un patient : dans le fichier ano, des XXXX sont présents à la place de données numériques.

### Vers la base de donnees

* de nouvelles fonctions `db_mco_in`, `db_mco_out`, `db_rsf_out` et `db_generique` permettent d'integrer les tables de pmeasyr dans une base de donnees `src_`, avec  *dplyr* & *dbplyr*, *DBI*, *MonetDBLite* *RSQLite*, *RPostgreSQL*, etc.

* des fonctions `tbl_mco`, `tbl_rsf`, `tbl_had`, `tbl_ssr`, `tbl_psy` pour se connecter a ses tables

* Une vignette présente ces fonctionalités
