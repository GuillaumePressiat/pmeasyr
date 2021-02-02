# pmeasyr 0.2.9

* ano in : lecture pour les 4 champs sur 2011 — 2020
* Psy in : lecture des rps et raa depuis 2012
* SSR in : lecture des rhs depuis 2011
* `dplyr::tbl_df` -> `tibble::as_tibble`
* tutoriel learnr pour démarrer


# pmeasyr 0.2.8

* Ajout de fonctions d'analyse des données MCO : chir ambu GHM C + 7 racines, gestes marqueurs

# pmeasyr 0.2.6

* Ajout de la fonction vvr_had_ght pour valoriser les sous-séquences de l'hospitalisation à domicile comme sur epmsi.

# pmeasyr 0.2.5

* Valorisation des rsa 2019 
* Ajout de spécificités de valorisation (GHS 5907, carT cells), voir [ici](https://im-aphp.github.io/pmeasyr/articles/vignette4.html#remarques-sur-le-mecanisme-de-valorisation), ces ajouts peuvent générer des ruptures dans certains programmes appelant `vvr_rsa` et `vvr_ghs_supp` (désormais on utilise la table um pour taguer les mono RUM UHCD, en particulier il n'est plus possible de valoriser avec des rsa typi = 1, et l'ajout du paramètre mo peut générer des erreurs lors de l'appel à `vvr_ghs_supp` si les paramètres ne sont pas nommés dans l'appel (rupture dans l'ordre des paramètres)).

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
