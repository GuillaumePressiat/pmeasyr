# Contribuer au développement de pmeasyr

L'objectif de ce guide est de vous aider à contribuer au développement ce ce package.

## Règles de codage

Afin que le code soit lisible et reste maintenable, il est important d'avoir un code écrit de manière homogène. Pour ce faire, les contributeurs de ce package suivent autant que possible les conseils proposés dans les ouvrages [R packages](http://r-pkgs.had.co.nz/) et [Advanced R](http://adv-r.had.co.nz/).

Ce package utilise le Français comme langue principale. Les noms d'objet doivent être écrit en Français en se limitant aux caractères ASCII. Pour une prise en charge correcte des caracètres hors ASCII, tous les fichiers doivent être enregistrés avec l'encodage UTF-8.

## Documentation

La documentation interne du package utilise Roxygen 6 ou supérieur. La documentation peut, pour des raisons de facilité de lecture, utiliser la [syntaxe markdown](https://cran.r-project.org/web/packages/roxygen2/vignettes/markdown.html) en ajoutant le tag `@md` aux blocs de code utilisant markdown.