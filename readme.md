### Mini moteur de données

ce mini moteur de base de données relationnelle est écrit en Go, inspiré de SQLite.
Il permet de créer des bases, des tables, d’insérer et de sélectionner des données, le tout en mémoire avec persistance sur disque.


C’est un projet pédagogique pour comprendre les notions de base des SGBD : indexation, tables, schéma, parsing SQL, et persistance.

### Fonctionnalités
- Gestion de bases de données multiples 
- Tables avec schéma personnalisé (type INT ou STRING, PRIMARY KEY, UNIQUE, NOT NULL)
- Index logique via un B+Tree simplifié 
- Commandes SQL supportées :
  - CREATE DATABASE 
  - USE <database>
  - CREATE TABLE 
  - INSERT INTO 
  - SELECT ... WHERE ... 
- Persistance des données sur disque via fichiers gob 
- Shell interactif minimal


### Notions et concepts abordés

1- Structures de données 
- BPTree : index logique pour une recherche rapide des lignes par clé primaire 
- Table : contient le schéma, les lignes, et l’index 
- Database : gestion des bases et tables en mémoire 

2- Contrainte de schéma 
- PRIMARY KEY : clé unique par table 
- NOT NULL : colonne obligatoire 
- UNIQUE : valeur unique dans la colonne
3- Persistance 
- Sérialisation avec encoding/gob
- Chaque table est sauvegardée dans un fichier .tbl
- Les données sont rechargées en mémoire au lancement

4- Parsing SQL 
- Utilisation de regex pour analyser les commandes SQL
- Création d’un AST simple (Statement interface) avec méthode Exec

5- Shell REPL 
- Lecture ligne par ligne
- Exécution directe des commandes SQL
- Affichage formaté des résultats


### Limitations actuelles
- Le parser ne supporte pas les lignes multiples ni les commentaires
- Pas de support pour UPDATE, DELETE, JOIN ou ORDER BY
- Le B+Tree est logique et simplifié, pas optimisé pour de grandes bases
- Le shell ne supporte que des commandes terminées par ; sur une seule ligne
