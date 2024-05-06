# L'extraction des données et leurs traitements

## Source Orcale noSQL

### Import des données
Pour notre projet nous avons décidé de placer les données clients et marketing dans le serveur Oracle NOSQL. Pour cela nous avons développé deux scripts d'extraction : Clients.java et Marketing.java. Ces deux scripts sont présents dans le dossier "programmesExtraction". Pour les utiliser, veuillez suivre les instructions suivantes : 
- placer les fichiers Clients.txt, Marketing.txt, Clients.java et Marketing.java dans le repertoire de la machine vagrant.
- connecter vous à la machine vagrant avec la commande 
```bash 
vagrant ssh
```	
- Définir les variables d'environement
```bash 
export MYTPHOME=/vagrant/TPA/DATA\ EXTRACTOR/programmesExtraction/
export DATAHOME=/vagrant/TPA/DATA\ EXTRACTOR/dataSources
```	

- Démarrer le serveur Oracle NOSQL (KV Store) avec la commande 
```bash
nohup java -Xmx64m -Xms64m -jar $KVHOME/lib/kvstore.jar kvlite -secure-config disable -root $KVROOT &
```
- réaliser l'import de Marketing
```bash
javac -g -cp "$KVHOME/lib/kvclient.jar:$MYTPHOME:." "$MYTPHOME/Marketing.java"
java -cp "$KVHOME/lib/kvclient.jar:$MYTPHOME:." Marketing

```
- réaliser l'import de Clients
```bash
javac -g -cp "$KVHOME/lib/kvclient.jar:." "$MYTPHOME/Clients.java"
java -cp "$KVHOME/lib/kvclient.jar:$MYTPHOME" Clients
    
```
Nous allons maintenant créer les tables externes sur HIVE pour pouvoir accéder aux données.

### Création des tables externes sur HIVE
Pour commencer, nous devons démarrer le serveur HIVE avec les commandes suivantes :  
```bash
start-dfs.sh
```
```bash
start-yarn.sh
```
```bash
nohup hive --service metastore > /dev/null &
```
```bash
nohup hiveserver2 > /dev/null &
```
- maintenant nous accédons à la console HIVE avec la commande : (il est possible que la commande ne fonctionne pas directement à cause du temps de lancement le mieux est d'attendre quelques secondes et de lancer la commande)
```bash
beeline -u jdbc:hive2://localhost:10000 vagrant
```
```bash
USE DEFAULT;
```
- script de création de la table Marketing

```bash
CREATE EXTERNAL TABLE IF NOT EXISTS MARKETING_H_EXT (
MARKETINGID INTEGER,
AGE INTEGER,
SEXE STRING,
TAUX INTEGER,
SITUATION_FAMILIALE STRING,
NBR_ENFANT INTEGER,
VOITURE_2 STRING
)
STORED BY 'oracle.kv.hadoop.hive.table.TableStorageHandler'
TBLPROPERTIES (
"oracle.kv.kvstore" = "kvstore",
"oracle.kv.hosts" = "localhost:5000",
"oracle.kv.tableName" = "Marketing"
);
```

- script de création de la table Clients

```bash
CREATE EXTERNAL TABLE IF NOT EXISTS Clients_H_EXT (
ClIENTID INTEGER,
AGE INTEGER,
SEXE STRING,
TAUX INTEGER,
SITUATION_FAMILIALE STRING,
NBR_ENFANT INTEGER,
VOITURE_2 STRING,
IMMATRICULATION STRING
)
STORED BY 'oracle.kv.hadoop.hive.table.TableStorageHandler'
TBLPROPERTIES (
"oracle.kv.kvstore" = "kvstore",
"oracle.kv.hosts" = "localhost:5000",
"oracle.kv.tableName" = "clients"
);
```

## Source Mongo DB

### Import des données

Pour notre projet nous avons décidé de placer les données de Catalogue et d'Immatriculations dans le serveur Mongo DB.

Pour réaliser l'import des données on va utiliser l'utilitaire mongoimport.

On lance MongoDB : 

```bash
sudo systemctl start mongod
```

On se connecte ensuite au MongoDB Client

```bash
mongo
```

On execute la serie des commandes suivante :

```bash
// Créer la BDD TPA
use TPA
// Créer les deux collections "Immatriculation" "Catalogue" :
db.createCollection("Immatriculation")
db.createCollection("Catalogue")
// Verifier les collections
show collections
//On quitte le mongo shell
quit()
```
Ensuite dans le bash du vagrant on lance la commande :

```bash
//On accede au repertoire ou se trouve nos CSV, dans notre cas, les fichiers sont dans le dossier partagé de Vagrant :
cd /vagrant
```

Ensuite, on lance la commande pour importer les données pour Catalogue :

```bash
mongoimport -d TPA -c Catalogue --type=csv --file="$DATAHOME/Catalogue.csv"  --headerline

```

De meme pour Immatriculation : 

```bash
mongoimport -d TPA -c Immatriculation --type=csv --file="$DATAHOME/Immatriculations.csv" --headerline

```

On peut verifier que les donnees on ete bien importees :

```bash
mongo
use TPA
db.Catalogue.find({})
db.Immatriculation.find({})
```

### Création des tables externes sur HIVE

Pour démarrer et accéder à la console HIVE il faut suivre les mêmes instructions que pour la source Oracle NOSQL.

- script de création de la table Catalogue

```bash
CREATE EXTERNAL TABLE catalogue_h_ext ( 
id STRING, 
Marque STRING,
Nom STRING,
Puissance DOUBLE,
Longueur STRING,
NbPlaces INT,
NbPortes INT,
Couleur STRING,
Occasion STRING,
Prix DOUBLE )
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"id":"_id", "marque":"marque", "nom" : "nom", "puissance": "puissance", "longueur" : "longueur", "nbPlaces" : "nbPlaces", "nbPortes" : "nbPortes", "couleur" : "couleur", "occasion" : "occasion", "prix" : "prix"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/TPA.Catalogue');
```

- script de création de la table Immatriculation

```bash
CREATE EXTERNAL TABLE immatriculation_h_ext ( 
id STRING,
Immatriculation STRING, 
Marque STRING,
Nom STRING,
Puissance DOUBLE,
Longueur STRING,
NbPlaces INT,
NbPortes INT,
Couleur STRING,
Occasion STRING,
Prix DOUBLE )
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"id":"_id", "immatriculation":"immatriculation", "marque":"marque", "nom" : "nom", "puissance": "puissance", "longueur" : "longueur", "nbPlaces" : "nbPlaces", "nbPortes" : "nbPortes", "couleur" : "couleur", "occasion" : "occasion", "prix" : "prix"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/TPA.Immatriculation');
```

## Hadoop Distributed File System (HDFS)

### Import des données

Tout d'abord il faut mettre le fichier CO2.csv dans la machine virtuelle
Ensuite assurez vous d'avoir lancer hdfs et yarn (si c'est déjà fait vous pouvez sauter cette étape) :
```bash
start-dfs.sh
```
```bash
start-yarn.sh
```
```bash
nohup hive --service metastore > /dev/null &
```
```bash
nohup hiveserver2 > /dev/null &
```
Mettre le fichier dans HDFS via la commande :

```bash
hadoop fs -rmr input/*
hadoop fs -mkdir input
hadoop fs -put /vagrant/TPA/CO2.csv input/CO2.csv
```

Pour être sûr qu'il n'existe pas de output déjà créer :
```bash
hadoop fs -rmr output/*
```

Maintenant que le fichier est dans HDFS, vous pouvez lancer le script python qui va modifier le csv pour le nettoyer :

```bash
spark-submit  "$MYTPHOME/HDFS/src/co2Reader.py"
```


- Maintenant nous accédons à la console HIVE avec la commande : 
```bash
beeline -u jdbc:hive2://localhost:10000 vagrant
```
```bash
USE DEFAULT;
```
Puis nous allons faire un lien externe vers ce fichier avec HIVE
Pour être sûr que la table n'existe pas déjà :
```bash
drop table CO2_HDFS_H_EXT;
```
Maintenant on crée notre table externe :
```bash
CREATE EXTERNAL TABLE  CO2_HDFS_H_EXT  (MARQUE STRING,  MALUSBONUS FLOAT, REJET FLOAT, COUTENERGIE FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'output/TransformationCO2';
```
Et voilà maintenant les données devraient être accessible depuis une commande comme celle-ci :
```bash
Select * from CO2_HDFS_H_EXT;
```
