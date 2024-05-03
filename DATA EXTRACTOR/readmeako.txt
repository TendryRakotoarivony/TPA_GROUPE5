KVHOME="/vagrant/kvstore/lib"
export MYTPHOME=/vagrant/TPA/DATA\ EXTRACTOR/programmesExtraction/

javac -g -cp "$KVHOME/lib/kvclient.jar:." "$MYTPHOME/Marketing.java"
java -cp "$KVHOME/lib/kvclient.jar:$MYTPHOME" Marketing

-- client
javac -g -cp "$KVHOME/lib/kvclient.jar:." "$MYTPHOME/Clients.java"
java -cp "$KVHOME/lib/kvclient.jar:$MYTPHOME" Clients

-- creation table externe hive
start-dfs.sh
start-yarn.sh
nohup hive --service metastore > /dev/null &
nohup hiveserver2 > /dev/null &
beeline -u jdbc:hive2://localhost:10000 vagrant
USE DEFAULT;


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


### Création des tables externes de Mongo DB sur HIVE

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
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"id":"_id", "Marque":"Marque", "Nom" : "Nom", "Puissance": "Puissance", "Longueur" : "Longueur", "NbPlaces" : "NbPlaces", "NbPortes" : "NbPortes", "Couleur" : "Couleur", "Occasion" : "Occasion", "Prix" : "Prix"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/MBDSTPA.Catalogue');
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
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"id":"_id", "Immatriculation":"Immatriculation", "Marque":"Marque", "Nom" : "Nom", "Puissance": "Puissance", "Longueur" : "Longueur", "NbPlaces" : "NbPlaces", "NbPortes" : "NbPortes", "Couleur" : "Couleur", "Occasion" : "Occasion", "Prix" : "Prix"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/MBDSTPA.Immatriculation');
```


