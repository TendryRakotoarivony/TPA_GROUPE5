beeline -u jdbc:hive2://localhost:10000 vagrant

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