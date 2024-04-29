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



