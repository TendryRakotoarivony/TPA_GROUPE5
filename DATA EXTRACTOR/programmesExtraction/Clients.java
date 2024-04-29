
import oracle.kv.KVStore;
import java.util.List;
import java.util.Iterator;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.FaultException;
import oracle.kv.StatementResult;
import oracle.kv.table.TableAPI;
import oracle.kv.table.Table;
import oracle.kv.table.Row;
import oracle.kv.table.PrimaryKey;
import oracle.kv.ConsistencyException;
import oracle.kv.RequestTimeoutException;
import java.lang.Integer;
import oracle.kv.table.TableIterator;
import oracle.kv.table.EnumValue;
import java.io.File;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

public class Clients {
    private final KVStore store;

    public static void main(String args[]) {
        try {
            Clients arb = new Clients(args);
            arb.initClientsTablesAndData(arb);

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    Clients(String[] argv) {

        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";
        final int nArgs = argv.length;
        int argc = 0;

        store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
    }

    public void initClientsTablesAndData(Clients arb) {
        arb.dropTableClients();
        arb.createTableClients();
        arb.loadClientsDataFromCSV("/vagrant/TPA/DATA EXTRACTOR/dataSources/Clients_4.csv");
        arb.loadClientsDataFromCSV("/vagrant/TPA/DATA EXTRACTOR/dataSources/Clients_15.csv");

    }

    public void dropTableClients() {
        String statement = null;
        statement = "drop table Clients";
        executeDDL(statement);
    }

    public void createTableClients() {
        String statement = null;
        statement = "create table Clients ("
                + "ClIENTID INTEGER,"
                + "AGE INTEGER,"
                + "SEXE STRING,"
                + "TAUX INTEGER,"
                + "SITUATION_FAMILIALE STRING,"
                + "NBR_ENFANT INTEGER,"
                + "VOITURE_2 STRING,"
                + "IMMATRICULATION STRING,"
                + "PRIMARY KEY(ClIENTID))";
        executeDDL(statement);

    }

    public void loadClientsDataFromCSV(String clientDataFileName) {
        String line;
        boolean headerSkipped = false;

        System.out.println("Loading clients from CSV...");

        int id = 1;
        try (BufferedReader br = new BufferedReader(new FileReader(clientDataFileName))) {
            while ((line = br.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }
                String[] clientRecord = line.split(",");

                // Check if any field is "N/D", if yes, skip this record
                if (clientRecord[0].equals("N/D") || clientRecord[0].equals("?") || clientRecord[0].equals(" ")
                    || clientRecord[2].equals("N/D") || clientRecord[2].equals("?") || clientRecord[2].equals(" ")
                    || clientRecord[4].equals("N/D") || clientRecord[4].equals("?") || clientRecord[4].equals(" ")) {
                    continue;
                }

                int age = Integer.parseInt(clientRecord[0]);
                String sexe = clientRecord[1];
                int taux = Integer.parseInt(clientRecord[2]);
                String SFamiliale = clientRecord[3];
                int nbEnfantsAcharge = Integer.parseInt(clientRecord[4]);
                String voiture_2 = clientRecord[5];
                String immatriculation = clientRecord[6];

                // Add the client to the KVStore (assuming the method insertAClientRow exists)
                this.insertAClientRow(id, age, sexe, taux, SFamiliale, nbEnfantsAcharge, voiture_2, immatriculation);
                id++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertAClientRow(int clientid, int age, String sexe, int taux, String SFamiliale, int nbEnfantsAcharge,
            String voiture_2, String immatriculation) {
        // TableAPI tableAPI = store.getTableAPI();
        StatementResult result = null;
        String statement = null;
        try {

            TableAPI tableH = store.getTableAPI();
            Table tableClient = tableH.getTable("Clients");

            Row clientRow = tableClient.createRow();

            clientRow.put("CLIENTID", clientid);
            clientRow.put("AGE", age);
            clientRow.put("SEXE", sexe);
            clientRow.put("TAUX", taux);
            clientRow.put("SITUATION_FAMILIALE", SFamiliale);
            clientRow.put("NBR_ENFANT", nbEnfantsAcharge);
            clientRow.put("VOITURE_2", voiture_2);
            clientRow.put("IMMATRICULATION", immatriculation);
            tableH.put(clientRow, null, null);

        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println("Statement couldn't be executed, please retry: " + e);
        }
    }

    public void executeDDL(String statement) {
        TableAPI tableAPI = store.getTableAPI();
        StatementResult result = null;
        try {
            result = store.executeSync(statement);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println("Statement couldn't be executed, please retry: " + e);
        }
    }
}
// CREATE EXTERNAL TABLE IF NOT EXISTS Clients (
// ClIENTID INTEGER,
// AGE INTEGER,
// SEXE STRING,
// TAUX INTEGER,
// SITUATION_FAMILIALE STRING,
// NBR_ENFANT INTEGER,
// VOITURE_2 STRING,
// IMMATRICULATION STRING,
// PRIMARY KEY(ClIENTID)
// )
// STORED BY 'oracle.kv.hadoop.hive.table.TableStorageHandler'
// TBLPROPERTIES (
// "oracle.kv.kvstore" = "kvstore",
// "oracle.kv.hosts" = "localhost:5000",
// "oracle.kv.tableName" = "vehicleTable"
// );
