package org.example;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {


        MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27018/");
        MongoClient mongoClient = new MongoClient(connectionString);


        MongoDatabase database = mongoClient.getDatabase("MBDSTPA");

        Catalogue.createCollCatalogue(database);
        Immatriculation.createCollImmatriculation(database);

        MongoCollection<Document> collectionImmatriculation = database.getCollection("Immatriculation");
        MongoCollection<Document> collectionCatalogue = database.getCollection("Catalogue");


        ArrayList<String[]> catalogueArr = new ArrayList<String[]>();
        ArrayList<String[]> immatriculationArr = new ArrayList<String[]>();

        try {
            catalogueArr = CsvReader.getCSV("E:\\vagrant-projects\\TPA\\DATA EXTRACTOR\\dataSources\\Catalogue.csv");
            Catalogue.insertCatalogue(catalogueArr,collectionCatalogue);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        try {
            immatriculationArr.addAll(CsvReader.getCSV("E:\\vagrant-projects\\TPA\\DATA EXTRACTOR\\dataSources\\Immatriculations.csv"));
            Immatriculation.insertImmatriculation(immatriculationArr,collectionImmatriculation);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }



}