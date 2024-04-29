package org.example;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;

public class Catalogue {

    public static void createCollCatalogue(MongoDatabase db) {

        Bson marqueType = Filters.type("Marque", BsonType.STRING);
        Bson nomType = Filters.type("Nom", BsonType.STRING);
        Bson puissanceType = Filters.type("Puissance", BsonType.DOUBLE);
        Bson longueurType = Filters.type("Longueur", BsonType.STRING);
        Bson nbPlacesType = Filters.type("NbPlaces", BsonType.INT32);
        Bson nbPortesType = Filters.type("NbPortes", BsonType.INT32);
        Bson Couleurtype = Filters.type("Couleur", BsonType.STRING);
        Bson Occasiontype = Filters.type("Occasion", BsonType.BOOLEAN);
        Bson Prixtype = Filters.type("Prix", BsonType.DOUBLE);


        Bson validator = Filters.and(marqueType, nomType, puissanceType,longueurType,nbPlacesType,nbPortesType,Couleurtype,Occasiontype,Prixtype);

        db.getCollection("Catalogue").drop();
        db.createCollection("Catalogue", new CreateCollectionOptions().validationOptions(new ValidationOptions()
                .validator(validator)));
    }

    public static void insertCatalogue (ArrayList<String[]> catalogueArr, MongoCollection<Document> collection) {
        for (String[] vehicule : catalogueArr) {
            Document doc = new Document()
                    .append("Marque",vehicule[0])
                    .append("Nom",vehicule[1])
                    .append("Puissance",Float.parseFloat(vehicule[2]))
                    .append("Longueur",vehicule[3])
                    .append("NbPlaces",Integer.parseInt(vehicule[4]))
                    .append("NbPortes",Integer.parseInt(vehicule[5]))
                    .append("Couleur",vehicule[6])
                    .append("Occasion",Boolean.parseBoolean(vehicule[7]))
                    .append("Prix",Float.parseFloat(vehicule[8]));

            collection.insertOne(doc);
        }
    }
}
