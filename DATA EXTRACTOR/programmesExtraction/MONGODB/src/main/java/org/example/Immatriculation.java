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

public class Immatriculation {

    public static void createCollImmatriculation(MongoDatabase db) {

        Bson immatriculationType = Filters.type("Immatriculation", BsonType.STRING);
        Bson marqueType = Filters.type("Marque", BsonType.STRING);
        Bson nomType = Filters.type("Nom", BsonType.STRING);
        Bson puissanceType = Filters.type("Puissance", BsonType.DOUBLE);
        Bson longueurType = Filters.type("Longueur", BsonType.STRING);
        Bson nbPlacesType = Filters.type("NbPlaces", BsonType.INT32);
        Bson nbPortesType = Filters.type("NbPortes", BsonType.INT32);
        Bson Couleurtype = Filters.type("Couleur", BsonType.STRING);
        Bson Occasiontype = Filters.type("Occasion", BsonType.BOOLEAN);
        Bson Prixtype = Filters.type("Prix", BsonType.DOUBLE);


        Bson validator = Filters.and(immatriculationType, marqueType, nomType, puissanceType,longueurType,nbPlacesType,nbPortesType,Couleurtype,Occasiontype,Prixtype);

        db.getCollection("Immatriculation").drop();
        db.createCollection("Immatriculation", new CreateCollectionOptions().validationOptions(new ValidationOptions()
                .validator(validator)));
    }

    public static void insertImmatriculation (ArrayList<String[]> immatriculationArr, MongoCollection<Document> collection) {
        for (String[] vehicule : immatriculationArr) {
            Document doc = new Document()
                    .append("Immatriculation",vehicule[0])
                    .append("Marque",vehicule[1])
                    .append("Nom",vehicule[2])
                    .append("Puissance",Float.parseFloat(vehicule[3]))
                    .append("Longueur",vehicule[4])
                    .append("NbPlaces",Integer.parseInt(vehicule[5]))
                    .append("NbPortes",Integer.parseInt(vehicule[6]))
                    .append("Couleur",vehicule[7])
                    .append("Occasion",Boolean.parseBoolean(vehicule[8]))
                    .append("Prix",Float.parseFloat(vehicule[9]));

            collection.insertOne(doc);
        }
    }

}
