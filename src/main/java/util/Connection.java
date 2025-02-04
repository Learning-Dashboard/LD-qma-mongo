package util;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import evaluation.Factor;
import evaluation.StrategicIndicator;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Connection {

    private static MongoClient mongoClient;
    private static MongoDatabase mongoDatabase;

    /**
    * This method creates the connection to the MongoDB container and databases.
    *
    * @param ip IP where MongoDB is available. [MANDATORY]
    * @param port PORT where the MongoDB services are available. [MANDATORY]
    * @param databaseName DATABASE name where all the operations are going to be performed. [MANDATORY]
    * @param username Credentials when MongoDB requires them. [OPTIONAL]
    * @param password Credentials when MongoDB requires them. [OPTIONAL]
    */
    public static void initConnection(String ip, int port, String databaseName, String username, String password) {
        Logger mongodbLogger = Logger.getLogger("org.mongodb.driver");
        mongodbLogger.setLevel(Level.WARNING);
        String connectionString;

        if (username != null && !username.isEmpty() && password != null && !password.isEmpty())
            connectionString = "mongodb://" + username + ":" + password + "@" + ip + ":" + port;
        else connectionString = "mongodb://" + ip + ":" + port;
        mongoClient = MongoClients.create(connectionString);
        mongoDatabase = mongoClient.getDatabase(databaseName);

        try {
            Bson command = new BsonDocument("ping", new BsonInt64(1));
            mongoDatabase.runCommand(command);
            System.out.println("Successfully connected to MongoDB");
        } catch (MongoException e) {
            e.printStackTrace();
            System.err.println("Error connecting to to MongoDB");
            System.exit(1);
        }
    }

    public static MongoClient getMongoClient() {
        return mongoClient;
    }

    public static MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    /**
    * This method closes the previously created connection to the MongoDB container and databases (if any).
    */
    public static void closeConnection() {
        if (mongoClient != null) mongoClient.close();
        Factor.resetFactorsIDNames();
        StrategicIndicator.resetIndicatorsIDNames();
    }

}
