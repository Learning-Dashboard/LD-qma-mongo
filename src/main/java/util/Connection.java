package util;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import evaluation.Factor;
import evaluation.StrategicIndicator;

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
    * @param username Credentials when MongoDB requires them. [OPTIONAL]
    * @param password Credentials when MongoDB requires them. [OPTIONAL]
    */
    public static void initConnection(String ip, int port, String databaseName, String username, String password) {
        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.WARNING);
        String connectionString;
        if (username != null) connectionString = "mongodb://" + username + ":" + password + "@" + ip + ":" + port;
        else connectionString = "mongodb://" + ip + ":" + port;
        mongoClient = MongoClients.create(connectionString);
        mongoDatabase = mongoClient.getDatabase(databaseName);
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
