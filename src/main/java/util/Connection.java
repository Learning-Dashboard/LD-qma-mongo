package util;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import evaluation.Factor;
import evaluation.StrategicIndicator;

public class Connection {

    private static MongoClient mongoClient;
    private static MongoDatabase mongoDatabase;

    /**
    * This method creates the connection to the MongoDB container and databases.
    *
    * @param ip IP where MongoDB is available. [MANDATORY]
    * @param port PORT where the MongoDB services are available. [MANDATORY]
    * @param username credentials when MongoDB requires them. [OPTIONAL]
    * @param password credentials when MongoDB requires them. [OPTIONAL]
    *
    */

    public static void initConnection(String ip, int port, String databaseName, String username, String password) {
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

    public static void closeConnection() {
        if (mongoClient != null) mongoClient.close();
        Factor.resetFactorsIDNames();
        StrategicIndicator.resetFactorsIDNames();
    }

}
