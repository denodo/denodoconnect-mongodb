/*
 * =============================================================================
 *
 *   This software is part of the denodo developer toolkit.
 *
 *   Copyright (c) 2013, denodo technologies (http://www.denodo.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * =============================================================================
 */
package com.denodo.connect.mongodb.wrapper;

import java.io.IOException;

import javax.net.ssl.SSLContext;

import com.denodo.util.configuration.ConfigurationParametersManager;
import com.denodo.util.denodoplatform.DenodoPlatformUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.util.JSONParseException;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;


public class MongoDBClient {

    private static final Logger logger = Logger.getLogger(MongoDBConnectionLocator.class);


    private MongoClient mongoClient;

    private MongoCollection<Document> collection;

    public MongoDBClient(String host, Integer port, String user, String password,
                         String dbName, String collectionName, String connectionString,
                         boolean ssl, boolean test) throws Exception {

        final String uri =
                MongoDBConnectionLocator.buildConnectionURI(host, port, user, password, dbName, connectionString);

        // We will only use this URI in order to check if SSL was specified in the connection string.
        final MongoClientURI rawURI = new MongoClientURI(uri);
        // We allow for the possibility that SSL was not specified at the checkbox, but it was specified in the URL
        boolean useSSL = (ssl || rawURI.getOptions().isSslEnabled());

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();

        if (useSSL) {

            if (logger.isTraceEnabled()) {
                logger.trace("Enabling SSL/TLS for connection to host: " + rawURI.getHosts());
            }

            if (checkDenodo6AndJava7AndTLS12Enabled()) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Forcing TLSv1.2 in Denodo 6 + Java 7 installation: " + rawURI.getHosts());
                }
                final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
                sslContext.init(null, null, null);
                optionsBuilder = optionsBuilder.socketFactory(sslContext.getSocketFactory());
            }

            optionsBuilder = optionsBuilder.sslEnabled(true);

        }

        final MongoClientURI mongoURI = new MongoClientURI(uri, optionsBuilder);

        final String databaseName = mongoURI.getDatabase();
        if(databaseName==null){
            throw new IllegalArgumentException("Database is mandatory in the Connection parameters");
        }

        this.mongoClient = MongoDBConnectionLocator.getConnection(mongoURI, databaseName, useSSL, test);


        MongoDatabase db = getMongoClient().getDatabase(databaseName);

        if(test){
            checkCollection(db, collectionName);
        }
        this.collection = db.getCollection(collectionName);
        
    }


    private static boolean checkDenodo6AndJava7AndTLS12Enabled() {

        // Only if this is Denodo 6 will this be necessary
        final String denodoPlatformVersion = DenodoPlatformUtil.getPlatformVersion();
        if (denodoPlatformVersion == null || !denodoPlatformVersion.trim().startsWith("6")) {
            return false;
        }

        final String javaVersion = System.getProperty("java.version");
        // This check will fail when Java 70 is released in 2048. Let's hope nobody is still using Denodo 6 by then...
        if (javaVersion == null || !(javaVersion.startsWith("7") || javaVersion.startsWith("1.7"))) {
            return false;
        }

        // This is Denodo 6 on Java 7. Let's check the "com.denodo.parser.connection.http.tlsProtocol" config property
        final String tlsConfigValue =
                ConfigurationParametersManager.getOptionalParameter("com.denodo.parser.connection.http.tlsProtocol");
        return (tlsConfigValue != null && tlsConfigValue.trim().equalsIgnoreCase("TLSv1.2"));

    }


    private static void checkCollection(final MongoDatabase db, final String collectionName) throws IOException {
        
        MongoIterable<String> collectionNames = db.listCollectionNames();
        boolean existCollection= false;
        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                existCollection=true;
                break;
            }
        }
        if (!existCollection) {
            throw new IOException("Unknown collection: '" + collectionName + "'");
        }
    }
    
    public MongoCollection<Document> getCollection() {
        return this.collection;
    }

    public  FindIterable<Document>  query(Bson query, Bson orderBy, Bson projection) {

        // An empty (or null) query document ({}) selects all documents in the collection.
        FindIterable<Document> cursor = this.collection.find(query);
        if (orderBy != null) {
            cursor.sort(orderBy);
        }
        if (projection != null) {
            cursor.projection(projection);
        }

        return cursor;
    }

    public FindIterable<Document> query(String jsonQuery) {

        try {
//             An empty (or null) query document ({}) selects all documents in the collection.
            Document query= new Document();
            if(jsonQuery!=null){
                query= Document.parse(jsonQuery);
            }
            return this.collection.find(query);
        } catch (JSONParseException e) {
            throw new IllegalArgumentException("Invalid query syntax", e);
        }
    }



    public MongoClient getMongoClient() {
        return this.mongoClient;
    }

}
