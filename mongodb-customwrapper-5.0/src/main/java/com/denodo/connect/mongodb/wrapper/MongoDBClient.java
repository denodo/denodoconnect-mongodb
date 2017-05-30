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

import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.util.JSONParseException;


public class MongoDBClient {
    
    private MongoClient mongoClient;

    private MongoCollection<Document> collection;

    public MongoDBClient(String host, Integer port, String user, String password,
        String dbName, String collectionName, String connectionString, Boolean test) throws Exception {
        String uri = MongoDBConnectionLocator.buildConnectionURI(host, port, user, password, dbName, connectionString);
        MongoClientURI mongoURI = new MongoClientURI(uri);
        String databaseName=dbName;
        if(StringUtils.isNotBlank(connectionString)){//Connection with connection string parameter
            databaseName=mongoURI.getDatabase();
            if(databaseName==null){
                throw new IllegalArgumentException("Database is mandatory in the Connection String parameter: [mongodb://]host1[:port1][,host2[:port2],...[,hostN[:portN]]]/database[?options]");
            }
        }
        
        this.mongoClient = MongoDBConnectionLocator.getConnection(host, port, user, password, databaseName, connectionString, uri, mongoURI, test);


        MongoDatabase db=getMongoClient().getDatabase(databaseName);

        if(test){
            checkCollection(db, collectionName);
        }
        this.collection = db.getCollection(collectionName);
        
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
