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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.util.JSONParseException;


public class MongoDBClient {


    private MongoCollection<Document> collection;


    public MongoDBClient(String host, Integer port, String user, String password,
        String dbName, String collectionName, String connectionString) throws IOException {
        String uri = MongoDBConnectionLocator.buildConnectionURI(host, port, user, password, dbName, connectionString);
        MongoClientURI mongoURI = new MongoClientURI(uri);
        MongoClient mongoClient = MongoDBConnectionLocator.getConnection(host, port, user, password, dbName, connectionString, uri, mongoURI);
       
        String databaseName=dbName;
        if(StringUtils.isNotBlank(connectionString)){//Connection with connection string parameter
            databaseName=mongoURI.getDatabase();
        }
        
        checkDB(mongoClient, databaseName);
        MongoDatabase db= mongoClient.getDatabase(databaseName);
       
        checkCollection(db, collectionName);
        this.collection = db.getCollection(collectionName);
        
    }

    private static void checkDB(MongoClient mongoClient, String dbName)
        throws IOException {
       
            MongoIterable<String> dbs = mongoClient.listDatabaseNames();
            Boolean existDb= false;
            
            MongoCursor<String> iterator=dbs.iterator();
            while (iterator.hasNext()) {
                if(iterator.next().equals(dbName)){
                   existDb= true; 
                }
            }
            if(!existDb){
                throw new IOException("Unknown database: '" + dbName + "'");
            }
    }

    private static void checkCollection(final MongoDatabase db, final String collectionName) throws IOException {
        if (db.getCollection(collectionName)==null) {
            throw new IOException("Unknown collection: '" + collectionName + "'");
        }
    }
    
    public MongoCollection<Document> getCollection() {
        return this.collection;
    }

    public  FindIterable<Document>  query(Bson query, Bson orderBy) {

       
        
        // An empty (or null) query document ({}) selects all documents in the collection.
        FindIterable<Document> cursor = this.collection.find(query);
        cursor.sort(orderBy);

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

}
