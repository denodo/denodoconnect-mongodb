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
import java.util.List;

import com.mongodb.CommandFailureException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;


public class MongoDBClient {


    private DBCollection collection;


    public MongoDBClient(String host, Integer port, String user, String password,
        String dbName, String collectionName) throws IOException {

        MongoClient mongoClient = MongoDBConnectionLocator.getConnection(host, port, user, password, dbName);

        checkDB(mongoClient, dbName);
        DB db = mongoClient.getDB(dbName);

        checkCollection(db, collectionName);
        this.collection = db.getCollection(collectionName);
    }

    private static void checkDB(MongoClient mongoClient, String dbName)
        throws IOException {

        try {
            List<String> dbs = mongoClient.getDatabaseNames();
            if (!dbs.contains(dbName)) {
                throw new IOException("Unknown database: '" + dbName + "'");
            }

        } catch (CommandFailureException e) {
            // when user credentials were provided and the user do not have enough privileges
            // the workaround to check for the database will fail but this exception will not be throwed
            if (!e.getMessage().contains("unauthorized")) {
                throw e;
            }
        }
    }

    private static void checkCollection(DB db, String collectionName) throws IOException {
        if (!db.collectionExists(collectionName)) {
            throw new IOException("Unknown collection: '" + collectionName + "'");
        }
    }
    
    public DBCollection getCollection() {
        return this.collection;
    }

    public DBCursor query(DBObject query, DBObject orderBy) {

        // An empty (or null) query document ({}) selects all documents in the collection.
        DBCursor cursor = this.collection.find(query);
        cursor.sort(orderBy);

        return cursor;
    }

    public DBCursor query(String jsonQuery) {

        try {
            // An empty (or null) query document ({}) selects all documents in the collection.
            DBObject query = (DBObject) JSON.parse(jsonQuery);
            return this.collection.find(query);
        } catch (JSONParseException e) {
            throw new IllegalArgumentException("Invalid query syntax", e);
        }
    }

}
