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
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;


public final class MongoDBConnectionLocator {

    private static final Logger logger = Logger.getLogger(MongoDBConnectionLocator.class);
    private static final String PREFIX="mongodb://";
    private static final ConcurrentMap<String, MongoClient> mongoCache =
        new ConcurrentHashMap<String, MongoClient>();

    // Suppress default constructor for noninstantiability
    private MongoDBConnectionLocator() {

    }


    /**
     * Attempts to find an existing MongoClient instance matching that URI
     * and returns it if exists. Otherwise creates a new MongoClient instance.
     * @throws Exception 
     */
    public static MongoClient getConnection(String host, Integer port, String user, String password,
        String db, String connectionString, String uri,  MongoClientURI mongoURI, Boolean test) throws Exception {

        try {
        
            MongoClient client = mongoCache.get(uri);
            if (client == null) {
                if(!StringUtils.isNotBlank(connectionString)){
                logger.debug("MongoDB connection for host:" + host + " port:" + port + " user:" + user
                    + " db:" + db + " does not exist, creating it... ");
                }else{
                    logger.debug("MongoDB connection with connection string: "+connectionString+" user:" + user
                    + " does not exist, creating it... ");
                }
                client = new MongoClient(mongoURI);
                MongoClient temp = mongoCache.putIfAbsent(uri, client);
                if (temp != null) {

                    // lost the race condition, close new mongoclient and return the
                    // previous one
                    client.close();
                    client = temp;
                }
            }
            if(test){//check the connection
                testConnection(uri, client,db);
            }

            return client;

        } catch (UnknownHostException e) {
            throw new IOException("Unknown host '" + e.getMessage() + "'", e);
        } catch (MongoException e) {
            throw new IOException("Connection error: " + e.getMessage(), e);
        }
        
    }

    /**
     * The format of the URI is:
     * mongodb://[username:password@]host1[:port1],...[,hostN[:portN]]][/[database][?options]].
     *
     * If the username:password syntax is used the name of the database to login to
     * should be specified, otherwise the "admin" database will be used by default and login could fail.
     *
     * If the username:password syntax is not used the db name should not be used
     * to build the URI, to avoid using one MongoClient instance for each db of the MongoDB node.
     *
     */
    public static String buildConnectionURI(String host, Integer port, String user,
            String password, String db, String connectionString) {

        if(StringUtils.isNotBlank(connectionString)){
            StringBuilder uri = new StringBuilder();
            if(!connectionString.startsWith(PREFIX)){
                uri.append(PREFIX);
                if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password)) {
                    uri.append(user).append(':').append(password).append('@');

                }
                uri.append(connectionString);
            }else{
                uri.append(connectionString);
                if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password)) {
                    uri.insert(10,user+":"+password+"@");
                }
            }
            return uri.toString(); 
        }else{
            boolean auth = false;
            StringBuilder uri = new StringBuilder(PREFIX);

            if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password)) {
                uri.append(user).append(':').append(password).append('@');
                auth = true;
            }
            uri.append((host != null) ? host : ServerAddress.defaultHost());
            uri.append(':');
            uri.append((port != null) ? port.intValue() : ServerAddress.defaultPort());

            if (auth) {
                uri.append('/').append(db);
            }
            logger.debug("Connection uri: "+ uri.toString());
            return uri.toString();
        }
    }

    /*
     * MongoClient constructor does not actually connect to the server: a connection
     * is obtained from the pool only when a request (ie. an operation as find, insert, ...)
     * is sent to the database. So getDatabaseNames() is invoked to test for database connectivity.
     */
    public static void testConnection(String uri, MongoClient client, String dbName) throws Exception {

        try {

            MongoDatabase database = client.getDatabase(dbName);
         
          if(database.listCollections()==null || database.listCollections().first()==null){
              clearConnection(uri, client);
              logger.debug("Error connecting to database: '" +dbName + "' ");
              //  mongoClient.close();
              throw new Exception("Error connecting to database: '" + dbName + "' " );  
          }
            
            //MongoIterable<String> strings=client.listDatabaseNames();
        } catch ( MongoSocketException e) {
            logger.debug("Unable to establish connection",e);
            clearConnection(uri, client);
            throw new IOException("Unable to establish connection", e);
        } catch (MongoCommandException e) {
            if (e.getMessage().contains("auth fails")) {
                clearConnection(uri, client);
                throw new IOException("Authentication error: wrong user/password", e);
            }
            // when user credentials were provided and the user do not have enough privileges
            // the workaround to test the connection will fail but this exception will not be thrown
            if (!e.getMessage().contains("unauthorized")) {
                clearConnection(uri, client);
                throw e;
            }
        }catch (Exception e) {
            clearConnection(uri, client);
            logger.debug("Unable to establish connection",e);
            throw new Exception("Unable to establish connection", e);
            
          
        }

    }

    private static void clearConnection(String uri, MongoClient client) {
        client.close();
        mongoCache.remove(uri, client);
    }

}
