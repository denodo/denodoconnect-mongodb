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

import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_EQ;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LIKE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.BSONObject;

import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperOrderByExpression;
import com.denodo.vdb.engine.customwrapper.CustomWrapperOrderByExpression.ORDER;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class MongoDBWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(MongoDBWrapper.class);

    private static Map<String, Integer> sqlTypes = getSQLTypes();
    private static Map<ORDER, Integer> mongoOrders = getMongoOrders();
    private static Map<String, MongoClient> mongoConnections = new HashMap<String, MongoClient>();

    private static final String HOST = "Host";
    private static final String PORT = "Port";
    private static final String USER = "User";
    private static final String PASSWORD = "Password";
    private static final String DATABASE = "Database";
    private static final String COLLECTION = "Collection";
    private static final String FIELDS = "Fields field1[[:type1],field2[:type2],...]";


    public static Map<String, Integer> getSQLTypes() {

        Map<String, Integer> map = new HashMap<String, Integer>();

        // Get all field in java.sql.Types
        Field[] fields = java.sql.Types.class.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                String name = fields[i].getName();
                Integer value = (Integer) fields[i].get(null);
                map.put(name, value);
            } catch (IllegalAccessException e) {
                logger.debug("Illegal access getting SQL types:", e);
            }
        }
        return map;
    }

    /*
     * In MongoDB sort value is either 1 for ascending or -1 for descending.
     */
    private static Map<ORDER, Integer> getMongoOrders() {

        Map<ORDER, Integer> map = new HashMap<ORDER, Integer>();
        map.put(ORDER.ASC, Integer.valueOf(1));
        map.put(ORDER.DESC, Integer.valueOf(-1));

        return map;
    }

    @Override
    public CustomWrapperInputParameter[] getInputParameters() {
        return new CustomWrapperInputParameter[] {
            new CustomWrapperInputParameter(HOST, "Name of the computer or IP address where MongoDB is running ",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(PORT, "Port number to connect to MongoDB, default is 27017 ",
                false, CustomWrapperInputParameterTypeFactory.integerType()),
            new CustomWrapperInputParameter(USER, "Username to connect to MongoDB, if authentication enabled ",
                false, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(PASSWORD, "Password associated with the username ",
                false, CustomWrapperInputParameterTypeFactory.passwordType()),
            new CustomWrapperInputParameter(DATABASE, "Database name ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(COLLECTION, "Collection name.  A collection is the equivalent of an RDBMS table, but do not enforce a schema ",
                true, CustomWrapperInputParameterTypeFactory.stringType()),
            new CustomWrapperInputParameter(FIELDS, "Fields documents to retrieve from the collection. Type, when specified, should be one of java.sql.Types ",
                true, CustomWrapperInputParameterTypeFactory.stringType())
            };
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration configuration = super.getConfiguration();
        configuration.setDelegateProjections(true);
        configuration.setDelegateOrConditions(true);
        configuration.setDelegateOrderBy(true);
        configuration.setAllowedOperators(new String[] {
            OPERATOR_EQ, OPERATOR_NE, OPERATOR_LT, OPERATOR_LE,
            OPERATOR_GT, OPERATOR_GE, OPERATOR_LIKE
        });

        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(
        final Map<String, String> inputValues) throws CustomWrapperException {

        boolean searchable = true;
        boolean updateable = true;
        boolean nullable = true;
        boolean mandatory = true;

        String trimmedFields = inputValues.get(FIELDS).replaceAll("\\s", "");
        String[] fields = trimmedFields.split(",");

        CustomWrapperSchemaParameter[] parameters = new CustomWrapperSchemaParameter[fields.length];
        for (int index = 0; index < fields.length; index++) {
            String[] field = fields[index].split(":");
            int type = Types.VARCHAR;
            if (field.length == 2) {
                if (sqlTypes.containsKey(field[1])) {
                    type = sqlTypes.get(field[1]).intValue();
                } else {
                    String errorMsg = "Unsupported field type: '" + field[1]
                        + "'. Supported types are: " + sqlTypes.keySet();
                    logger.error(errorMsg);
                    throw new CustomWrapperException(errorMsg);
                }
            }

            parameters[index] = new CustomWrapperSchemaParameter(field[0], type,
                null, searchable, CustomWrapperSchemaParameter.NOT_SORTABLE,
                !updateable, nullable, !mandatory);

        }

        return parameters;

    }

    @Override
    public void run(final CustomWrapperConditionHolder condition,
        final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperResult result, final Map<String, String> inputValues)
        throws CustomWrapperException {

        try {

            String host = inputValues.get(HOST);
            String port = inputValues.get(PORT);
            String user = inputValues.get(USER);
            String password = inputValues.get(PASSWORD);
            String databaseName = inputValues.get(DATABASE);
            String collectionName = inputValues.get(COLLECTION);

            MongoClient mongoClient = getConnection(host, port);
            DB db = mongoClient.getDB(databaseName);

            boolean authenticated = authenticate(db, user, password);
            if (authenticated) {
                DBCollection collection = db.getCollection(collectionName);
                DBCursor cursor = query(collection, condition);
                while (cursor.hasNext()) {
                    DBObject document = cursor.next();

                    try {
                        List<Object> params = new ArrayList<Object>();
                        for (CustomWrapperFieldExpression field : projectedFields) {
                            params.add(getField(document, field.getName()));
                        }

                        result.addRow(params.toArray(), projectedFields);

                    } catch (final Exception e) {
                        logger.error("Error executing the stored procedure", e);
                        throw new CustomWrapperException(
                            "Exception while executing mongoDB wrapper", e);
                    }
                }

            } else {
                String errorMsg = "MongoDB authentication error: wrong user/password";
                logger.error(errorMsg);
                throw new CustomWrapperException(errorMsg);
            }
        } catch (Exception e) {
            String errorMsg = "MongoDB wrapper error:" + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }

    private static MongoClient getConnection(String host, String port)
        throws CustomWrapperException {

        try {

            String connectionID = getConnectionID(host, port);
            logger.debug("Getting MongoDB connection for " + connectionID);
            MongoClient mongoClient = mongoConnections.get(connectionID);
            if ( mongoClient == null) {
                logger.debug("MongoDB connection '" + connectionID + "' does not exist, creating it... ");
                ServerAddress connectionAddress = getConnectionAddress(host, port);
                mongoClient = new MongoClient(connectionAddress);
                testConnection(mongoClient);
                mongoConnections.put(connectionID, mongoClient);
            }

            return mongoClient;

        } catch (MongoException e) {
            String errorMsg = "MongoDB connection error: " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }

    private static String getConnectionHost(String host) {
        return (StringUtils.isNotEmpty(host)) ? host : ServerAddress.defaultHost();
    }

    private static int getConnectionPort(String port) {
        return (StringUtils.isNotEmpty(port)) ? Integer.parseInt(port) : ServerAddress.defaultPort();
    }

    private static String getConnectionID(String host, String port) {
        return getConnectionHost(host) + ":" + getConnectionPort(port);

    }

    private static ServerAddress getConnectionAddress(String host, String port)
        throws CustomWrapperException {

        try {

            return new ServerAddress(getConnectionHost(host), getConnectionPort(port));

        } catch (UnknownHostException e) {
            String errorMsg = "MongoDB connection error: unknown host '" + host + "'";
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }

    /*
     * MongoClient constructor does not actually connect to the server: a connection
     * is obtained from the pool only when a request (ie. an operation as find, insert, ...)
     * is sent to the database. So getDatabaseNames() is invoked to test for database connectivity.
     */
    private static void testConnection(MongoClient mongoClient) throws CustomWrapperException {

        try {
            mongoClient.getDatabaseNames();
        } catch (MongoException.Network e) {
            logger.error("MongoDB connection error: unable to establish connection ", e);
            throw new CustomWrapperException("MongoDB connection error: unable to establish connection");
        }
    }

    private static boolean authenticate(DB db, String user, String password) {

        boolean authenticated = true;
        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            char[] pssw = password.toCharArray();
            authenticated = db.authenticate(user, pssw);
        }
        return authenticated;
    }

    private DBCursor query(DBCollection collection, final CustomWrapperConditionHolder conditionHolder) {

        CustomWrapperCondition condition = conditionHolder.getComplexCondition();
        DBCursor cursor = null;
        if (condition != null) {
            DBObject query = QueryUtils.buildQuery(condition);
            logger.debug("VDP query is:'" + condition + "' resulting in MongoDB query:'" + query + "'");

            cursor = collection.find(query);
        } else {
            cursor = collection.find();
        }

        cursor = sort(cursor);

        return cursor;
    }


    private DBCursor sort(DBCursor cursor) {

        DBCursor sortedCursor = cursor;
        Collection<CustomWrapperOrderByExpression> sortFields = getOrderByExpressions();
        if (!sortFields.isEmpty()) {

            Map<String, Integer> sortCriteria = new LinkedHashMap<String, Integer>();
            for (CustomWrapperOrderByExpression sortField : sortFields) {
                String field = sortField.getField().getName();
                Integer order = mongoOrders.get(sortField.getOrder());
                sortCriteria.put(field, order);
            }
            BasicDBObject orderBy = new BasicDBObject(sortCriteria);
            sortedCursor.sort(orderBy);
        }

        return sortedCursor;

    }

    /*
     * Get a (potentially recursive) field
     */
    private static Object getField(DBObject document, String name) {

        final String[] tokens = name.split("\\.");

        Object field = null;
        if (tokens.length > 0) {
            field = document.get(tokens[0]);
            for (int i = 1; i < tokens.length; i++) {
                if (field != null) {
                    field = ((BSONObject) field).get(tokens[i]);
                }
            }
        }

        return field;
    }
}
