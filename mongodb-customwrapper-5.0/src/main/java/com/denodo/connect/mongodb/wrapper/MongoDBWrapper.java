/*
 * =============================================================================
 *
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
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_ISNOTNULL;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_ISNULL;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LIKE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.denodo.connect.mongodb.wrapper.schema.SchemaBuilder;
import com.denodo.connect.mongodb.wrapper.util.DocumentUtils;
import com.denodo.connect.mongodb.wrapper.util.QueryUtils;
import com.denodo.vdb.engine.customwrapper.AbstractCustomWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperConfiguration;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.CustomWrapperInputParameter;
import com.denodo.vdb.engine.customwrapper.CustomWrapperResult;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.input.type.CustomWrapperInputParameterTypeFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoDBWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(MongoDBWrapper.class);

    private static final String HOST = "Host";
    private static final String PORT = "Port";
    private static final String USER = "User";
    private static final String PASSWORD = "Password";
    private static final String DATABASE = "Database";
    private static final String COLLECTION = "Collection";
    private static final String FIELDS = "Fields";
    private static final String INTROSPECTION_QUERY = "Introspection query";

    private static final Map<String, Integer> SQL_TYPES = getSQLTypes();

    // schema cache, so we do not have to recalculate the schema for each execution of a custom wrapper view
    private static Map<Map<String, String>, CustomWrapperSchemaParameter[]> schemaCache = new HashMap<Map<String,String>, CustomWrapperSchemaParameter[]>();

    private static Map<String, Integer> getSQLTypes() {

        Map<String, Integer> map = new HashMap<String, Integer>();

        // Get all field in java.sql.Types
        Field[] fields = java.sql.Types.class.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                String name = fields[i].getName().toLowerCase();
                Integer value = (Integer) fields[i].get(null);
                map.put(name, value);
            } catch (IllegalAccessException e) {
                logger.debug("Illegal access getting SQL types: ", e);
            }
        }
        return map;
    }
    
    public MongoDBWrapper() {
        super();
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
                new CustomWrapperInputParameter(COLLECTION, "Collection name ",
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(FIELDS, "field1[[:type1],field2[:type2],...] Fields document to retrieve from the collection. Type, when specified, should be one of java.sql.Types ",
                        false, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(INTROSPECTION_QUERY, "Documents retrieved by this query will be analyzed to reveal their fields and build the view schema. An empty query selects all documents in the collection ",
                        false, CustomWrapperInputParameterTypeFactory.stringType())
        };
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        CustomWrapperConfiguration configuration = super.getConfiguration();
        configuration.setDelegateProjections(true);
        configuration.setDelegateCompoundFieldProjections(true);
        configuration.setDelegateOrConditions(true);
        configuration.setDelegateOrderBy(true);
        configuration.setAllowedOperators(new String[] {
                OPERATOR_EQ, OPERATOR_NE, OPERATOR_LT, OPERATOR_LE,
                OPERATOR_GT, OPERATOR_GE, OPERATOR_LIKE, OPERATOR_ISNULL,
                OPERATOR_ISNOTNULL
        });

        return configuration;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSchemaParameters(
            final Map<String, String> inputValues) throws CustomWrapperException {

        try {
            // Check input here so we can inform the user about errors at base view creation time.
            checkInput(inputValues);

            CustomWrapperSchemaParameter[] schema = schemaCache.get(inputValues);
            if (schema == null) {
                String fields = inputValues.get(FIELDS);
                if (StringUtils.isNotBlank(fields)) {
                    schema = getSchemaFromFields(inputValues);
                } else {
                    schema = getSchemaFromQuery(inputValues);
                }

                schemaCache.put(inputValues, schema);
            }

            return schema;

        } catch (Exception e) {
            String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }

    }

    private static void checkInput(Map<String, String> inputValues) {

        String user = inputValues.get(USER);
        String password = inputValues.get(PASSWORD);

        if (StringUtils.isNotBlank(user) && StringUtils.isBlank(password)) {
            throw new IllegalArgumentException(PASSWORD + " is missing.");
        }
    }

    /*
     * Only simple types are allowed. An introspection query should be used for
     * configuring documents with complex field types like Types.Array and
     * Types.Struct.
     */
    private static CustomWrapperSchemaParameter[] getSchemaFromFields(
            final Map<String, String> inputValues) {

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
                type = getSQLType(field[1]);
            }

            parameters[index] = new CustomWrapperSchemaParameter(field[0], type,
                    null, searchable, CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
                    updateable, nullable, !mandatory);

        }

        return parameters;

    }

    private static int getSQLType(String userType) {

        int type = -1;
        String lowerCaseType = userType.toLowerCase();
        Integer typeAsInteger = SQL_TYPES.get(lowerCaseType);
        if (typeAsInteger != null) {
            type = typeAsInteger.intValue();
            if (type == Types.ARRAY || type == Types.STRUCT) {
                throw new IllegalArgumentException("You should use an " + INTROSPECTION_QUERY
                        + " for configuring fields of type " + userType);
            }
        } else {
            Set<String> supportedTypes = new HashSet<String>(SQL_TYPES.keySet());
            supportedTypes.remove("array");
            supportedTypes.remove("struct");
            throw new IllegalArgumentException("Unsupported field type: '" + userType
                    + "'. Supported types are: " + supportedTypes);
        }
        return type;
    }

    /*
     * Builds the schema as the union of the fields of all the documents retrieved by
     * the introspection query.
     *
     * Due to the fact:
     * - Documents in the same collection do not need to have the same set of fields.
     * Then, all the documents returned by the query have to be considered when building the schema.
     *
     * Due to the fact:
     * - Common fields in a collection's documents may hold different types of data.
     * Then, the structure of a common field will be the highest common denominator between
     * all the fields with the same name.
     *
     */
    private static CustomWrapperSchemaParameter[] getSchemaFromQuery(
            final Map<String, String> inputValues) throws IOException {

        String jsonQuery = inputValues.get(INTROSPECTION_QUERY);

        MongoDBClient client = connect(inputValues);
        DBCursor cursor = client.query(jsonQuery);
        SchemaBuilder builder = new SchemaBuilder();
        while (cursor.hasNext()) {
            DBObject document = cursor.next();
            builder.addToSchema(document);
        }

        CustomWrapperSchemaParameter[] schema = builder.buildSchema();
        if (schema.length == 0) {
            throw new IllegalArgumentException(INTROSPECTION_QUERY + " does not retrieve any document");
        }


        return schema;

    }

    @Override
    public void run(final CustomWrapperConditionHolder condition,
            final List<CustomWrapperFieldExpression> projectedFields,
            final CustomWrapperResult result, final Map<String, String> inputValues)
                    throws CustomWrapperException {

        try {

            MongoDBClient client = connect(inputValues);
            DBCursor cursor = query(client, condition);

            CustomWrapperSchemaParameter[] schema = getSchemaParameters(inputValues);
            List<Object> row = new ArrayList<Object>();
            while (cursor.hasNext()) {
                DBObject document = cursor.next();
                for (CustomWrapperFieldExpression field : projectedFields) {
                    Object column = DocumentUtils.buildVDPColumn(document, field.getName(), schema);
                    row.add(column);
                }

                result.addRow(row.toArray(), projectedFields);
                row.clear();
            }

        } catch (Exception e) {
            String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }

    @Override
    public int insert(final Map<CustomWrapperFieldExpression, Object> insertValues,
            final Map<String, String> inputValues)
                    throws CustomWrapperException {
        try {

            MongoDBClient client = connect(inputValues);
            DBCollection coll = client.getCollection();

            CustomWrapperSchemaParameter[] schema = getSchemaParameters(inputValues);

            BasicDBObject doc = DocumentUtils.buildMongoDBObject(insertValues, schema);
            coll.insert(doc);
            return 1;
        } catch (Exception e) {
            String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }
    
    @Override
    public int update(Map<CustomWrapperFieldExpression, Object> newValues,
            CustomWrapperConditionHolder condition, Map<String, String> inputValues)
            throws CustomWrapperException {
        try {
            CustomWrapperSchemaParameter[] schema = getSchemaParameters(inputValues);
            
            MongoDBClient client = connect(inputValues);
            DBCollection coll = client.getCollection();
            
            //Search query
            DBObject searchQuery = QueryUtils.buildQuery(condition.getComplexCondition());
            
            // New values
            BasicDBObject updateQuery = new BasicDBObject();
            updateQuery.append("$set", DocumentUtils.buildMongoDBObject(newValues, schema));
            
            // Execute update
            coll.updateMulti(searchQuery, updateQuery);
            
            /*
             * MongoDB does not tell you how many records have been updated
             * To get this number, we would have to run the search query first.
             * That would be very slow. therfore, as a tradeoff, 1 is returned always
             */
            return 1;
        } catch (Exception e) {
            String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }


    private static MongoDBClient connect(final Map<String, String> inputValues) throws IOException {

        String host = inputValues.get(HOST);
        String portAsString = inputValues.get(PORT);
        Integer port = (portAsString == null) ? null : Integer.valueOf(portAsString);
        String user = inputValues.get(USER);
        String password = inputValues.get(PASSWORD);
        String dbName = inputValues.get(DATABASE);
        String collectionName = inputValues.get(COLLECTION);

        return new MongoDBClient(host, port, user, password, dbName, collectionName);
    }

    private DBCursor query(MongoDBClient client, final CustomWrapperConditionHolder condition) {

        DBObject query = QueryUtils.buildQuery(condition.getComplexCondition());
        logger.debug("VDP query is: '" + condition.getComplexCondition() + "' resulting in MongoDB query: '" + query + "'");
        getCustomWrapperPlan().addPlanEntry("MongoDB query", query.toString());

        DBObject orderBy = QueryUtils.buildOrderBy(getOrderByExpressions());

        return client.query(query, orderBy);
    }


}
