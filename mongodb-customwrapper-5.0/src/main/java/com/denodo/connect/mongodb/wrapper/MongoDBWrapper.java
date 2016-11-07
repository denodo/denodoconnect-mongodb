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
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

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
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.DeleteResult;

public class MongoDBWrapper extends AbstractCustomWrapper {

    private static final Logger logger = Logger.getLogger(MongoDBWrapper.class);

    private static final String HOST = "Host";
    private static final String PORT = "Port";
    private static final String USER = "User";
    private static final String PASSWORD = "Password";
    private static final String DATABASE = "Database";
    private static final String COLLECTION = "Collection";
    private static final String FIELDS = "Fields";
    private static final String CONNECTION_STRING = "Connection String";
    private static final String INTROSPECTION_QUERY = "Introspection query";

    private static final Map<String, Integer> SQL_TYPES = getSQLTypes();

    // schema cache, so we do not have to recalculate the schema for each execution of a custom wrapper view
    private static Map<Map<String, String>, CustomWrapperSchemaParameter[]> schemaCache = new HashMap<Map<String, String>, CustomWrapperSchemaParameter[]>();

    private static Map<String, Integer> getSQLTypes() {

        final Map<String, Integer> map = new HashMap<String, Integer>();

        // Get all field in java.sql.Types
        final Field[] fields = java.sql.Types.class.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                final String name = fields[i].getName().toLowerCase();
                final Integer value = (Integer) fields[i].get(null);
                map.put(name, value);
            } catch (final IllegalAccessException e) {
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
                        false, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(COLLECTION, "Collection name ",
                        true, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(
                                CONNECTION_STRING,
                                "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]]/database[?options] \n"                                
                                + "This parameter is an alternative to put database, host and port parameters  ",
                                false, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(
                        FIELDS,
                        "field1[[:type1],field2[:type2],...] Fields document to retrieve from the collection. Type, when specified, should be one of java.sql.Types ",
                        false, CustomWrapperInputParameterTypeFactory.stringType()),
                new CustomWrapperInputParameter(
                        INTROSPECTION_QUERY,
                        "Documents retrieved by this query will be analyzed to reveal their fields and build the view schema. An empty query selects all documents in the collection ",
                        false, CustomWrapperInputParameterTypeFactory.stringType())
        };
    }

    @Override
    public CustomWrapperConfiguration getConfiguration() {

        final CustomWrapperConfiguration configuration = super.getConfiguration();
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

            CustomWrapperSchemaParameter[] schema;
            final MongoDBClient client = connect(inputValues,true);
            
          
                final String fields = inputValues.get(FIELDS);
                if (StringUtils.isNotBlank(fields)) {
                    schema = getSchemaFromFields(inputValues);
                } else {
                    schema = getSchemaFromQuery(inputValues,client);
                }

                schemaCache.put(inputValues, schema);
            return schema;

        } catch (final Exception e) {
            final String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
       

    }

    private static void checkInput(final Map<String, String> inputValues) {

        final String user = inputValues.get(USER);
        final String password = inputValues.get(PASSWORD);
        final String host = inputValues.get(HOST);
        final String portAsString = inputValues.get(PORT);
        final String dbName = inputValues.get(DATABASE);
        final String connectionString = inputValues.get(CONNECTION_STRING);

        if (StringUtils.isNotBlank(user) && StringUtils.isBlank(password)) {
            throw new IllegalArgumentException(PASSWORD + " is missing.");
        }
        if(StringUtils.isNotBlank(connectionString)){
            if(StringUtils.isNotBlank(dbName)||StringUtils.isNotBlank(host)||StringUtils.isNotBlank(portAsString)){
                final String errorMsg = "You cannot specify at the same time connection string parameter or database, host and port parameters";
                logger.info(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
         }else if(!StringUtils.isNotBlank(dbName)){
             final String errorMsg = "Connection string parameter ou Database paramater is mandatory ";
             logger.info(errorMsg);
             throw new IllegalArgumentException(errorMsg);
         }
       
    }

    /*
     * Only simple types are allowed. An introspection query should be used for configuring documents with complex field
     * types like Types.Array and Types.Struct.
     */
    private static CustomWrapperSchemaParameter[] getSchemaFromFields(
            final Map<String, String> inputValues) {

        final boolean searchable = true;
        final boolean updateable = true;
        final boolean nullable = true;
        final boolean mandatory = true;

        final String trimmedFields = inputValues.get(FIELDS).replaceAll("\\s", "");
        final String[] fields = trimmedFields.split(",");

        final CustomWrapperSchemaParameter[] parameters = new CustomWrapperSchemaParameter[fields.length];
        for (int index = 0; index < fields.length; index++) {
            final String[] field = fields[index].split(":");
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

    private static int getSQLType(final String userType) {

        int type = -1;
        final String lowerCaseType = userType.toLowerCase();
        final Integer typeAsInteger = SQL_TYPES.get(lowerCaseType);
        if (typeAsInteger != null) {
            type = typeAsInteger.intValue();
            if ((type == Types.ARRAY) || (type == Types.STRUCT)) {
                throw new IllegalArgumentException("You should use an " + INTROSPECTION_QUERY
                        + " for configuring fields of type " + userType);
            }
        } else {
            final Set<String> supportedTypes = new HashSet<String>(SQL_TYPES.keySet());
            supportedTypes.remove("array");
            supportedTypes.remove("struct");
            throw new IllegalArgumentException("Unsupported field type: '" + userType
                    + "'. Supported types are: " + supportedTypes);
        }
        return type;
    }

    /*
     * Builds the schema as the union of the fields of all the documents retrieved by the introspection query.
     * 
     * Due to the fact:
     * 
     * - Documents in the same collection do not need to have the same set of fields. Then, all the documents returned
     * by the query have to be considered when building the schema.
     * 
     * Due to the fact:
     * 
     * - Common fields in a collection's documents may hold different types of data. Then, the structure of a common
     * field will be the highest common denominator between all the fields with the same name.
     */
    private static CustomWrapperSchemaParameter[] getSchemaFromQuery(
            final Map<String, String> inputValues,MongoDBClient client) throws Exception {

        final String jsonQuery = inputValues.get(INTROSPECTION_QUERY);

        
        final FindIterable<Document> cursor = client.query(jsonQuery);
        final SchemaBuilder builder = new SchemaBuilder();
        MongoCursor<Document> iterator=cursor.iterator();
        while (iterator.hasNext()) {
            final Document document = iterator.next();
            builder.addToSchema(document);
        }

        final CustomWrapperSchemaParameter[] schema = builder.buildSchema();
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

            final MongoDBClient client = connect(inputValues,false);
            final FindIterable<Document> cursor = query(client, condition);
            CustomWrapperSchemaParameter[] schema = schemaCache.get(inputValues);
            if (schema == null) {
                schema = getSchemaParameters(inputValues);
            }
            final List<Object> row = new ArrayList<Object>();
            MongoCursor<Document> iterator=cursor.iterator();
            while (iterator.hasNext()) {
                final Document document = iterator.next();
                for (final CustomWrapperFieldExpression field : projectedFields) {
                    final Object column = DocumentUtils.buildVDPColumn(document, field.getName(), schema);
                    row.add(column);
                }

                result.addRow(row.toArray(), projectedFields);
                row.clear();
            }
            iterator.close();

        } catch (final Exception e) {
            final String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }

    @Override
    public int insert(final Map<CustomWrapperFieldExpression, Object> insertValues,
            final Map<String, String> inputValues)
            throws CustomWrapperException {
        
        try {

            final MongoDBClient client = connect(inputValues,false);
            final MongoCollection<Document> coll = client.getCollection();

            final CustomWrapperSchemaParameter[] schema = getSchemaParameters(inputValues);

            final Document doc = DocumentUtils.buildMongoDocument(insertValues, schema);
            coll.insertOne(doc);
            return 1;
        } catch (final Exception e) {
            final String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }

    @Override
    public int update(final Map<CustomWrapperFieldExpression, Object> newValues,
            final CustomWrapperConditionHolder condition, final Map<String, String> inputValues)
            throws CustomWrapperException {
        
        try {
            final CustomWrapperSchemaParameter[] schema = getSchemaParameters(inputValues);

            final MongoDBClient client = connect(inputValues,false);
            final MongoCollection<Document> coll = client.getCollection();

            // Search query
            final Bson searchQuery = QueryUtils.buildQuery(condition.getComplexCondition());

            // New values
            final Document updateQuery = new Document();
            updateQuery.append("$set", DocumentUtils.buildMongoDocument(newValues, schema));

            // Execute update
            coll.updateMany(searchQuery, updateQuery);

//            /*
//             * MongoDB does not tell you how many records have been updated To get this number, we would have to run the
//             * search query first. That would be very slow. therefore, as a tradeoff, 1 is returned always
//             */
            return 1;
        } catch (final Exception e) {
            final String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
    }

    @Override
    public int delete(final CustomWrapperConditionHolder condition, final Map<String, String> inputValues)
            throws CustomWrapperException {
        try {
            final CustomWrapperSchemaParameter[] schema = getSchemaParameters(inputValues);

            final MongoDBClient client = connect(inputValues, false);
            final MongoCollection<Document> coll = client.getCollection();

            final Map<CustomWrapperFieldExpression, Object> conditionValues = condition.getConditionMap();

            final Document doc = DocumentUtils.buildMongoDocument(conditionValues, schema);

            final DeleteResult wr = coll.deleteMany(doc);

            // Return the number of documents affected
            return (int) wr.getDeletedCount();
        } catch (final Exception e) {
            final String errorMsg = "MongoDB wrapper error. " + e.getMessage();
            logger.error(errorMsg, e);
            throw new CustomWrapperException(errorMsg, e);
        }
        
       
    }

    private static MongoDBClient connect(final Map<String, String> inputValues, Boolean test) throws Exception {

        final String host = inputValues.get(HOST);
        final String portAsString = inputValues.get(PORT);
        final Integer port = (portAsString == null) ? null : Integer.valueOf(portAsString);
        final String user = inputValues.get(USER);
        final String password = inputValues.get(PASSWORD);
        final String dbName = inputValues.get(DATABASE);
        final String collectionName = inputValues.get(COLLECTION);
        final String connectionString = inputValues.get(CONNECTION_STRING);
        
        return new MongoDBClient(host, port, user, password, dbName, collectionName, connectionString, test);
    }

    private FindIterable<Document> query(final MongoDBClient client, final CustomWrapperConditionHolder condition) {

        final Bson query = QueryUtils.buildQuery(condition.getComplexCondition());
        BsonDocument doc = query.toBsonDocument(null, client.getMongoClient().getMongoClientOptions().getCodecRegistry());
        logger.debug("VDP query is: '" + condition.getComplexCondition() + "' resulting in MongoDB query: '" + doc
                + "'");
        getCustomWrapperPlan().addPlanEntry("MongoDB query", doc.toString());

        final Bson orderBy = QueryUtils.buildOrderBy(getOrderByExpressions());

        return client.query(query, orderBy);
    }

}
