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
package com.denodo.connect.mongodb.wrapper.util;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;

import com.denodo.connect.mongodb.wrapper.MongoDBWrapper;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import org.apache.log4j.Logger;
import org.bson.BsonTimestamp;
import org.bson.Document;


public final class DocumentUtils {

    // We are getting the logger for the MongoDBWrapper on purpose, so that all logging is done from there
    private static final Logger logger = Logger.getLogger(MongoDBWrapper.class);


    private DocumentUtils() {
    }

    public static Object buildVDPColumn(Document document, String fullName, CustomWrapperSchemaParameter[] schema) {

        String[] tokens = fullName.split("\\.");

        Object field = document;
        CustomWrapperSchemaParameter[] currentSchema = schema;
        CustomWrapperSchemaParameter schemaParam = null;
        for (int i = 0; i < tokens.length && field != null; i++) {
            String name = tokens[i];
            field =  ((Document)field).get(name);
            schemaParam = getSchemaParameter(currentSchema, name);
            if (schemaParam != null) {
                currentSchema = schemaParam.getColumns();
            }
        }

        field = doBuildVDPColumn(field, schemaParam);

        return field;
    }

    public static Document buildMongoDocument(Map<CustomWrapperFieldExpression, Object> insertValues) throws RuntimeException {
        Document doc = new Document();
        for (final CustomWrapperFieldExpression field : insertValues.keySet()) {
            doc.append(field.getStringRepresentation(), insertValues.get(field));
        }

        return doc;
    }
    
    
    private static CustomWrapperSchemaParameter getSchemaParameter(CustomWrapperSchemaParameter[] schema, String field) {

        CustomWrapperSchemaParameter parameter = null;
        boolean found = false;
        if (schema != null) {
            for (int i = 0; i < schema.length && !found; i++) {
                CustomWrapperSchemaParameter p = schema[i];
                if (field.equals(p.getName())) {
                    parameter = p;
                    found = true;
                }
            }
        }

        return parameter;
    }

    private static Object doBuildVDPColumn(Object value, CustomWrapperSchemaParameter schemaParam) {

        if (logger.isTraceEnabled()) {
            logger.trace(
                    String.format("Building VDP column '%s' with type '%s' and value '%s'",
                            schemaParam.getName(), schemaParam.getType(), (value == null? "null" : value.toString())));
        }

        if (schemaParam != null) {
            if (schemaParam.getType() == Types.ARRAY ) {
                ArrayList<Object> mongoDBArray = (ArrayList<Object>) value;
                if(mongoDBArray!=null){
                    Object[][] vdpArray = new Object[mongoDBArray.size()][1];
                    int i = 0;
                    for (Object element : mongoDBArray) {
                        CustomWrapperSchemaParameter[] elementSchema = schemaParam.getColumns();
                        vdpArray[i++][0] = doBuildVDPColumn(element, elementSchema[0]);
                    }

                    return vdpArray;}
                else{
                    return null;
                }

            } else if (schemaParam.getType() == Types.STRUCT) {
                Document mongoDBRecord =  (Document) value;


                if(mongoDBRecord!=null){
                    Object[] vdpRecord = new Object[schemaParam.getColumns().length];
                    int i = 0;
                    for (CustomWrapperSchemaParameter param : schemaParam.getColumns()) {
                        Object fieldValue = mongoDBRecord.get(param.getName());
                        vdpRecord[i++] = doBuildVDPColumn(fieldValue, param);
                    }


                    return vdpRecord;
                }else{
                    return null;
                }

            } else if (schemaParam.getType() == Types.TIMESTAMP) {

                if (value != null) {

                    if (value instanceof BsonTimestamp) {
                        // BsonTimestamp, precision: second (UNIX time_t)
                        final BsonTimestamp mongoDBTimestamp =  (BsonTimestamp) value;
                        return new java.sql.Timestamp(mongoDBTimestamp.getTime() * 1000L);
                    } else {
                        // Most probably Date (BSON Date, with time), precision: millisecond
                        return value;
                    }

                }else{
                    return null;
                }
            }

        }

        return value;
    }

}
