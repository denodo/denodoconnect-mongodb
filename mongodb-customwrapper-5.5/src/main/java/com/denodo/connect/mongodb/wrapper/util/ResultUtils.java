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

import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import org.apache.log4j.Logger;
import org.bson.BsonTimestamp;
import org.bson.Document;


public final class ResultUtils {

    private static final Logger logger = Logger.getLogger(ResultUtils.class);


    private ResultUtils() {
    }

    public static Object buildResultColumnValue(Document document, String fullName, CustomWrapperSchemaParameter[] schema) {

        String[] tokens = fullName.split("\\.");

        Object field = document;
        CustomWrapperSchemaParameter[] currentSchema = schema;
        CustomWrapperSchemaParameter schemaParam = null;
        for (int i = 0; i < tokens.length && field != null; i++) {
            String name = tokens[i];
            field =  ((Document)field).get(name);
            schemaParam = findParameterInSchema(currentSchema, name);
            if (schemaParam != null) {
                currentSchema = schemaParam.getColumns();
            }
        }

        field = doBuildResultColumnValue(field, schemaParam);

        return field;
    }

    
    private static CustomWrapperSchemaParameter findParameterInSchema(CustomWrapperSchemaParameter[] schema, String field) {
        if (schema != null) {
            for (int i = 0; i < schema.length; i++) {
                CustomWrapperSchemaParameter p = schema[i];
                if (field.equals(p.getName())) {
                    return p;
                }
            }
        }
        return null;
    }

    private static Object doBuildResultColumnValue(Object value, CustomWrapperSchemaParameter schemaParam) {

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
                        vdpArray[i++][0] = doBuildResultColumnValue(element, elementSchema[0]);
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
                        vdpRecord[i++] = doBuildResultColumnValue(fieldValue, param);
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
