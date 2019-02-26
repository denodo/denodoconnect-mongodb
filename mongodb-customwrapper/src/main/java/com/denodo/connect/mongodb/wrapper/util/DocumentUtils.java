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

import java.util.Map;

import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import org.apache.log4j.Logger;
import org.bson.Document;


public final class DocumentUtils {

    // We are getting the logger for the MongoDBWrapper on purpose, so that all logging is done from there
    private static final Logger logger = Logger.getLogger(DocumentUtils.class);


    private DocumentUtils() {
    }


    public static Document buildMongoDocument(
            final CustomWrapperSchemaParameter[] schema, Map<CustomWrapperFieldExpression, Object> values)
            throws RuntimeException {

        final Document doc = new Document();

        for (final CustomWrapperFieldExpression field : values.keySet()) {

            final String fieldName = field.getStringRepresentation();
            final Object fieldValue = values.get(field);

            if (logger.isTraceEnabled()) {
                logger.trace(
                        String.format("Building MongoDB document field '%s' with type '%s' and value '%s'",
                                fieldName,
                                (fieldValue == null? "null" : fieldValue.getClass().getName()),
                                (fieldValue == null? "null" : fieldValue.toString())));
            }

            // As of Denodo 6.0u08, fieldValue will never be a java.sql.Timestamp (all fields of type Date,
            // whatever the subtype, are provided as java.util.Date). Also, we have no way to establish the difference
            // between a Date that was a BSON TIMESTAMP at MongoDB and a Date that was a BSON DATE at MongoDB, so
            // we will only use DATE and assume as limitation the fact that no queries can be performed with filters
            // on fields of type BSON TIMESTAMP.

            doc.append(fieldName, fieldValue);

        }

        return doc;
    }

}
