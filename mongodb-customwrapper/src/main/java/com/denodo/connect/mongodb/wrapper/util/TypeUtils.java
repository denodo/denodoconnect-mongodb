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
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.ObjectId;


public final class TypeUtils {

    private static final Logger logger = Logger.getLogger(TypeUtils.class);

    private static final Map<Class<?>, Integer> SQL_TYPES = getTypeMappings();

    private TypeUtils() {

    }

    private static Map<Class<?>, Integer> getTypeMappings() {

        Map<Class<?>, Integer> map = new HashMap<Class<?>, Integer>();
        map.put(String.class, Integer.valueOf(Types.VARCHAR));
        map.put(Integer.class, Integer.valueOf(Types.INTEGER));
        map.put(Long.class, Integer.valueOf(Types.BIGINT));
        map.put(Boolean.class, Integer.valueOf(Types.BOOLEAN));
        map.put(Double.class, Integer.valueOf(Types.DOUBLE));
        map.put(Float.class, Integer.valueOf(Types.FLOAT));
        map.put(ArrayList.class, Integer.valueOf(Types.ARRAY));
        map.put(Document.class, Integer.valueOf(Types.STRUCT));
        map.put(Binary.class, Integer.valueOf(Types.VARBINARY));
        map.put(ObjectId.class, Integer.valueOf(Types.VARCHAR));
        map.put(Code.class, Integer.valueOf(Types.VARCHAR));
        map.put(Object.class, Integer.valueOf(Types.JAVA_OBJECT));

       /*
         * Note: Even if as of Denodo 6.0u08 we could use SQL DATE and SQL TIMESTAMP to
         * differentiate MongoDB's BSON DATE and BSON TIMESTAMP thanks to the fact that VDP
         * an SQL DATE would be DATE+TIME, this will change in Denodo 7.0 and SQL DATE will
         * be a proper Date (no time) in VQL, so such thing would be a bad idea for the future.
         *
         * This means that we will identify both MongoDB data types as the same in VDP, which will
         * have as a consequence a limitation on WHERE clauses: no filters will be possible on
         * MongoDB BSON TIMESTAMP fields. Also, no insertion of BSON TIMESTAMPS will be possible.
         *
         * This should be no real issue as MongoDB BSON TIMESTAMP data is considered for internal MongoDN
         * use by the BSON specification: https://docs.mongodb.com/manual/reference/bson-types/#timestamps
         */

        // BSON Timestamps are considered SQL Timestamp (second precision - UNIX time_t)
        map.put(BsonTimestamp.class, Integer.valueOf(Types.TIMESTAMP));
        // BSON Dates are considered SQL Timestamp because they include the time (millisecond precision)
        map.put(java.util.Date.class, Integer.valueOf(Types.TIMESTAMP));

        return map;
    }

    public static int toSQL(Class<?> javaClass) {

        Integer sqlType = SQL_TYPES.get(javaClass);
        if (sqlType == null) {
            logger.warn("Class '" + javaClass + "' is not supported. Returning Types.VARCHAR");
            return Types.VARCHAR;
        }

        return sqlType.intValue();
    }

}
