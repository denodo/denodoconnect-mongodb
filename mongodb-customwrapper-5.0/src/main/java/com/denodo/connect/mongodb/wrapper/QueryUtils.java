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

import java.util.HashMap;
import java.util.Map;

import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;


public final class QueryUtils {

    private static Map<String, String> mongoOperators = getMongoOperators();

    private static final String START_OF_LINE = "^";
    private static final String END_OF_LINE = "$";
    private static final String SQL_SINGLE_CHARACTER = "_";
    private static final String MONGODB_SINGLE_CHARACTER = ".";
    private static final String SQL_ZERO_MORE_CHARACTER = "%";
    private static final String MONGODB_ZERO_MORE_CHARACTER = "*";
    private static final String ESCAPE_CHARACTER = "\\";

    /*
     * Equivalences between VDP and MongoDB operators.
     */
    private static Map<String, String> getMongoOperators() {

        Map<String, String> map = new HashMap<String, String>();
        map.put(OPERATOR_EQ, null);
        map.put(OPERATOR_NE, "$ne");
        map.put(OPERATOR_LT, "$lt");
        map.put(OPERATOR_LE, "$lte");
        map.put(OPERATOR_GT, "$gt");
        map.put(OPERATOR_GE, "$gte");
        map.put(OPERATOR_LIKE, "$regex");

        return map;
    }

    private QueryUtils() {

    }

    public static DBObject buildQuery(CustomWrapperCondition vdpCondition) {

        QueryBuilder query = new QueryBuilder();

        if (vdpCondition.isAndCondition()) {

            CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) vdpCondition;
            for (CustomWrapperCondition condition : andCondition.getConditions()) {
                DBObject andQuery = buildQuery(condition);
                query.and(andQuery);
            }

        } else if (vdpCondition.isOrCondition()) {
            CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) vdpCondition;
            for (CustomWrapperCondition condition : orCondition.getConditions()) {
                DBObject orQuery = buildQuery(condition);
                query.or(orQuery);
            }

        } else {
            CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) vdpCondition;

            String field = ((CustomWrapperFieldExpression) simpleCondition.getField()).getName();
            String operator = simpleCondition.getOperator();
            Object value = ((CustomWrapperSimpleExpression) simpleCondition.getRightExpression()[0]).getValue();

            addOperand((BasicDBObject) query.get(), field, operator, value);

        }

        return query.get();
    }

    private static BasicDBObject addOperand(BasicDBObject query, String field, String op, Object value) {

        BasicDBObject result = query;
        String mongoDBop = mongoOperators.get(op);
        // operand equals is a special case, it does not have an equivalent operand in MongoDB:
        if (mongoDBop == null) {
            //  e.g. find all where i = 71 -> BasicDBObject("i", 71)
            result.append(field, value);
        } else {
            // e.g. find all where i > 50 -> new BasicDBObject("i", new BasicDBObject("$gt", 50));
            result.append(field, new BasicDBObject(mongoDBop,
                op.equals(OPERATOR_LIKE) ? translateRegex(value) : value));
        }
        return result;
    }

    /*
     * MongoDB $regex uses "Perl Compatible Regular Expressions" (PCRE)
     * as the matching engine:
     *
     * VDP LIKE  ---  MongoDB $regex
     *
     *   '%'     ---  '*': 0 or more quantifier
     *   '_'     ---  '.': match any character of length 1
     *
     * The "start of line" metacharacter (^) matches only at the start of the string,
     * and the "end of line" metacharacter ($) matches only at the end of the string,
     * or before a terminating newline.
     */
    private static Object translateRegex(Object value) {

        String regex = (String) value;

        // escape metacharacters
        regex = regex.replace(START_OF_LINE, ESCAPE_CHARACTER + START_OF_LINE);
        regex = regex.replace(END_OF_LINE, ESCAPE_CHARACTER + END_OF_LINE);
        regex = regex.replace(MONGODB_SINGLE_CHARACTER, ESCAPE_CHARACTER + MONGODB_SINGLE_CHARACTER);
        regex = regex.replace(MONGODB_ZERO_MORE_CHARACTER, ESCAPE_CHARACTER + MONGODB_ZERO_MORE_CHARACTER);

        // convert SQL to MongoDB
        regex = regex.replace(SQL_SINGLE_CHARACTER, MONGODB_SINGLE_CHARACTER);
        regex = regex.replace(SQL_ZERO_MORE_CHARACTER, MONGODB_SINGLE_CHARACTER + MONGODB_ZERO_MORE_CHARACTER);

        return START_OF_LINE + regex + END_OF_LINE;
    }

}
