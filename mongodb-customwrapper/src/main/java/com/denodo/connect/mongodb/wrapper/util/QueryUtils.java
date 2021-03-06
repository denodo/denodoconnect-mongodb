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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.denodo.connect.mongodb.wrapper.schema.SchemaBuilder;
import com.denodo.vdb.engine.customwrapper.CustomWrapperOrderByExpression;
import com.denodo.vdb.engine.customwrapper.CustomWrapperOrderByExpression.ORDER;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_EQ;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_IN;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_ISNOTNULL;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_ISNULL;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LIKE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;


public final class QueryUtils {

    private static final String MONGODB_ID_FIELD = "_id";



    private static final Map<String, String> MONGODB_OPERATORS = getOperatorMappings();
    private static final Map<ORDER, Integer> MONGODB_ORDERS = getOrderMappings();

    private static final String START_OF_LINE = "^";
    private static final String END_OF_LINE = "$";
    private static final String SQL_SINGLE_CHAR = "_";
    private static final String MONGODB_SINGLE_CHAR = ".";
    private static final String SQL_ZERO_MORE_CHAR = "%";
    private static final String MONGODB_ZERO_MORE_CHAR = "*";
    private static final String ESCAPE_CHAR = "\\";


    private QueryUtils() {
    }

    /*
     * Mappings between VDP and MongoDB operators.
     */
    private static Map<String, String> getOperatorMappings() {

        Map<String, String> map = new HashMap<String, String>();
        map.put(OPERATOR_EQ, null);
        map.put(OPERATOR_NE, "$ne");
        map.put(OPERATOR_LT, "$lt");
        map.put(OPERATOR_LE, "$lte");
        map.put(OPERATOR_GT, "$gt");
        map.put(OPERATOR_GE, "$gte");
        map.put(OPERATOR_LIKE, "$regex");
        map.put(OPERATOR_ISNULL, "$exists");
        map.put(OPERATOR_ISNOTNULL, "$exists");
        map.put(OPERATOR_IN, "$in");

        return map;
    }

    /*
     * Mappings between VDP and MongoDB sort values.
     */
    private static Map<ORDER, Integer> getOrderMappings() {

        Map<ORDER, Integer> map = new HashMap<ORDER, Integer>();
        map.put(ORDER.ASC, Integer.valueOf(1));
        map.put(ORDER.DESC, Integer.valueOf(-1));

        return map;
    }


    public static Bson buildQuery(CustomWrapperSchemaParameter[] schema, CustomWrapperCondition vdpCondition) {

        // Note that the schema CAN BE NULL, as we will only have it in run() executions (queries), but not in
        // updates, inserts or deletes due to a restriction in the Custom Wrapper API.

        Bson query =null;

        if (vdpCondition != null) {

            if (vdpCondition.isAndCondition()) {

                CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) vdpCondition;
                for (CustomWrapperCondition condition : andCondition.getConditions()) {
                    Bson andQuery =  buildQuery(schema, condition);
                   
                    if(query==null){
                        query=Filters.and(andQuery);
                    }else{
                        query=Filters.and(query,andQuery);  
                    }
                }

            } else if (vdpCondition.isOrCondition()) {
                CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) vdpCondition;
                for (CustomWrapperCondition condition : orCondition.getConditions()) {
                    Bson orQuery =  buildQuery(schema, condition);
                    if(query==null){
                        query=Filters.or(orQuery);
                    }else{
                        query=Filters.or(query,orQuery);  
                    }
                }

            } else {
                CustomWrapperSimpleCondition simpleCondition = (CustomWrapperSimpleCondition) vdpCondition;
                CustomWrapperFieldExpression fieldExpression = (CustomWrapperFieldExpression) simpleCondition.getField();

                String field = buildLeftOperand(fieldExpression);
                String operator = simpleCondition.getOperator();
                if (OPERATOR_ISNULL.equals(operator) || OPERATOR_ISNOTNULL.equals(operator)) {
                    query = buildNullCondition((Document)query, field, operator);
                } else {

                    // Note we are not currently using the schema for anything here, as we are assuming as a limitation
                    // the fact that we cannot make a difference betwen BSON Date and BSON Timestamp types.
                    // Receiving the schema here however might be useful in the future in Denodo 7 when we have a richer
                    // type system and hopefully a set of "original source type" metainformation associated with
                    // the CustomWrapperSchemaParameter objects.

                    query=new Document();

                    final CustomWrapperExpression[] rightSide = simpleCondition.getRightExpression();
                    final Object rightSideValue;
                    if (OPERATOR_IN.equals(operator)) {
                        // If operator is an IN, we need to take care of all the specified values, and set them into a List
                        final List<Object> rightSideValues = new ArrayList<Object>();
                        for (int i = 0; i < rightSide.length; i++) {
                            rightSideValues.add(((CustomWrapperSimpleExpression)rightSide[i]).getValue());
                        }
                        rightSideValue = rightSideValues;
                    } else {
                        // Otherwise, just get the first position of the array (there will be only one)
                        rightSideValue = ((CustomWrapperSimpleExpression)rightSide[0]).getValue();
                    }

                    addCondition( (Document)query, field, operator, rightSideValue);

                }



            }
        }
        if(query==null){
            query = new Document();
        }
        return query;
    }

   
    public static Bson buildOrderBy(Collection<CustomWrapperOrderByExpression> sortFields) {

        if (sortFields == null || sortFields.isEmpty()) {
            return null;
        }

        Map<String, Object> sortCriteria = new LinkedHashMap<String, Object>();
        for (CustomWrapperOrderByExpression sortField : sortFields) {
            String field = sortField.getField().getName();
            Integer order = MONGODB_ORDERS.get(sortField.getOrder());
            sortCriteria.put(field, order);
        }

        return new Document(sortCriteria);

    }

    /* Removes the names of the elements arrays, they are not required in MongoDB queries.
     * For example: if 'memos' field contains an array that contains subdocuments with the field 'by':
     * VDP will query for 'memos.memos_ITEM.by' and MongoDB will query for 'memos.by'.
     */
    private static String buildLeftOperand(CustomWrapperFieldExpression field) {

        List<CustomWrapperFieldExpression> fields = new ArrayList<CustomWrapperFieldExpression>();
        fields.add(field);
        fields.addAll(field.getSubFields());

        StringBuilder sb = new StringBuilder();
        for (CustomWrapperFieldExpression f : fields) {
            String name = f.getName();
            if (!name.endsWith(SchemaBuilder.ARRAY_ITEM_SUFFIX)) {
                sb.append(name);
                sb.append('.');
            }
        }

        // deletes last "."
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

    private static void addCondition(Document query, String field, String op, Object value) {

        Document result = query;
        String mongoDBop = MONGODB_OPERATORS.get(op);

        Object finalValue = transform(field, op, value);
        // operator equals is a special case, it does not have an equivalent operator in MongoDB:
        if (mongoDBop == null) {
            //  e.g. find all where i = 71 -> Document("i", 71)
            result.append(field, finalValue);
        } else {
            // e.g. find all where i > 50 -> new Document("i", new Document("$gt", 50));
            result.append(field, new Document(mongoDBop, finalValue));
        }
    }

    /*
     *  Alternative implementation for IS NULL and IS NOT NULL
     *  Since in MongoDB these can mean two things:
     *    - The field does not exist
     *    - The field is set to null explicitely
     *  the implementetion is an OR of both
     */
    private static Bson buildNullCondition(Document query, String field, String op) {
        if (OPERATOR_ISNULL.equals(op)) {
            return Filters.or(Filters.exists(field, false), Filters.eq(field, null));
        } else { // IS NOT NULL
            return Filters.and(Filters.exists(field), Filters.ne(field, null));
        }
    }

    private static Object transform(String field, String op, Object value) {

        Object result = null;
        if (MONGODB_ID_FIELD.equals(field)) {
            result = handleMongoDBId(value);
        } else if (OPERATOR_LIKE.equals(op)){
            result = translateRegex(value);
        } else {
            result = value;
        }

        return result;
    }

    /**
     * All MongoDB documents must have an _id field with a unique value. If the
     * documents do not explicitly specify a value for the _id field, MongoDB
     * creates a unique ObjectId value for the field before inserting it into
     * the collection.
     *
     * When querying for an _id value we have to bear in mind that the value
     * could be an ObjectId or an user supplied value.
     */
    @SuppressWarnings("unchecked")
    private static Object handleMongoDBId(Object value) {

    	Object result = null;
    	try {
    		// IN clause context
    		if (value instanceof List) {
    			final List<Object> items = new ArrayList<Object>();
    			for (Object o : (List<CustomWrapperCondition>) value) {
    				items.add(new ObjectId(o.toString()));
    			}

    			result = items;

    		} else {
    			result = new ObjectId(value.toString());				
    		}
    	} catch (IllegalArgumentException e) {
    		// not a valid ObjectId
    		result = value;
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
        regex = escape(regex, START_OF_LINE);
        regex = escape(regex, END_OF_LINE);
        regex = escape(regex, MONGODB_SINGLE_CHAR);
        regex = escape(regex, MONGODB_ZERO_MORE_CHAR);

        // convert SQL to MongoDB
        regex = regex.replace(SQL_SINGLE_CHAR, MONGODB_SINGLE_CHAR);
        regex = regex.replace(SQL_ZERO_MORE_CHAR, MONGODB_SINGLE_CHAR + MONGODB_ZERO_MORE_CHAR);

        return START_OF_LINE + regex + END_OF_LINE;
    }

    private static String escape(String regex, String metacharacter) {
        return regex.replace(metacharacter, ESCAPE_CHAR + metacharacter);
    }

}
