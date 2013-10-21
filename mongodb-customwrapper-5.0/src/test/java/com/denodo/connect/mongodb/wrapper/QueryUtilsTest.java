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
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;
import com.mongodb.DBObject;



@RunWith(PowerMockRunner.class)
@PrepareForTest({CustomWrapperSimpleCondition.class, CustomWrapperAndCondition.class, CustomWrapperOrCondition.class})
public class QueryUtilsTest {


    private static CustomWrapperSimpleCondition buildSimpleCondition(String fieldName,
        String operator, int valueType, Object value) {

        CustomWrapperSimpleCondition simpleCondition = PowerMockito.mock(CustomWrapperSimpleCondition.class);
        when(simpleCondition.isSimpleCondition()).thenReturn(true);

        CustomWrapperFieldExpression field = new CustomWrapperFieldExpression(fieldName);
        CustomWrapperSimpleExpression rightExpression = new CustomWrapperSimpleExpression(valueType, value);

        when(simpleCondition.getField()).thenReturn(field);
        when(simpleCondition.getOperator()).thenReturn(operator);
        when(simpleCondition.getRightExpression()).thenReturn(new CustomWrapperSimpleExpression[] { rightExpression });

        return simpleCondition;
    }

    private static CustomWrapperAndCondition buildANDCondition(CustomWrapperCondition firstCondition,
        CustomWrapperCondition secondCondition) {

        List<CustomWrapperCondition> list = new ArrayList<CustomWrapperCondition>();
        list.add(firstCondition);
        list.add(secondCondition);

        CustomWrapperAndCondition andCondition = PowerMockito.mock(CustomWrapperAndCondition.class);
        when(andCondition.isAndCondition()).thenReturn(true);
        when(andCondition.getConditions()).thenReturn(list);
        return andCondition;
    }

    private static CustomWrapperOrCondition buildORCondition(CustomWrapperCondition firstCondition,
        CustomWrapperCondition secondCondition) {

        List<CustomWrapperCondition> list = new ArrayList<CustomWrapperCondition>();
        list.add(firstCondition);
        list.add(secondCondition);

        CustomWrapperOrCondition orCondition = PowerMockito.mock(CustomWrapperOrCondition.class);
        when(orCondition.isOrCondition()).thenReturn(true);
        when(orCondition.getConditions()).thenReturn(list);

        return orCondition;
    }

    /*
     * VDP condition: WHERE status = "A"
     */
    @Test
    public void testEqualConditionQuery() {

        CustomWrapperSimpleCondition simpleCondition =
            buildSimpleCondition("status", OPERATOR_EQ, Types.VARCHAR, "A");

        DBObject query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"status\" : \"A\"}", query.toString());
    }


    /*
     * VDP condition: WHERE status != "A"
     */
    @Test
    public void testNotEqualConditionQuery() {

        CustomWrapperSimpleCondition simpleCondition =
            buildSimpleCondition("status", OPERATOR_NE, Types.VARCHAR, "A");


        DBObject query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"status\" : { \"$ne\" : \"A\"}}", query.toString());
    }

    /*
     * VDP condition: WHERE age > 25
     */
    @Test
    public void testGreaterThanConditionQuery() {

        CustomWrapperSimpleCondition simpleCondition =
            buildSimpleCondition("age", OPERATOR_GT, Types.NUMERIC, Integer.valueOf(25));


        DBObject query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"age\" : { \"$gt\" : 25}}", query.toString());
    }

    /*
     * VDP condition: WHERE age >= 25
     */
    @Test
    public void testGreaterThanEqualConditionQuery() {

        CustomWrapperSimpleCondition simpleCondition =
            buildSimpleCondition("age", OPERATOR_GE, Types.NUMERIC, Integer.valueOf(25));


        DBObject query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"age\" : { \"$gte\" : 25}}", query.toString());
    }

    /*
     * VDP condition: WHERE age < 25
     */
    @Test
    public void testLessThanConditionQuery() {

        CustomWrapperSimpleCondition simpleCondition =
            buildSimpleCondition("age", OPERATOR_LT, Types.NUMERIC, Integer.valueOf(25));


        DBObject query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"age\" : { \"$lt\" : 25}}", query.toString());
    }

    /*
     * VDP condition: WHERE age <= 25
     */
    @Test
    public void testLessThanEqualConditionQuery() {

        CustomWrapperSimpleCondition simpleCondition =
            buildSimpleCondition("age", OPERATOR_LE, Types.NUMERIC, Integer.valueOf(25));


        DBObject query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"age\" : { \"$lte\" : 25}}", query.toString());
    }

    /*
     * VDP condition: WHERE user_id like "%bc%"
     */
    @Test
    public void testLikeConditionQuery() {

        CustomWrapperSimpleCondition simpleCondition =
            buildSimpleCondition("user_id", OPERATOR_LIKE, Types.VARCHAR, "%bc%");


        DBObject query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"user_id\" : { \"$regex\" : \"^.*bc.*$\"}}", query.toString());

        simpleCondition = buildSimpleCondition("user_id", OPERATOR_LIKE, Types.VARCHAR, "b$c");
        query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"user_id\" : { \"$regex\" : \"^b\\\\$c$\"}}", query.toString());

        simpleCondition = buildSimpleCondition("user_id", OPERATOR_LIKE, Types.VARCHAR, "b^c");
        query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"user_id\" : { \"$regex\" : \"^b\\\\^c$\"}}", query.toString());

        simpleCondition = buildSimpleCondition("user_id", OPERATOR_LIKE, Types.VARCHAR, "b.c");
        query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"user_id\" : { \"$regex\" : \"^b\\\\.c$\"}}", query.toString());

        simpleCondition = buildSimpleCondition("user_id", OPERATOR_LIKE, Types.VARCHAR, "b.*c");
        query = QueryUtils.buildQuery(simpleCondition);
        Assert.assertEquals("{ \"user_id\" : { \"$regex\" : \"^b\\\\.\\\\*c$\"}}", query.toString());
    }

    /*
     * VDP condition: WHERE status = "A" AND age < 25
     */
    @Test
    public void testANDConditionQuery() {

        CustomWrapperSimpleCondition firstCondition =
            buildSimpleCondition("status", OPERATOR_EQ, Types.VARCHAR, "A");
        CustomWrapperSimpleCondition secondCondition =
            buildSimpleCondition("age", OPERATOR_LE, Types.NUMERIC, Integer.valueOf(25));

        CustomWrapperAndCondition andCondition = buildANDCondition(firstCondition, secondCondition);

        DBObject query = QueryUtils.buildQuery(andCondition);
        Assert.assertEquals("{ \"$and\" : [ { \"status\" : \"A\"} , { \"age\" : { \"$lte\" : 25}}]}",
            query.toString());
    }

    /*
     * VDP condition: WHERE status like "A%" OR age != 25
     */
    @Test
    public void testORConditionQuery() {

        CustomWrapperSimpleCondition firstCondition =
            buildSimpleCondition("status", OPERATOR_LIKE, Types.VARCHAR, "A_");
        CustomWrapperSimpleCondition secondCondition =
            buildSimpleCondition("age", OPERATOR_NE, Types.NUMERIC, Integer.valueOf(25));


        CustomWrapperOrCondition orCondition = buildORCondition(firstCondition, secondCondition);

        DBObject query = QueryUtils.buildQuery(orCondition);
        Assert.assertEquals("{ \"$or\" : [ { \"status\" : { \"$regex\" : \"^A.$\"}} , { \"age\" : { \"$ne\" : 25}}]}",
            query.toString());
    }

    /*
     * VDP condition: WHERE price = 1.99 AND ( qty < 20) OR (sale = true))
     */
    @Test
    public void testANDComplexConditionQuery() {

        CustomWrapperSimpleCondition condition =
            buildSimpleCondition("price", OPERATOR_EQ, Types.NUMERIC, Double.valueOf(1.99));

        CustomWrapperSimpleCondition firstORCondition =
            buildSimpleCondition("qty", OPERATOR_LT, Types.NUMERIC, Integer.valueOf(20));

        CustomWrapperSimpleCondition secondORCondition =
            buildSimpleCondition("sale", OPERATOR_EQ, Types.BOOLEAN, Boolean.TRUE);


        CustomWrapperOrCondition orCondition = buildORCondition(firstORCondition, secondORCondition);
        CustomWrapperAndCondition andCondition = buildANDCondition(condition, orCondition);

        DBObject query = QueryUtils.buildQuery(andCondition);
        Assert.assertEquals("{ \"$and\" : [ { \"price\" : 1.99} , { \"$or\" : [ { \"qty\" : { \"$lt\" : 20}} , { \"sale\" : true}]}]}",
            query.toString());
    }

    /*
     * VDP condition: WHERE price = 1.99 OR ( qty < 20) AND (sale = true))
     */
    @Test
    public void testORComplexConditionQuery() {

        CustomWrapperSimpleCondition condition =
            buildSimpleCondition("price", OPERATOR_EQ, Types.NUMERIC, Double.valueOf(1.99));

        CustomWrapperSimpleCondition firstANDCondition =
            buildSimpleCondition("qty", OPERATOR_LT, Types.NUMERIC, Integer.valueOf(20));

        CustomWrapperSimpleCondition secondANDCondition =
            buildSimpleCondition("sale", OPERATOR_EQ, Types.BOOLEAN, Boolean.TRUE);


        CustomWrapperAndCondition andCondition = buildANDCondition(firstANDCondition, secondANDCondition);
        CustomWrapperOrCondition orCondition = buildORCondition(condition, andCondition);

        DBObject query = QueryUtils.buildQuery(orCondition);
        Assert.assertEquals("{ \"$or\" : [ { \"price\" : 1.99} , { \"$and\" : [ { \"qty\" : { \"$lt\" : 20}} , { \"sale\" : true}]}]}",
            query.toString());
    }
}
