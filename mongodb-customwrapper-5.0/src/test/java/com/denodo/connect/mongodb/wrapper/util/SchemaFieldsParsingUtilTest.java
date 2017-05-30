/*
 * =============================================================================
 *
 *   This software is part of the denodo developer toolkit.
 *
 *   Copyright (c) 2017, denodo technologies (http://www.denodo.com)
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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;


public class SchemaFieldsParsingUtilTest {



    @Test
    public void testDivideByFieldDeclarations() throws Exception {

        checkDivideByFieldDeclarations("",null);
        checkDivideByFieldDeclarations(" ",null);
        checkDivideByFieldDeclarations("a:A","a", "A");
        checkDivideByFieldDeclarations("a:1,b:3","a", "1", "b", "3");
        checkDivideByFieldDeclarations("'a,b':2","a,b", "2");
        checkDivideByFieldDeclarations("\"a,b\":'lala'","a,b","'lala'");
        checkDivideByFieldDeclarations("\"''''a''''',b\":a,a:22,c:a","''''a''''',b","a","a","22","c","a");
        checkDivideByFieldDeclarations("\"a,b\":1,c:2","a,b", "1","c","2");
        checkDivideByFieldDeclarations("\"a,b\":1,c: {'a':'123','b':'2314',c:{v:23123}},d:'{as:as}'","a,b", "1","c","{'a':'123','b':'2314',c:{v:23123}}", "d","'{as:as}'");
        checkDivideByFieldDeclarations("\"a,b\":1,c: {'a':'123','b':'2314',c:{v:23123}},d:{as:as}","a,b", "1","c","{'a':'123','b':'2314',c:{v:23123}}", "d","{as:as}");
        checkDivideByFieldDeclarations("\"a,b\":1,c: {'a':'123','b':'2314',c:{v:23123}},d:'aa,b'","a,b", "1", "c","{'a':'123','b':'2314',c:{v:23123}}", "d","'aa,b'");
        checkDivideByFieldDeclarations("\"a,b\":1,    c: {'a':'123','b':'2314',c:{v:23123}}   , d:'aa, b'","a,b", "1", "c","{'a':'123','b':'2314',c:{v:23123}}", "d","'aa, b'");
        checkDivideByFieldDeclarations("{\"a,b\":1,    c: {'a':'123','b':'2314',c:{v:23123}}   , d:'aa, b'}","a,b", "1", "c","{'a':'123','b':'2314',c:{v:23123}}", "d","'aa, b'");
        checkDivideByFieldDeclarations("  {   \"a,b\":1,    c: {'a':'123','b':'2314',c:{v:23123}}   , d:'aa, b'  }   ","a,b", "1", "c","{'a':'123','b':'2314',c:{v:23123}}", "d","'aa, b'");

    }

    private static void checkDivideByFieldDeclarations(final String text, final String... expected) {
        final List<String> result = SchemaFieldsParsingUtil.divideByFieldDeclarations(text);
        Assert.assertArrayEquals(expected, result == null? null : result.toArray(new String[result.size()]));
    }




    @Test
    public void testParseSchemaFields() throws Exception {

        Assert.assertEquals(
                "{\"a,b\":1,\"c\":{\"a\":'123',\"b\":'2314',\"c\":{\"v\":23123}},\"d\":'{as:as}'}",
                SchemaFieldsParsingUtil.getSchemaStringRepresentation(SchemaFieldsParsingUtil.parseSchemaFields("\"a,b\":1,c: {'a':'123','b':'2314',c:{v:23123}},d:'{as:as}'")));
        Assert.assertEquals(
                "{\"a,b\":1,\"c\":{\"a\":'123',\"b\":'2314',\"c\":{\"v\":23123}},\"d\":{\"as\":as}}",
                SchemaFieldsParsingUtil.getSchemaStringRepresentation(SchemaFieldsParsingUtil.parseSchemaFields("\"a,b\":1,c: {'a':'123','b':'2314',c:{v:23123}},d:{as:as}")));
        Assert.assertEquals(
                "{\"a,b\":array(1),\"c\":array({\"a\":'123',\"b\":'2314',\"c\":{\"v\":23123}}),\"d\":{\"as\":as}}",
                SchemaFieldsParsingUtil.getSchemaStringRepresentation(SchemaFieldsParsingUtil.parseSchemaFields("\"a,b\":array(1),c: array({'a':'123','b':'2314',c:{v:23123}}),d:{as:as}")));
        Assert.assertEquals(
                "{\"a,b\":array(1),\"c\":array({\"a\":'123',\"b\":'2314',\"c\":{\"v\":array(23123)}}),\"d\":{\"as\":as}}",
                SchemaFieldsParsingUtil.getSchemaStringRepresentation(SchemaFieldsParsingUtil.parseSchemaFields("\"a,b\":array(1),c: array({'a':'123','b':'2314',c:{v:array(23123)}}),d:{as:as}")));

    }




}
