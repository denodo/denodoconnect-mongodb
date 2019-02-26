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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SchemaFieldsParsingUtil {

    private static final char NO_CHAR = (char)0x0;
    private static final String ARRAY_PREFIX = "array(";
    private static final String ARRAY_SUFFIX = ")";




    /*
     * This will return a Map with entries for each declared field. For each field, its value
     * will be either a String with what was declared after the colon (:) for such field, or another
     * Map<String,Object> containing the declaration of a complex object, or an Object[] with only one
     * position containing either a String or a Map, depending on whether the array is of a simple of complex
     * field.
     *
     * Syntax allowed:
     *
     *    field:type
     *    "field":type
     *    'field':type
     *    'field1':type,field2:type,"field3":type
     *    field1:{subfield1:type,subfield2:type},field2:type
     *    field:array(type)
     *    field:array({subfield1:type,subfield2:type})
     */
    public static Map<String,Object> parseSchemaFields(final String text) {

        final List<String> tokens = divideByFieldDeclarations(text);
        if (tokens == null) {
            return null;
        }

        final int tokensSize = tokens.size(); // will always be an even number
        final Map<String,Object> fields = new LinkedHashMap<String,Object>();

        int i = 0;
        while (i < tokensSize) {

            final String fieldName = tokens.get(i++);
            String tokenValue = tokens.get(i++);

            final Object fieldValue;

            boolean isArray = false;
            if (isArraySyntax(tokenValue)) {
                tokenValue = extractArray(tokenValue);
                isArray = true;
            }

            if (isComplexSyntax(tokenValue)) {
                fieldValue = parseSchemaFields(tokenValue);
                if (fieldValue == null) {
                    throw new IllegalArgumentException("Bad syntax: complex field declarations cannot be empty");
                }
            } else {
                fieldValue = tokenValue;
            }

            fields.put(fieldName, isArray? new Object[]{fieldValue} : fieldValue );
        }

        return fields;

    }



    public static String getSchemaStringRepresentation(final Map<String,Object> schema) {
        final StringBuilder stringBuilder = new StringBuilder();
        computeSchemaStringRepresentation(schema, stringBuilder);
        return stringBuilder.toString();
    }

    private static void computeSchemaStringRepresentation(final Map<String,Object> schema, final StringBuilder strBuilder) {

        strBuilder.append("{");
        boolean first = true;
        for (final Map.Entry<String,Object> entry: schema.entrySet()) {

            if (!first) {
                strBuilder.append(",");
            } else {
                first = false;
            }

            strBuilder.append("\"");
            strBuilder.append(entry.getKey());
            strBuilder.append("\"");
            strBuilder.append(":");

            Object value = entry.getValue();

            boolean array = false;
            if (value instanceof Object[]) {
                array = true;
                strBuilder.append(ARRAY_PREFIX);
                value = Array.get(value,0);
            }

            if (value instanceof Map<?,?>) {
                computeSchemaStringRepresentation((Map<String,Object>)value, strBuilder);
            } else {
                strBuilder.append(value);
            }

            if (array) {
                strBuilder.append(ARRAY_SUFFIX);
            }

        }
        strBuilder.append("}");

    }




    /*
     * This will return a list of Strings in which the even items will be field names, and the
     * odd items will be field declarations (i.e. types). Note that field declarations might need
     * further recursive parsing (in case they are complex).
     */
    static List<String> divideByFieldDeclarations(final String text) {

        if (text == null || text.trim().length() == 0) {
            return null;
        }

        String procText = text.trim();
        if (isComplexSyntax(procText)) {
            procText = procText.substring(1,procText.length() - 1).trim();
        }

        final List<String> fieldDeclarations = new ArrayList<String>();

        char literalMarker = NO_CHAR;
        int depth = 0;

        int lastStart = 0;
        boolean hasColon = false;

        char c;
        int i = 0;
        int maxi = procText.length();

        while (i < maxi) {

            c = procText.charAt(i++);

            if (literalMarker != NO_CHAR) {
                // We are inside a literal, so we should ignore all contents unless we are closing it
                if ((c == '\'' || c == '"') && c == literalMarker) {
                    // We are closing the literal!
                    literalMarker = NO_CHAR;
                }
                continue;
            }

            if (c == '\'' || c == '"') {
                // We are opening a literal, nothing else to do
                literalMarker = c;
                continue;
            }

            // From here on, we know we are NOT inside a literal

            if (c == '{') {
                // We are opening a depth level (a complex)
                depth++;
            } else if (c == '}') {
                // We are closing a depth level (a complex structure)
                if (depth <= 0) {
                    throw new IllegalArgumentException("Wrong format: '}' is never open");
                }
                depth--;
            } else if (depth == 0 && c == ':') {
                // We are at the point in which a field name is divided from its value (type declaration)
                fieldDeclarations.add(normalizeFieldName(procText.substring(lastStart, i - 1)));
                lastStart = i;
                hasColon = true;
            } else if (depth == 0 && c == ',') {
                // we are at first level and outside literals, so this comma actually separates fields
                if (!hasColon) {
                    throw new IllegalArgumentException("Wrong format: fields and their types should be separated by ':'");
                }
                if (i == (lastStart + 1)) {
                    throw new IllegalArgumentException("Wrong format: value cannot be empty");
                }
                fieldDeclarations.add(procText.substring(lastStart, i - 1).trim());
                lastStart = i;
                hasColon = false;
            } // if depth > 0 we are not interested, we will parse in a nested execution later

        }

        if (i > lastStart) {
            if (!hasColon) {
                throw new IllegalArgumentException("Wrong format: fields and their types should be separated by ':'");
            }
            fieldDeclarations.add(procText.substring(lastStart).trim());
        } else if (hasColon) {
            throw new IllegalArgumentException("Wrong format: value cannot be empty");
        }

        if (depth > 0) {
            throw new IllegalArgumentException("Wrong format: unclosed '{'");
        }

        return fieldDeclarations;

    }



    private static String normalizeFieldName(final String text) {
        if (text == null || text.trim().length() == 0) {
            throw new IllegalArgumentException("Empty or null schema tokens are not allowed");
        }
        final String procText = text.trim();
        if ((procText.charAt(0) == '\'' && procText.charAt(procText.length() - 1) == '\'') ||
                (procText.charAt(0) == '"' && procText.charAt(procText.length() - 1) == '"')) {
            // Note that ONCE UNQUOTED we should NEVER trim() again, as we'd be losing information
            return procText.substring(1,procText.length() - 1);
        }
        // In such case we simply trim()
        return procText;
    }


    private static boolean isComplexSyntax(final String text) {
        final String procText = text.trim();
        return (procText.charAt(0) == '{' && procText.charAt(procText.length() - 1) == '}');
    }


    private static boolean isArraySyntax(final String text) {
        final String procText = text.trim();
        return procText.startsWith(ARRAY_PREFIX) && procText.endsWith(ARRAY_SUFFIX);
    }


    private static String extractArray(final String text) {
        final String procText = text.trim();
        return procText.substring(ARRAY_PREFIX.length(), procText.length() - ARRAY_SUFFIX.length()).trim();
    }


}
