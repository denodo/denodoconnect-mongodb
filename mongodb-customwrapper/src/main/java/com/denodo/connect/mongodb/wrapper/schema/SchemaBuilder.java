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
package com.denodo.connect.mongodb.wrapper.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import org.bson.Document;


public class SchemaBuilder {

    /*
     * Indicates the name of an element array. When used at MongoDB queries this name
     * will be removed.
     * For example: 'memos' field contains an array that contains subdocuments with the field 'by':
     * VDP will query for 'memos.memos_ITEM.by' and MongoDB will query for 'memos.by'.
     */
    public static final String ARRAY_ITEM_SUFFIX = "_ITEM";
    private static final Class<String> DEFAULT_CLASS = String.class;

    private DocumentType type;


    public SchemaBuilder() {
        this.type = new DocumentType("Schema");
    }

    public void addToSchema(Document document) {

        for (String key : document.keySet()) {
            boolean typeFound = false;
            Object field = document.get(key);
            if (field != null) {
                Type fieldType = getFieldType(key, field);
                if (fieldType != null){
                    this.type.add(fieldType);
                    typeFound = true;
                }
            }
            if (!typeFound) {
                // Types can be null, but they need to be added anyway
                this.type.add(new SimpleType(key, DEFAULT_CLASS));
            }
        }
    }

    public CustomWrapperSchemaParameter[] buildSchema() {

        Collection<CustomWrapperSchemaParameter> schema = new ArrayList<CustomWrapperSchemaParameter>();
        for (Type t : this.type.getTypes()) {
            schema.add(t.buildSchemaParameter());
        }

        return schema.toArray(new CustomWrapperSchemaParameter[schema.size()]);
    }

    private Type getFieldType(String key, Object field) {

        Type fieldType = null;
        if(field instanceof ArrayList){
            ArrayList<Object> array = (ArrayList<Object>) field;
            if(!array.isEmpty()){
                ArrayType arrayType = new ArrayType(key);
                boolean isEmpty=true;
                for (Object item : array) {
                    Type subType =getFieldType(key+ARRAY_ITEM_SUFFIX,item);
                    if(subType!=null){
                        arrayType.add(subType);
                        isEmpty=false;
                    }
                }
                if (!isEmpty){//if the array is empty is not possible to represent in VDP 
                    fieldType = arrayType;
                }
            }

        } else if (field instanceof Document) {

            Document subDocument = (Document) field;
            DocumentType documentType = new DocumentType(key);
            if(!subDocument.isEmpty()){
                // if the document is empty it is not possible to represent in VDP
                boolean isEmpty = true;
                for (Map.Entry<String, Object> entry : subDocument.entrySet()) {
                    final Type entryType = getFieldType(entry.getKey(), entry.getValue());
                    if (entryType != null) {
                        documentType.add(entryType);
                        isEmpty = false;
                    }
                }
                if (!isEmpty) {
                    // if the document is empty it is not possible to represent in VDP
                    fieldType = documentType;
                }
            }
        } else {
            if(field!= null){
                fieldType = new SimpleType(key, field.getClass());
            } else {
                fieldType = new SimpleType(key, DEFAULT_CLASS);
            }
        }

        return fieldType;
    }

}
