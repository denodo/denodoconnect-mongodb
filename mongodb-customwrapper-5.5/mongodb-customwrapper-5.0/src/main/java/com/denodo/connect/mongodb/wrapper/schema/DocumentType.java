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

import java.sql.Types;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;


public class DocumentType extends Type {

    private Map<String, Type> fields;

    public DocumentType(String name) {
        super(name);
        this.fields = new LinkedHashMap<String, Type>();
    }

    public Collection<Type> getTypes() {
        return this.fields.values();
    }

    /**
     * The type of a document is the result of the merge (highest common denominator)
     * of its subfields type.
     */
    public void add(Type type) {

        String name = type.getName();
        Type previousType = this.fields.get(name);
        Type newType = type.merge(previousType);

        this.fields.put(name, newType);
    }

    /**
     * If the two types are not equal but both are DocumentTypes the result is
     * the merge of the types of their subfields.
     */
    @Override
    public Type doMerge(Type type) {

        if (!(type instanceof DocumentType)) {
            throw new IllegalArgumentException("Incompatible types");
        }

        for (Type t : ((DocumentType) type).getTypes()) {
            add(t);
        }

        return this;
    }

    @Override
    public int getSQLType() {
        return Types.STRUCT;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSubSchema() {

        CustomWrapperSchemaParameter[] subSchema = new CustomWrapperSchemaParameter[getTypes().size()];
        int i = 0;
        for (Type type : getTypes()) {
            subSchema[i++] = type.buildSchemaParameter();
        }

        return subSchema;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.fields == null) ? 0 : this.fields.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object object) {

        if (object == this) {
            return true;
        }
        if (!(object instanceof DocumentType)) {
            return false;
        }

        DocumentType other = (DocumentType) object;
        if (this.fields == null) {
            if (other.fields != null) {
                return false;
            }
        } else if (!this.fields.equals(other.fields)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "name:" + getName() + ", fields:" + this.fields;
    }


}
