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

import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;


public abstract class Type  {


    private String name;

    public Type(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Calculates the highest common denominator between the two types.
     * Common fields in a collection's documents may hold different types of data.
     * Then, the type of a common field will be the highest common denominator between
     * all the fields with the same name.
     *
     * If the two types are equal no merged is needed.
     * If the two types are not equal but both are of the same type specific merge is required.
     * Otherwise the two types are incompatible and the resulting type is a
     * generic type: SimpleType of Object.class. VDP handles it as text.
     */
    public Type merge(Type type) {

        if (type == null || equals(type)) {
            return this;
        }

        Type merged = null;
        try {
            merged = doMerge(type);
        } catch (IllegalArgumentException e) {
            // incompatible types
            merged = SimpleType.generic(getName());
        }

        return merged;
    }

    /**
     * Transforms this type to a parameter of the Custom Wrapper's schema.
     * The SQL type and the subschema may vary depending on the concrete type.
     */
    public CustomWrapperSchemaParameter buildSchemaParameter() {

        boolean searchable = true;
        boolean updateable = true;
        boolean nullable = true;
        boolean mandatory = true;

        int sqlType = getSQLType();
        CustomWrapperSchemaParameter[] subSchema = getSubSchema();

        return new CustomWrapperSchemaParameter(getName(), sqlType,
            subSchema, searchable, CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
            !updateable, nullable, !mandatory);
    }

    /**
     * Call when two types are both of the same type but they are not identical.
     */
    public abstract Type doMerge(Type type);

    public abstract int getSQLType();
    public abstract CustomWrapperSchemaParameter[] getSubSchema();

}
