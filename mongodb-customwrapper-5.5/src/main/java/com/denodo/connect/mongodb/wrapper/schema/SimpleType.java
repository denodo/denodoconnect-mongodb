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

import com.denodo.connect.mongodb.wrapper.util.TypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;
import org.bson.BsonTimestamp;


public class SimpleType extends Type {

    private static final Class<Object> GENERIC_CLASS = Object.class;
    private static final Class<String> DEFAULT_CLASS = String.class;

    private Class<?> javaClass;


    public SimpleType(String name, Class<?> javaClass) {
        super(name);
        this.javaClass = javaClass;
    }

    public static SimpleType generic(String name) {
        return new SimpleType(name, GENERIC_CLASS);
    }

    public Class<?> getJavaClass() {
        return this.javaClass;
    }

    public void setJavaClass(Class<?> javaClass) {
        this.javaClass = javaClass;
    }

    /**
     * If the two types are not equal but both are SimpleTypes the result is a
     * SimpleType of String.class.
     */
    @Override
    public Type doMerge(Type type) {

        if (!(type instanceof SimpleType)) {
            throw new IllegalArgumentException("Incompatible types");
        }
        setJavaClass(DEFAULT_CLASS);
        return this;
    }

    @Override
    public int getSQLType() {
        return TypeUtils.toSQL(getJavaClass());
    }

    @Override
    public CustomWrapperSchemaParameter[] getSubSchema() {
        return null;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.javaClass == null) ? 0 : this.javaClass.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (!(obj instanceof SimpleType)) {
            return false;
        }
        SimpleType other = (SimpleType) obj;
        if (this.javaClass == null) {
            if (other.javaClass != null) {
                return false;
            }
        } else if (!this.javaClass.equals(other.javaClass)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "name:" + getName() + ", javaClass:" + this.javaClass.getSimpleName();
    }

    /**
     * Transforms this type to a parameter of the Custom Wrapper's schema.
     * The SQL type and the subschema may vary depending on the concrete type.
     */
    @Override
    public CustomWrapperSchemaParameter buildSchemaParameter() {

        boolean searchable = true;
        boolean updateable = true;
        boolean nullable = true;
        boolean mandatory = true;

        int sqlType = getSQLType();
        CustomWrapperSchemaParameter[] subSchema = getSubSchema();
        
        //  BSON Timestamp fields are supported but are not allowed to be present in WHERE clauses. 
        // The reason is, due to the impossibility to differentiate them from the normal BSON Date type, 
        // BSON Timestamp have no way of being adequately represented in the query documents. 
        // We make BSON Timestamps "not-searchables" so they can be filtered at the VDP side
        if (Types.TIMESTAMP == sqlType && this.getJavaClass().equals(BsonTimestamp.class)) {
        	return new CustomWrapperSchemaParameter(getName(), sqlType,
                    subSchema, false, CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
                    updateable, nullable, !mandatory);
        } else if (sqlType != Types.ARRAY && sqlType!= Types.STRUCT) {
            return new CustomWrapperSchemaParameter(getName(), sqlType,
                    subSchema, searchable, CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
                    updateable, nullable, !mandatory);
        } 
        return new CustomWrapperSchemaParameter(getName(), sqlType,
            subSchema, searchable, CustomWrapperSchemaParameter.ASC_AND_DESC_SORT,
            !updateable, nullable, !mandatory);
    }

}
