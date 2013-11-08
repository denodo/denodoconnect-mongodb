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

import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;


public class ArrayType extends Type {


    private Type elementType;

    public ArrayType(String name) {
        super(name);
        this.elementType = null;
    }

    public Type getElementType() {
        return this.elementType;
    }

    /**
     * The type of an array is the result of the merge (highest common denominator)
     * of its elements type.
     */
    public void add(Type type) {
        this.elementType = (this.elementType == null) ? type : this.elementType.merge(type);
    }

    /**
     * If the two types are not equal but both are ArrayTypes the result is the
     * merge of the types of their elements.
     */
    @Override
    public Type doMerge(Type type) {

        if (!(type instanceof ArrayType)) {
            throw new IllegalArgumentException("Incompatible types");
        }

        add(((ArrayType) type).getElementType());
        return this;
    }

    @Override
    public int getSQLType() {
        return Types.ARRAY;
    }

    @Override
    public CustomWrapperSchemaParameter[] getSubSchema() {

        CustomWrapperSchemaParameter[] subSchema = new CustomWrapperSchemaParameter[1];
        subSchema[0] = this.elementType.buildSchemaParameter();

        return subSchema;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result
            + ((this.elementType == null) ? 0 : this.elementType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object object) {

        if (object == this) {
            return true;
        }
        if (!(object instanceof ArrayType)) {
            return false;
        }

        ArrayType other = (ArrayType) object;
        if (this.elementType == null) {
            if (other.elementType != null) {
                return false;
            }
        } else if (!this.elementType.equals(other.elementType)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "name:" + getName() + ", elementType:" + this.elementType;
    }



}
