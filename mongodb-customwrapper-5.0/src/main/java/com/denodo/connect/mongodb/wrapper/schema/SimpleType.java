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

import com.denodo.connect.mongodb.wrapper.util.TypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;


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



}
