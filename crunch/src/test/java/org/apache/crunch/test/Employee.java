/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.test;

@SuppressWarnings("all")
public class Employee extends org.apache.avro.specific.SpecificRecordBase implements
    org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
      .parse("{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"org.apache.crunch.test\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"salary\",\"type\":\"int\"},{\"name\":\"department\",\"type\":[\"string\",\"null\"]}]}");
  @Deprecated
  public java.lang.CharSequence name;
  @Deprecated
  public int salary;
  @Deprecated
  public java.lang.CharSequence department;

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0:
      return name;
    case 1:
      return salary;
    case 2:
      return department;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader. Applications should not call.
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0:
      name = (java.lang.CharSequence) value$;
      break;
    case 1:
      salary = (java.lang.Integer) value$;
      break;
    case 2:
      department = (java.lang.CharSequence) value$;
      break;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'name' field.
   */
  public java.lang.CharSequence getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'salary' field.
   */
  public java.lang.Integer getSalary() {
    return salary;
  }

  /**
   * Sets the value of the 'salary' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setSalary(java.lang.Integer value) {
    this.salary = value;
  }

  /**
   * Gets the value of the 'department' field.
   */
  public java.lang.CharSequence getDepartment() {
    return department;
  }

  /**
   * Sets the value of the 'department' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setDepartment(java.lang.CharSequence value) {
    this.department = value;
  }

  /** Creates a new Employee RecordBuilder */
  public static org.apache.crunch.test.Employee.Builder newBuilder() {
    return new org.apache.crunch.test.Employee.Builder();
  }

  /** Creates a new Employee RecordBuilder by copying an existing Builder */
  public static org.apache.crunch.test.Employee.Builder newBuilder(org.apache.crunch.test.Employee.Builder other) {
    return new org.apache.crunch.test.Employee.Builder(other);
  }

  /**
   * Creates a new Employee RecordBuilder by copying an existing Employee
   * instance
   */
  public static org.apache.crunch.test.Employee.Builder newBuilder(org.apache.crunch.test.Employee other) {
    return new org.apache.crunch.test.Employee.Builder(other);
  }

  /**
   * RecordBuilder for Employee instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Employee> implements
      org.apache.avro.data.RecordBuilder<Employee> {

    private java.lang.CharSequence name;
    private int salary;
    private java.lang.CharSequence department;

    /** Creates a new Builder */
    private Builder() {
      super(org.apache.crunch.test.Employee.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(org.apache.crunch.test.Employee.Builder other) {
      super(other);
    }

    /** Creates a Builder by copying an existing Employee instance */
    private Builder(org.apache.crunch.test.Employee other) {
      super(org.apache.crunch.test.Employee.SCHEMA$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = (java.lang.CharSequence) data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.salary)) {
        this.salary = (java.lang.Integer) data().deepCopy(fields()[1].schema(), other.salary);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.department)) {
        this.department = (java.lang.CharSequence) data().deepCopy(fields()[2].schema(), other.department);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'name' field */
    public java.lang.CharSequence getName() {
      return name;
    }

    /** Sets the value of the 'name' field */
    public org.apache.crunch.test.Employee.Builder setName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /** Checks whether the 'name' field has been set */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }

    /** Clears the value of the 'name' field */
    public org.apache.crunch.test.Employee.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'salary' field */
    public java.lang.Integer getSalary() {
      return salary;
    }

    /** Sets the value of the 'salary' field */
    public org.apache.crunch.test.Employee.Builder setSalary(int value) {
      validate(fields()[1], value);
      this.salary = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /** Checks whether the 'salary' field has been set */
    public boolean hasSalary() {
      return fieldSetFlags()[1];
    }

    /** Clears the value of the 'salary' field */
    public org.apache.crunch.test.Employee.Builder clearSalary() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'department' field */
    public java.lang.CharSequence getDepartment() {
      return department;
    }

    /** Sets the value of the 'department' field */
    public org.apache.crunch.test.Employee.Builder setDepartment(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.department = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /** Checks whether the 'department' field has been set */
    public boolean hasDepartment() {
      return fieldSetFlags()[2];
    }

    /** Clears the value of the 'department' field */
    public org.apache.crunch.test.Employee.Builder clearDepartment() {
      department = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public Employee build() {
      try {
        Employee record = new Employee();
        record.name = fieldSetFlags()[0] ? this.name : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.salary = fieldSetFlags()[1] ? this.salary : (java.lang.Integer) defaultValue(fields()[1]);
        record.department = fieldSetFlags()[2] ? this.department : (java.lang.CharSequence) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
