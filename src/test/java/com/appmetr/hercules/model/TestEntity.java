package com.appmetr.hercules.model;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.keys.ParentFK;

import java.io.Serializable;

@Entity
@PKIndex
@Indexes({
        @Index(keyClass = ParentFK.class)
})
public class TestEntity implements Serializable {
    @Id public String id;

    public String stringValue;
    public Integer intValue;
    public Long longValue;
    public Double doubleValue;
    public Float floatValue;

    public String parent;

    @NotNullField public String notNullValue = "notNull";
    public String nullField;

    public TestEntity() {
    }

    public TestEntity(String id) {
        this.id = id;
        this.stringValue = "value" + id;
        this.intValue = Integer.valueOf(id);
        this.longValue = Long.valueOf(id);
        this.doubleValue = Double.valueOf(id);
        this.floatValue = Float.valueOf(id);

        this.parent = Integer.valueOf(id) % 2 == 0 ? "even" : "odd";
    }

    @Override public String toString() {
        return "TestEntity{" +
                "id='" + id + '\'' +
                ", stringValue='" + stringValue + '\'' +
                ", intValue=" + intValue +
                ", longValue=" + longValue +
                ", doubleValue=" + doubleValue +
                ", floatValue=" + floatValue +
                ", parent='" + parent + '\'' +
                ", notNullValue='" + notNullValue + '\'' +
                ", nullField='" + nullField + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestEntity entity = (TestEntity) o;

        if (doubleValue != null ? !doubleValue.equals(entity.doubleValue) : entity.doubleValue != null) return false;
        if (floatValue != null ? !floatValue.equals(entity.floatValue) : entity.floatValue != null) return false;
        if (id != null ? !id.equals(entity.id) : entity.id != null) return false;
        if (intValue != null ? !intValue.equals(entity.intValue) : entity.intValue != null) return false;
        if (longValue != null ? !longValue.equals(entity.longValue) : entity.longValue != null) return false;
        if (notNullValue != null ? !notNullValue.equals(entity.notNullValue) : entity.notNullValue != null)
            return false;
        if (nullField != null ? !nullField.equals(entity.nullField) : entity.nullField != null) return false;
        if (parent != null ? !parent.equals(entity.parent) : entity.parent != null) return false;
        if (stringValue != null ? !stringValue.equals(entity.stringValue) : entity.stringValue != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (stringValue != null ? stringValue.hashCode() : 0);
        result = 31 * result + (intValue != null ? intValue.hashCode() : 0);
        result = 31 * result + (longValue != null ? longValue.hashCode() : 0);
        result = 31 * result + (doubleValue != null ? doubleValue.hashCode() : 0);
        result = 31 * result + (floatValue != null ? floatValue.hashCode() : 0);
        result = 31 * result + (parent != null ? parent.hashCode() : 0);
        result = 31 * result + (notNullValue != null ? notNullValue.hashCode() : 0);
        result = 31 * result + (nullField != null ? nullField.hashCode() : 0);
        return result;
    }
}
