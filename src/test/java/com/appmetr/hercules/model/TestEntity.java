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
}
