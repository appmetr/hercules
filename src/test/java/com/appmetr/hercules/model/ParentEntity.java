package com.appmetr.hercules.model;

import com.appmetr.hercules.annotations.Entity;
import com.appmetr.hercules.annotations.Id;

@Entity
public class ParentEntity {
    @Id public String id;

    public String data;

    public ParentEntity() {
    }

    public ParentEntity(String id, String data) {
        this.id = id;
        this.data = data;
    }

    @Override public String toString() {
        return "ParentEntity{" +
                "id='" + id + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
