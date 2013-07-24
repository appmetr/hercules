package com.appmetr.hercules.model;

import com.appmetr.hercules.annotations.Entity;
import com.appmetr.hercules.annotations.Id;
import com.appmetr.hercules.annotations.comparator.EntityComparatorType;

@Entity(comparatorType = EntityComparatorType.INTEGERTYPE)
public class TestRangeEntity {
    @Id public Integer id;

    public String name;

    public TestRangeEntity() {
    }

    public TestRangeEntity(Integer id, String name) {
        this.id = id;
        this.name = name;
    }
}
