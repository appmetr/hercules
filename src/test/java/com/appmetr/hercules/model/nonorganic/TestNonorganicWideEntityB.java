package com.appmetr.hercules.model.nonorganic;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.serializers.TestNonorganicWideEntityBSerializer;
import com.appmetr.hercules.utils.SerializationUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@WideEntity(columnFamily = "TestNonorganicWideEntity")
@RowKey(keyClass = String.class)
@Serializer(TestNonorganicWideEntityBSerializer.class)
@Partitioned
public class TestNonorganicWideEntityB implements Externalizable {
    @TopKey public String topKey;

    private String bClassField;

    public TestNonorganicWideEntityB() {
    }

    public TestNonorganicWideEntityB(String topKey, String bClassField) {
        this.topKey = topKey;
        this.bClassField = bClassField;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        SerializationUtils.writeNullUTF(out, bClassField);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bClassField = SerializationUtils.readNullUTF(in);
    }

    @Override public String toString() {
        return "TestNonorganicWideEntityB{" +
                "topKey=" + topKey +
                ", bClassField='" + bClassField + '\'' +
                '}';
    }
}
