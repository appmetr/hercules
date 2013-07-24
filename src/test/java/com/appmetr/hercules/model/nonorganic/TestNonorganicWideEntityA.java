package com.appmetr.hercules.model.nonorganic;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.serializers.TestNonorganicWideEntityASerializer;
import com.appmetr.hercules.utils.SerializationUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@WideEntity(columnFamily = "TestNonorganicWideEntity")
@RowKey(keyClass = String.class)
@Serializer(TestNonorganicWideEntityASerializer.class)
@Partitioned
public class TestNonorganicWideEntityA implements Externalizable {
    @TopKey public String topKey;

    private String aClassField;
    private String aClassField2;

    public TestNonorganicWideEntityA() {
    }

    public TestNonorganicWideEntityA(String topKey, String aClassField, String aClassField2) {
        this.topKey = topKey;
        this.aClassField = aClassField;
        this.aClassField2 = aClassField2;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        SerializationUtils.writeNullUTF(out, aClassField);
        SerializationUtils.writeNullUTF(out, aClassField2);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        aClassField = SerializationUtils.readNullUTF(in);
        aClassField2 = SerializationUtils.readNullUTF(in);
    }

    @Override public String toString() {
        return "TestNonorganicWideEntityA{" +
                "topKey=" + topKey +
                ", aClassField='" + aClassField + '\'' +
                ", aClassField2='" + aClassField2 + '\'' +
                '}';
    }
}
