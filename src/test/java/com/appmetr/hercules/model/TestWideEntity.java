package com.appmetr.hercules.model;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.serializers.TestWideEntitySerializer;
import com.appmetr.hercules.utils.SerializationUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@WideEntity(columnFamily = "TestWideEntityCF")
@RowKey(keyClass = String.class)
//@TopKey(keyClass = String.class)
@Serializer(TestWideEntitySerializer.class)
@Partitioned
public class TestWideEntity implements Externalizable {
//    @RowKey private String rowKey;
    @TopKey public String topKey;

    private String stringData;
    private Integer intData;

    public TestWideEntity() {
    }

    public TestWideEntity(String stringData, Integer intData) {
        this.stringData = stringData;
        this.intData = intData;
    }

    public String getStringData() {
        return stringData;
    }

    public void setStringData(String stringData) {
        this.stringData = stringData;
    }

    public Integer getIntData() {
        return intData;
    }

    public void setIntData(Integer intData) {
        this.intData = intData;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        SerializationUtils.writeNullUTF(out, stringData);
        SerializationUtils.writeNullSafeInt(out, intData);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        stringData = SerializationUtils.readNullUTF(in);
        intData = SerializationUtils.readNullSafeInt(in);
    }

    @Override public String toString() {
        return "TestWideEntity{" +
                "stringData='" + stringData + '\'' +
                ", intData=" + intData +
                '}';
    }
}
