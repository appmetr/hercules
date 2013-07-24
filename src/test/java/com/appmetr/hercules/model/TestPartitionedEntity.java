package com.appmetr.hercules.model;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.column.TestDatedColumn;
import com.appmetr.hercules.partition.MonthDatePartitionProvider;
import com.appmetr.hercules.serializers.TestDatedColumnSerializer;
import com.appmetr.hercules.serializers.TestPartitionedEntitySerializer;
import com.appmetr.hercules.utils.SerializationUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@WideEntity
@RowKey(keyClass = String.class)
@Serializer(TestPartitionedEntitySerializer.class)
@Partitioned(MonthDatePartitionProvider.class)
public class TestPartitionedEntity implements Externalizable {
    @TopKey(serializer = TestDatedColumnSerializer.class) public TestDatedColumn topKey;

    private String data;

    public TestPartitionedEntity() {
    }

    public TestPartitionedEntity(TestDatedColumn topKey, String data) {
        this.topKey = topKey;
        this.data = data;
    }

    public TestDatedColumn getTopKey() {
        return topKey;
    }

    public void setTopKey(TestDatedColumn topKey) {
        this.topKey = topKey;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        SerializationUtils.writeNullUTF(out, data);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = SerializationUtils.readNullUTF(in);
    }

    @Override public String toString() {
        return "TestPartitionedEntity{" +
                "topKey=" + topKey +
                ", data='" + data + '\'' +
                '}';
    }
}
