package com.appmetr.hercules.serializers;

import com.appmetr.hercules.column.TestDatedColumn;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;

import java.nio.ByteBuffer;

public class TestDatedColumnSerializer extends TypeCodec<TestDatedColumn> {


    protected TestDatedColumnSerializer(DataType cqlType, Class<TestDatedColumn> javaClass) {
        super(cqlType, javaClass);
    }

    protected TestDatedColumnSerializer(DataType cqlType, TypeToken<TestDatedColumn> javaType) {
        super(cqlType, javaType);
    }

    @Override public ByteBuffer serialize(TestDatedColumn obj, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (obj == null) {
            return null;
        }
        Long date = obj.getDate();
        return TypeCodec.bigint().serialize(date, protocolVersion);
    }

    @Override public TestDatedColumn deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (byteBuffer == null) {
            return null;
        }
        Long date = TypeCodec.bigint().deserialize(byteBuffer, protocolVersion);
        return new TestDatedColumn(date);
    }

    @Override public TestDatedColumn parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(TestDatedColumn testDatedColumn) throws InvalidTypeException {
        return null;
    }
}
