package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityA;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;

import java.nio.ByteBuffer;

public class TestNonorganicWideEntityASerializer extends TypeCodec<TestNonorganicWideEntityA> {


    protected TestNonorganicWideEntityASerializer(DataType cqlType, Class<TestNonorganicWideEntityA> javaClass) {
        super(cqlType, javaClass);
    }

    protected TestNonorganicWideEntityASerializer(DataType cqlType, TypeToken<TestNonorganicWideEntityA> javaType) {
        super(cqlType, javaType);
    }

    @Override public ByteBuffer serialize(TestNonorganicWideEntityA testNonorganicWideEntityA, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return null;
    }

    @Override public TestNonorganicWideEntityA deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return null;
    }

    @Override public TestNonorganicWideEntityA parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(TestNonorganicWideEntityA testNonorganicWideEntityA) throws InvalidTypeException {
        return null;
    }
}
