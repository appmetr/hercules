package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityB;
import com.appmetr.hercules.utils.SerializationUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

public class TestNonorganicWideEntityBSerializer extends TypeCodec<TestNonorganicWideEntityB> {
    protected TestNonorganicWideEntityBSerializer(DataType cqlType, Class<TestNonorganicWideEntityB> javaClass) {
        super(cqlType, javaClass);
    }

    @Override public ByteBuffer serialize(TestNonorganicWideEntityB testNonorganicWideEntityB, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return ByteBuffer.wrap(SerializationUtils.serialize(testNonorganicWideEntityB));
    }

    @Override public TestNonorganicWideEntityB deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        TestNonorganicWideEntityB entity = new TestNonorganicWideEntityB();
        SerializationUtils.deserialize(TypeCodec.blob().deserialize(byteBuffer, protocolVersion).array(), entity);

        return entity;
    }

    @Override public TestNonorganicWideEntityB parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(TestNonorganicWideEntityB testNonorganicWideEntityB) throws InvalidTypeException {
        return null;
    }
}