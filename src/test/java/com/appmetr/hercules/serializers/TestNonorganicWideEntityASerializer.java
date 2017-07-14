package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityA;
import com.appmetr.hercules.utils.SerializationUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

public class TestNonorganicWideEntityASerializer extends TypeCodec<TestNonorganicWideEntityA> {


    protected TestNonorganicWideEntityASerializer() {
        super(DataType.blob(), TestNonorganicWideEntityA.class);
    }


    @Override public ByteBuffer serialize(TestNonorganicWideEntityA obj, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return ByteArrayCodec.bytearray().serialize(SerializationUtils.serialize(obj), protocolVersion);
    }

    @Override public TestNonorganicWideEntityA deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        TestNonorganicWideEntityA entity = new TestNonorganicWideEntityA();
        SerializationUtils.deserialize(ByteArrayCodec.bytearray().deserialize(byteBuffer, protocolVersion), entity);

        return entity;
    }

    @Override public TestNonorganicWideEntityA parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(TestNonorganicWideEntityA testNonorganicWideEntityA) throws InvalidTypeException {
        return null;
    }
}
