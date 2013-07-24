package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityB;
import com.appmetr.hercules.utils.SerializationUtils;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;

import java.nio.ByteBuffer;

public class TestNonorganicWideEntityBSerializer extends AbstractHerculesSerializer<TestNonorganicWideEntityB> {
    @Override public ByteBuffer toByteBuffer(TestNonorganicWideEntityB obj) {
        return ByteBuffer.wrap(SerializationUtils.serialize(obj));
    }

    @Override public TestNonorganicWideEntityB fromByteBuffer(ByteBuffer byteBuffer) {
        TestNonorganicWideEntityB entity = new TestNonorganicWideEntityB();
        SerializationUtils.deserialize(BytesArraySerializer.get().fromByteBuffer(byteBuffer), entity);

        return entity;
    }
}