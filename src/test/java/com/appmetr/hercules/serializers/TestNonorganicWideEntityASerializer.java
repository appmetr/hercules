package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityA;
import com.appmetr.hercules.utils.SerializationUtils;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;

import java.nio.ByteBuffer;

public class TestNonorganicWideEntityASerializer extends AbstractHerculesSerializer<TestNonorganicWideEntityA> {
    @Override public ByteBuffer toByteBuffer(TestNonorganicWideEntityA obj) {
        return ByteBuffer.wrap(SerializationUtils.serialize(obj));
    }

    @Override public TestNonorganicWideEntityA fromByteBuffer(ByteBuffer byteBuffer) {
        TestNonorganicWideEntityA entity = new TestNonorganicWideEntityA();
        SerializationUtils.deserialize(BytesArraySerializer.get().fromByteBuffer(byteBuffer), entity);

        return entity;
    }
}
