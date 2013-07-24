package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.TestWideEntity;
import com.appmetr.hercules.utils.SerializationUtils;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;

import java.nio.ByteBuffer;

public class TestWideEntitySerializer extends AbstractHerculesSerializer<TestWideEntity> {
    @Override public ByteBuffer toByteBuffer(TestWideEntity obj) {
        return ByteBuffer.wrap(SerializationUtils.serialize(obj));
    }

    @Override public TestWideEntity fromByteBuffer(ByteBuffer byteBuffer) {
        TestWideEntity entity = new TestWideEntity();
        SerializationUtils.deserialize(BytesArraySerializer.get().fromByteBuffer(byteBuffer), entity);

        return entity;
    }
}
