package com.appmetr.hercules.serializers;

import com.appmetr.hercules.keys.ParentFK;
import me.prettyprint.cassandra.serializers.StringSerializer;

import java.nio.ByteBuffer;

public class ParentFKSerializer extends AbstractHerculesSerializer<ParentFK> {
    @Override public ByteBuffer toByteBuffer(ParentFK obj) {
        return StringSerializer.get().toByteBuffer(obj.parent);
    }

    @Override public ParentFK fromByteBuffer(ByteBuffer byteBuffer) {
        return new ParentFK(StringSerializer.get().fromByteBuffer(byteBuffer));
    }
}
