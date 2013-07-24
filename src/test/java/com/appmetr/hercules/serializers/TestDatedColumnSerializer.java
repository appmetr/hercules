package com.appmetr.hercules.serializers;

import com.appmetr.hercules.column.TestDatedColumn;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

import java.nio.ByteBuffer;

import static me.prettyprint.hector.api.ddl.ComparatorType.LONGTYPE;

public class TestDatedColumnSerializer extends AbstractHerculesSerializer<TestDatedColumn> {

    private static final TestDatedColumnSerializer instance = new TestDatedColumnSerializer();

    public static TestDatedColumnSerializer get() {
        return instance;
    }

    @Override public ByteBuffer toByteBuffer(TestDatedColumn obj) {
        if (obj == null) {
            return null;
        }
        Long date = obj.getDate();
        return LongSerializer.get().toByteBuffer(date);
    }

    @Override public TestDatedColumn fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        Long date = LongSerializer.get().fromByteBuffer(byteBuffer);
        return new TestDatedColumn(date);
    }

    @Override
    public ComparatorType getComparatorType() {
        return LONGTYPE;
    }
}
