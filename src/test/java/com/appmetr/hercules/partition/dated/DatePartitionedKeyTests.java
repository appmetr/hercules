package com.appmetr.hercules.partition.dated;

import com.appmetr.hercules.column.TestDatedColumn;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DatePartitionedKeyTests {

    static class PartitionedKey implements DatePartitionedKey<PartitionedKey>{
        public PartitionedKey(PartitionedKey other) {
            this.part1 = other.part1;
            this.part2 = other.part2;
            this.partitionKey = other.partitionKey;
        }

        public PartitionedKey(String part1, String part2) {

            this.part1 = part1;
            this.part2 = part2;
        }

        private String part1;
        private String part2;
        private Long partitionKey;

        public PartitionedKey copyWithPartitionKey(long partitionKey) {
            final PartitionedKey copy = new PartitionedKey(this);
            copy.partitionKey = partitionKey;
            return copy;
        }

        @Override public String toString() {
            final StringBuilder sb = new StringBuilder("PartitionedKey{");
            sb.append("part1='").append(part1).append('\'');
            sb.append(", part2='").append(part2).append('\'');
            sb.append(", partitionKey=").append(partitionKey);
            sb.append('}');
            return sb.toString();
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartitionedKey that = (PartitionedKey) o;

            if (part1 != null ? !part1.equals(that.part1) : that.part1 != null) return false;
            if (part2 != null ? !part2.equals(that.part2) : that.part2 != null) return false;
            return partitionKey != null ? partitionKey.equals(that.partitionKey) : that.partitionKey == null;

        }

        @Override public int hashCode() {
            int result = part1 != null ? part1.hashCode() : 0;
            result = 31 * result + (part2 != null ? part2.hashCode() : 0);
            return result;
        }
    }

    @Test
    public void testKeyPartitioning() {
        final PartitionedKey key = new PartitionedKey("p1", "p2");

        final DatePartitionProvider<DatedColumn> partiotionProvider = new DatePartitionProvider<DatedColumn>(new DatePartsConfig(
                new DateTime(2013, DateTimeConstants.JANUARY, 1, 0, 0, DateTimeZone.UTC),
                DateTimeFieldType.monthOfYear(),
                3
        ));

        final TestDatedColumn low = new TestDatedColumn(new DateTime(2016, 1, 1, 0, 1).getMillis());
        final TestDatedColumn low1 = new TestDatedColumn(new DateTime(2016, 1, 1, 3, 0).getMillis());
        final TestDatedColumn high = new TestDatedColumn(new DateTime(2016, 1, 3, 0, 1).getMillis());

        final SliceDataSpecificator<DatedColumn> sliceDataSpecificator = new SliceDataSpecificator<DatedColumn>(low, high, false, null);

        final List<Object> keys = partiotionProvider.getPartitionedRowKeys(key, sliceDataSpecificator);
        for (Object o : keys) {
            assertTrue(o instanceof DatePartitionedKey);
            assertTrue(o instanceof PartitionedKey);
            final PartitionedKey datePartitionedKey = (PartitionedKey) o;

            assertEquals("p1",datePartitionedKey.part1);
            assertEquals("p2",datePartitionedKey.part2);

            System.out.println(datePartitionedKey);
        }

        System.out.println(partiotionProvider.getPartitionedRowKey(key,low1));
        assertEquals(DatePartitionProvider.ceilDateToPeriod(new DateTime(2016, 1, 1, 0, 0),DateTimeFieldType.monthOfYear(),1),
                DatePartitionProvider.ceilDateToPeriod(new DateTime(2016, 1, 1, 0, 0),DateTimeFieldType.monthOfYear(),1));

        assertEquals(DatePartitionProvider.ceilDateToPeriod(new DateTime(2013, 1, 1, 0, 1),DateTimeFieldType.dayOfMonth(),1),
                DatePartitionProvider.ceilDateToPeriod(new DateTime(2013, 1, 1, 3, 0),DateTimeFieldType.dayOfMonth(),1));

        assertEquals(partiotionProvider.getPartitionedRowKey(key,low),partiotionProvider.getPartitionedRowKey(key,low1));
    }
}
