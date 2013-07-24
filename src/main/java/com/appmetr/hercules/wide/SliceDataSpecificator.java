package com.appmetr.hercules.wide;

import com.appmetr.hercules.driver.DataDriver;
import me.prettyprint.cassandra.model.thrift.ThriftMultigetSliceQuery;
import me.prettyprint.cassandra.model.thrift.ThriftRangeSlicesQuery;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class SliceDataSpecificator<N> {

    public enum SliceDataSpecificatorType {RANGE, COLUMNS}

    private SliceDataSpecificatorType type;

    private N[] columnsArray;
    private Collection<N> columnsCollection;

    private N start;
    private N end;
    private boolean orderDesc;
    private int limit;

    public SliceDataSpecificator(N... columns) {
        type = SliceDataSpecificatorType.COLUMNS;

        this.columnsArray = columns;
    }

    public SliceDataSpecificator(Collection<N> columns) {
        type = SliceDataSpecificatorType.COLUMNS;

        this.columnsCollection = columns;
    }

    public SliceDataSpecificator(N start, N end, boolean orderDesc, Integer limit) {
        type = SliceDataSpecificatorType.RANGE;

        this.start = start;
        this.end = end;
        this.orderDesc = orderDesc;
        this.limit = limit == null || limit > DataDriver.MAX_TOP_COUNT ? DataDriver.MAX_TOP_COUNT : limit;
    }

    public <K, V> void fillRangeSliceQuery(RangeSlicesQuery<K, N, V> sliceQuery) {
        if (type == SliceDataSpecificatorType.COLUMNS) {
            if (columnsArray != null) {
                sliceQuery.setColumnNames(columnsArray);
            }  else {
                ((ThriftRangeSlicesQuery) sliceQuery).setColumnNames(columnsCollection);
            }
        } else if (type == SliceDataSpecificatorType.RANGE) {
            if (orderDesc) { // Cassandra's WTF requirement
                sliceQuery.setRange(end, start, orderDesc, limit);
            } else {
                sliceQuery.setRange(start, end, orderDesc, limit);
            }

        } else {
            throw new IllegalStateException("Invalid type: " + type);
        }
    }

    public <K, V> void fillMultigetSliceQuery(MultigetSliceQuery<K, N, V> sliceQuery) {
        if (type == SliceDataSpecificatorType.COLUMNS) {
            if (columnsArray != null) {
                sliceQuery.setColumnNames(columnsArray);
            }  else {
                ((ThriftMultigetSliceQuery) sliceQuery).setColumnNames(columnsCollection);
            }
        } else if (type == SliceDataSpecificatorType.RANGE) {
            if (orderDesc) { // Cassandra's WTF requirement
                sliceQuery.setRange(end, start, orderDesc, limit);
            } else {
                sliceQuery.setRange(start, end, orderDesc, limit);
            }

        } else {
            throw new IllegalStateException("Invalid type: " + type);
        }
    }

    public <K, V> void fillSliceQuery(SliceQuery<K, N, V> sliceQuery) {
        if (type == SliceDataSpecificatorType.COLUMNS) {
            if (columnsArray != null) {
                sliceQuery.setColumnNames(columnsArray);
            }  else {
                ((ThriftSliceQuery) sliceQuery).setColumnNames(columnsCollection);
            }
        } else if (type == SliceDataSpecificatorType.RANGE) {
            if (orderDesc) { // Cassandra's WTF requirement
                sliceQuery.setRange(end, start, orderDesc, limit);
            } else {
                sliceQuery.setRange(start, end, orderDesc, limit);
            }

        } else {
            throw new IllegalStateException("Invalid type: " + type);
        }
    }

    public SliceDataSpecificatorType getType() {
        return type;
    }

    public N[] getColumnsArray() {
        return columnsArray;
    }

    public Collection<N> getColumnsCollection() {
        return columnsCollection;
    }

    public Iterator<N> getColumnsIterator() {
        return columnsArray != null ? Arrays.asList(columnsArray).iterator() : columnsCollection.iterator();
    }

    public N getStart() {
        return start;
    }

    public N getEnd() {
        return end;
    }

    public boolean isOrderDesc() {
        return orderDesc;
    }

    public int getLimit() {
        return limit;
    }
}
