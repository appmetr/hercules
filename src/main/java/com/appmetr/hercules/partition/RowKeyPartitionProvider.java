package com.appmetr.hercules.partition;

import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;

import java.util.Collections;
import java.util.List;

public abstract class RowKeyPartitionProvider<R, T> implements PartitionProvider<R, T> {

    public abstract String getPartition(R rowKey);

    public abstract List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator);

    public abstract List<String> getPartitionsForCreation();

    public String getPartition(R rowKey, T topKey) {
        return getPartition(rowKey);
    }

    public List<R> getPartitionedRowKeys(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        return Collections.singletonList(rowKey);
    }

    public R getPartitionedRowKey(R rowKey, T topKey) {
        return rowKey;
    }
}
