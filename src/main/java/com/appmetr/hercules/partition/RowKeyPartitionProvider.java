package com.appmetr.hercules.partition;

import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;

import java.util.List;

public abstract class RowKeyPartitionProvider<R, T> implements PartitionProvider<R, T> {

    public abstract String getPartition(R rowKey);

    @Override
    public abstract List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator);

    @Override public abstract List<String> getPartitionsForCreation();

    @Override public String getPartition(R rowKey, T topKey) {
        return getPartition(rowKey);
    }
}
