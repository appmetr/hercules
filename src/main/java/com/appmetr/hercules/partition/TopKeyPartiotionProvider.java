package com.appmetr.hercules.partition;

import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;

import java.util.Collections;
import java.util.List;

public abstract class TopKeyPartiotionProvider<T> implements PartitionProvider<Object, T> {

    public abstract String getPartition(T topKey);

    public abstract List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(SliceDataSpecificator<T> sliceDataSpecificator);

    public abstract List<String> getPartitionsForCreation();

    public String getPartition(Object rowKey, T topKey) {
        return getPartition(topKey);
    }

    public List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(Object rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        return getPartitionedQueries(sliceDataSpecificator);
    }

    public List<Object> getPartitionedRowKeys(Object rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        return Collections.singletonList(rowKey);
    }

    public Object getPartitionedRowKey(Object rowKey, T topKey) {
        return rowKey;
    }
}
