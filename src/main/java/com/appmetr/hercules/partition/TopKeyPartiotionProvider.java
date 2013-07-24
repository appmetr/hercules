package com.appmetr.hercules.partition;

import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;

import java.util.List;

public abstract class TopKeyPartiotionProvider<T> implements PartitionProvider<Object, T> {

    public abstract String getPartition(T topKey);

    public abstract List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(SliceDataSpecificator<T> sliceDataSpecificator);

    @Override public abstract List<String> getPartitionsForCreation();

    @Override public String getPartition(Object rowKey, T topKey) {
        return getPartition(topKey);
    }

    @Override
    public List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(Object rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        return getPartitionedQueries(sliceDataSpecificator);
    }
}
