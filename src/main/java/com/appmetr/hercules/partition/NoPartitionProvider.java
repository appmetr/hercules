package com.appmetr.hercules.partition;

import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;

import java.util.ArrayList;
import java.util.List;

public class NoPartitionProvider<R, T> implements PartitionProvider<R, T> {
    @Override
    public List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        List<SliceDataSpecificatorByCF<T>> result = new ArrayList<SliceDataSpecificatorByCF<T>>();
        result.add(new SliceDataSpecificatorByCF<T>("", sliceDataSpecificator));
        return result;
    }

    @Override public String getPartition(R rowKey, T topKey) {
        return "";
    }

    @Override public List<String> getPartitionsForCreation() {
        List<String> result = new ArrayList<String>();
        result.add("");
        return result;
    }
}
