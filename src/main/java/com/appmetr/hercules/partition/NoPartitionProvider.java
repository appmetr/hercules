package com.appmetr.hercules.partition;

import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NoPartitionProvider<R, T> implements PartitionProvider<R, T> {
    public List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        List<SliceDataSpecificatorByCF<T>> result = new ArrayList<SliceDataSpecificatorByCF<T>>();
        result.add(new SliceDataSpecificatorByCF<T>("", sliceDataSpecificator));
        return result;
    }

    public String getPartition(R rowKey, T topKey) {
        return "";
    }

    public List<R> getPartitionedRowKeys(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        return Collections.singletonList(rowKey);
    }

    public List<String> getPartitionsForCreation() {
        List<String> result = new ArrayList<String>();
        result.add("");
        return result;
    }

    public R getPartitionedRowKey(R rowKey, T topKey) {
        return rowKey;
    }
}
