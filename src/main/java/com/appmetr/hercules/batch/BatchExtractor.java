package com.appmetr.hercules.batch;

import java.util.List;

public interface BatchExtractor<E, K> {
    List<E> getBatch(K from, K to, int batchSize);

    K getKey(E item);
}
