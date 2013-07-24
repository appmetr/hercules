package com.appmetr.hercules.batch;

import java.util.List;

public interface BatchProcessor<E> {

    void processBatch(List<E> batch);
}
