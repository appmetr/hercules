package com.appmetr.hercules.batch;

import java.util.List;

public interface BatchIterator<E, K> {
    List<E> next();

    boolean hasNext();
}
