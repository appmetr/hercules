package com.appmetr.hercules.batch;

import com.appmetr.hercules.profile.DataOperationsProfile;

import java.util.List;

public interface BatchIterator<E, K> {
    List<E> next(DataOperationsProfile dataOperationsProfile);

    boolean hasNext();
}
