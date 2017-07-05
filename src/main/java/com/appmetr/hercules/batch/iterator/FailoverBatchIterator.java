package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.batch.BatchIterator;
import com.appmetr.hercules.failover.FailoverConf;
import com.appmetr.hercules.failover.FailoverQueryProcessor;
import com.appmetr.hercules.profile.DataOperationsProfile;
import org.slf4j.Logger;

import java.util.List;

public class FailoverBatchIterator<E, K> implements BatchIterator<E, K> {
    private BatchIterator<E, K> iterator;
    private FailoverConf conf;
    private Logger logger;

    public FailoverBatchIterator(BatchIterator<E, K> iterator, FailoverConf conf, Logger logger) {
        this.iterator = iterator;
        this.conf = conf;
        this.logger = logger;
    }

    @Override public List<E> next(final DataOperationsProfile dataOperationsProfile) {
        return FailoverQueryProcessor.process(conf, logger, () -> iterator.next(dataOperationsProfile));
    }

    @Override public boolean hasNext() {
        return FailoverQueryProcessor.process(conf, logger, () -> iterator.hasNext());
    }
}
