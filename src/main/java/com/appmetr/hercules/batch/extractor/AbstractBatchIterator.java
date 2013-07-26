package com.appmetr.hercules.batch.extractor;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.batch.BatchIterator;
import com.appmetr.hercules.failover.FailoverConf;
import org.slf4j.Logger;

public abstract class AbstractBatchIterator<E, K> implements BatchIterator<E, K> {
    protected K from;
    protected K to;
    protected int batchSize = Hercules.DEFAULT_BATCH_SIZE;

    protected AbstractBatchIterator(K from, K to) {
        this.from = from;
        this.to = to;
    }

    protected AbstractBatchIterator(K from, K to, int batchSize) {
        this.from = from;
        this.to = to;
        this.batchSize = batchSize;
    }

    public FailoverBatchIterator<E, K> failover(FailoverConf conf, Logger logger) {
        return new FailoverBatchIterator<E, K>(this, conf, logger);
    }
}
