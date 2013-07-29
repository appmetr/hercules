package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.batch.BatchIterator;
import com.appmetr.hercules.failover.FailoverConf;
import org.slf4j.Logger;

public abstract class AbstractBatchIterator<E, K> implements BatchIterator<E, K> {
    protected K from;
    protected K to;
    protected int batchSize = Hercules.DEFAULT_BATCH_SIZE;

    protected boolean hasNext = true;

    public AbstractBatchIterator() {
        this.from = null;
        this.to = null;
    }

    public AbstractBatchIterator(int batchSize) {
        this.from = null;
        this.to = null;

        this.batchSize = batchSize;
    }

    public AbstractBatchIterator(K from, K to) {
        this.from = from;
        this.to = to;
    }

    public AbstractBatchIterator(K from, K to, int batchSize) {
        this.from = from;
        this.to = to;
        this.batchSize = batchSize;
    }

    @Override public boolean hasNext() {
        return hasNext;
    }

    public FailoverBatchIterator<E, K> failover(FailoverConf conf, Logger logger) {
        return new FailoverBatchIterator<E, K>(this, conf, logger);
    }
}
