package com.appmetr.hercules.batch.iterator;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.batch.BatchIterator;
import com.appmetr.hercules.failover.FailoverConf;
import org.slf4j.Logger;

public abstract class AbstractBatchIterator<E, K> implements BatchIterator<E, K> {
    protected K lowEnd = null;
    protected K highEnd = null;
    protected boolean reverse = false;
    protected int batchSize = Hercules.DEFAULT_BATCH_SIZE;

    protected boolean hasNext = true;

    public AbstractBatchIterator() {
    }

    public AbstractBatchIterator(K lowEnd, K highEnd) {
        this.lowEnd = lowEnd;
        this.highEnd = highEnd;
    }

    public AbstractBatchIterator(int batchSize) {
        this.batchSize = batchSize;
    }

    public AbstractBatchIterator(K lowEnd, K highEnd, int batchSize) {
        this.lowEnd = lowEnd;
        this.highEnd = highEnd;
        this.batchSize = batchSize;
    }

    public AbstractBatchIterator(K lowEnd, K highEnd, boolean reverse, int batchSize) {
        this.lowEnd = lowEnd;
        this.highEnd = highEnd;
        this.reverse = reverse;
        this.batchSize = batchSize;
    }

    @Override public boolean hasNext() {
        return hasNext;
    }

    public FailoverBatchIterator<E, K> failover(FailoverConf conf, Logger logger) {
        return new FailoverBatchIterator<E, K>(this, conf, logger);
    }
}
