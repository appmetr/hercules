package com.appmetr.hercules.batch.extractor;

import com.appmetr.hercules.batch.BatchIterator;
import com.appmetr.hercules.failover.FailoverConf;
import com.appmetr.hercules.failover.FailoverQuery;
import com.appmetr.hercules.failover.FailoverQueryProcessor;
import org.slf4j.Logger;

import java.util.List;

public class FailoverBatchIterator<E, K> implements BatchIterator<E, K> {
    private BatchIterator<E, K> extractor;
    private FailoverConf conf;
    private Logger logger;

    public FailoverBatchIterator(BatchIterator<E, K> extractor, FailoverConf conf, Logger logger) {
        this.extractor = extractor;
        this.conf = conf;
        this.logger = logger;
    }

    @Override public List<E> next() {
        return FailoverQueryProcessor.process(conf, logger, new FailoverQuery<List<E>>() {
            @Override public List<E> query() {
                return extractor.next();
            }
        });
    }

    @Override public boolean hasNext() {
        return FailoverQueryProcessor.process(conf, logger, new FailoverQuery<Boolean>() {
            @Override public Boolean query() {
                return extractor.hasNext();
            }
        });
    }
}
