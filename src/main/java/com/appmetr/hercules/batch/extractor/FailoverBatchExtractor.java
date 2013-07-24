package com.appmetr.hercules.batch.extractor;

import com.appmetr.hercules.batch.BatchExtractor;
import com.appmetr.hercules.failover.FailoverConf;
import com.appmetr.hercules.failover.FailoverQuery;
import com.appmetr.hercules.failover.FailoverQueryProcessor;
import org.slf4j.Logger;

import java.util.List;

public class FailoverBatchExtractor<E, K> implements BatchExtractor<E, K> {
    private BatchExtractor<E, K> extractor;
    private FailoverConf conf;
    private Logger logger;

    public FailoverBatchExtractor(BatchExtractor<E, K> extractor, FailoverConf conf, Logger logger) {
        this.extractor = extractor;
        this.conf = conf;
        this.logger = logger;
    }

    @Override public List<E> getBatch(final K from, final K to, final int batchSize) {
        return FailoverQueryProcessor.process(conf, logger, new FailoverQuery<List<E>>() {
            @Override public List<E> query() {
                return extractor.getBatch(from, to, batchSize);
            }
        });
    }

    @Override public K getKey(final E item) {
        return FailoverQueryProcessor.process(conf, logger, new FailoverQuery<K>() {
            @Override public K query() {
                return extractor.getKey(item);
            }
        });
    }
}
