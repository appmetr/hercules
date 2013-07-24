package com.appmetr.hercules.batch.extractor;

import com.appmetr.hercules.batch.BatchExtractor;
import com.appmetr.hercules.failover.FailoverConf;
import org.slf4j.Logger;

public abstract class AbstractBatchExtractor<E, K> implements BatchExtractor<E, K> {
    public FailoverBatchExtractor<E, K> failover(FailoverConf conf, Logger logger) {
        return new FailoverBatchExtractor<E, K>(this, conf, logger);
    }
}
