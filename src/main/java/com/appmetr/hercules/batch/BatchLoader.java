package com.appmetr.hercules.batch;

import com.appmetr.hercules.dao.AbstractWideDAO;
import com.appmetr.hercules.failover.FailoverConf;
import com.appmetr.hercules.profile.DataOperationsProfile;
import org.slf4j.Logger;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class BatchLoader<E, R, T> {

    private boolean finished;
    private T start;
    private T end;
    private boolean nextBatchCalled;

    private Logger logger;
    private FailoverConf conf;
    private DataOperationsProfile dataLoadProfile;
    private R rowKey;
    private boolean orderDesc;
    private int stdBatchSize;
    private AbstractWideDAO<E, R, T> wideDAO;

    public BatchLoader(Logger logger, R rowKey, int stdBatchSize, AbstractWideDAO<E, R, T> wideDAO) {
        this(logger, FailoverConf.NO, rowKey, stdBatchSize, wideDAO);
    }

    public BatchLoader(Logger logger, FailoverConf conf, R rowKey, int stdBatchSize, AbstractWideDAO<E, R, T> wideDAO) {
        this(logger, conf, rowKey, null, null, false, stdBatchSize, wideDAO);
    }

    public BatchLoader(Logger logger, FailoverConf conf, R rowKey, T start, T end, boolean orderDesc, int stdBatchSize, AbstractWideDAO<E, R, T> wideDAO) {
        this(logger, conf, rowKey, start, end, orderDesc, stdBatchSize, null, wideDAO);
    }

    public BatchLoader(Logger logger, FailoverConf conf, R rowKey, T start, T end, boolean orderDesc, int stdBatchSize, DataOperationsProfile dataLoadProfile, AbstractWideDAO<E, R, T> wideDAO) {
        this.logger = logger;
        this.conf = conf;
        this.rowKey = rowKey;
        this.start = start;
        this.end = end;
        this.orderDesc = orderDesc;
        this.stdBatchSize = stdBatchSize;
        this.dataLoadProfile = dataLoadProfile;
        this.wideDAO = wideDAO;
    }

    public List<E> nextBatch() {
        return nextBatch(stdBatchSize);
    }

    public List<E> nextBatch(int batchSize) {

        if (finished) {
            throw new IllegalStateException("Iteration finished");
        }

        List<E> fetchedBatchList = wideDAO.get(rowKey, start, end, orderDesc, batchSize);

        List<E> truncatedBatchList;
        if (!nextBatchCalled) {
            truncatedBatchList = new ArrayList<E>(fetchedBatchList);
            nextBatchCalled = true;
        } else {
            if (fetchedBatchList.size() < 1) {
                logger.warn(MessageFormat.format("Empty list for wide service get. Row key: [{0}], start: [{1}], end: [{2}]. batchSize:[{3}]", rowKey, start, end, batchSize));
                truncatedBatchList = new ArrayList<E>();
            } else {
                truncatedBatchList = new ArrayList<E>(fetchedBatchList.subList(1, fetchedBatchList.size()));
            }
        }
        if (truncatedBatchList.size() != 0) {
            T lastBatch = wideDAO.getTopKey(truncatedBatchList.get(truncatedBatchList.size() - 1));
            if (orderDesc) {
                end = lastBatch;
            } else {
                start = lastBatch;
            }
        } else {
            finished = true;
        }

        return truncatedBatchList;
    }
}
