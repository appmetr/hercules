package com.appmetr.hercules.driver;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesMonitoringGroup;
import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.serializers.EnumSerializer;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.StopWatch;
import com.google.inject.Inject;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class ThriftDataDriver implements DataDriver {

    private Logger logger = LoggerFactory.getLogger(ThriftDataDriver.class);

    @Inject private Monitoring monitoring;
    @Inject private Hercules hercules;

    @Override public Cluster getOrCreateCluster(String keyspaceName, String host) {
        CassandraHostConfigurator configurator = new CassandraHostConfigurator(host);
        configurator.setMaxActive(100);

        return HFactory.getOrCreateCluster(keyspaceName, configurator);
    }

    @Override public void shutdownCluster(Cluster cluster) {
        HFactory.shutdownCluster(cluster);
        cluster.getConnectionManager().shutdown();
    }

    @Override public Keyspace getOrCreateKeypace(String keyspaceName, int replicationFactor, Cluster cluster) {
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);
        List<ColumnFamilyDefinition> cfDefs = new LinkedList<ColumnFamilyDefinition>();
        // If keyspace does not exist create it without CFs.
        if (keyspaceDef == null) {
            keyspaceDef = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, cfDefs);
            cluster.addKeyspace(keyspaceDef);
        }

        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);

        ConfigurableConsistencyLevel consistencyLevel = new ConfigurableConsistencyLevel();
        consistencyLevel.setDefaultReadConsistencyLevel(HConsistencyLevel.QUORUM);
        consistencyLevel.setDefaultWriteConsistencyLevel(HConsistencyLevel.QUORUM);
        keyspace.setConsistencyLevelPolicy(consistencyLevel);

        return keyspace;
    }

    //synchronized only protects from concurrent changes within one server, still there's possibility of schema updates from different machines
    //todo synchronized can be removed after upgrade to cassandra 1.1 (issue: CASSANDRA-1391)
    @Override
    public synchronized boolean checkAndCreateColumnFamily(Cluster cluster, String keyspaceName, String cfName, ComparatorType comparator, boolean awaitAgreement) {
        if (!hercules.isSchemaModificationEnabled()) return false;

        if (cluster.describeKeyspace(keyspaceName) != null) {
            for (ColumnFamilyDefinition cfDef : cluster.describeKeyspace(keyspaceName).getCfDefs()) {
                if (cfDef.getName().equals(cfName)) return false;
            }
        }

        logger.info("Create column family "+cfName);
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, cfName, comparator);
        cluster.addColumnFamily(cfDef, awaitAgreement);

        return true;
    }

    @Override
    public boolean checkAndCreateColumnFamily(Cluster cluster, String keyspaceName, String cfName, ComparatorType comparator) {
        return checkAndCreateColumnFamily(cluster, keyspaceName, cfName, comparator, true);
    }

    //synchronized only protects from concurrent changes within one server, still there's possibility of schema updates from different machines
    //todo synchronized can be removed after upgrade to cassandra 1.1 (issue: CASSANDRA-1391)
    @Override
    public synchronized boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName, boolean awaitAgreement) {
        if (!hercules.isSchemaModificationEnabled()) return false;

        cluster.dropColumnFamily(keyspaceName, cfName, awaitAgreement);

        return true;
    }

    @Override
    public boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName) {
        return deleteColumnFamily(cluster, keyspaceName, cfName, true);
    }

    @Override public <T> Serializer<T> getSerializerForObject(Object obj) {
        if (obj != null && obj.getClass().isEnum()) {
            return new EnumSerializer(obj.getClass());
        }
        return SerializerTypeInferer.getSerializer(obj);
    }

    @Override public <T> Serializer<T> getSerializerForClass(Class clazz) {
        if (clazz.isEnum()) {
            return new EnumSerializer(clazz);
        }
        return SerializerTypeInferer.getSerializer(clazz);
    }

    @Override
    public <R, T> int getRowCount(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R from, R to, Integer count) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get row count " + columnFamily);

        try {
            RangeSlicesQuery<R, T, ByteBuffer> rangeSlicesQuery = HFactory.createRangeSlicesQuery(
                    keyspace,
                    rowSerializer.getRowKeySerializer(),
                    rowSerializer.getTopKeySerializer(),
                    ByteBufferSerializer.get());
            rangeSlicesQuery.setColumnFamily(columnFamily);

            rangeSlicesQuery.setKeys(from, to);
            rangeSlicesQuery.setRange(null, null, false, 1);
            rangeSlicesQuery.setRowCount(getBoundedRowCount(count));
            rangeSlicesQuery.setReturnKeysOnly();

            return rangeSlicesQuery.execute().get().getCount();
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> int getTopCount(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R rowKey, T from, T to, Integer count) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get top count " + columnFamily);

        try {
            CountQuery<R, T> query = HFactory.createCountQuery(keyspace, rowSerializer.getRowKeySerializer(), rowSerializer.getTopKeySerializer());
            query.setColumnFamily(columnFamily);

            query.setKey(rowKey);
            query.setRange(from, to, getBoundedTopCount(count));

            return query.execute().get();
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> HerculesQueryResult<T> getRow(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R rowKey) {
        HerculesMultiQueryResult<R, T> queryResult = getSlice(keyspace, columnFamily, rowSerializer, Arrays.<R>asList(rowKey), new SliceDataSpecificator<T>(null, null, false, null));

        return queryResult.hasResult && queryResult.getEntries().containsKey(rowKey) ? new HerculesQueryResult<T>(queryResult.getEntries().get(rowKey)) : new HerculesQueryResult<T>();
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getRows(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, Iterable<R> rowKeys) {
        return getSlice(keyspace, columnFamily, rowSerializer, rowKeys, new SliceDataSpecificator<T>(null, null, false, null));
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getAllRows(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer) {
        return getRangeSlice(keyspace, columnFamily, rowSerializer, null, null, null, new SliceDataSpecificator<T>(null, null, false, null));
    }

    @Override
    public <R, T> HerculesQueryResult<T> getSlice(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        HerculesMultiQueryResult<R, T> queryResult = getSlice(keyspace, columnFamily, rowSerializer, Arrays.asList(rowKey), sliceDataSpecificator);

        if (queryResult.hasResult() && queryResult.containsKey(rowKey)) {
            return new HerculesQueryResult<T>(queryResult.getEntries().get(rowKey));
        }

        return new HerculesQueryResult<T>();
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getSlice(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer,
                                                          Iterable<R> rowKeys, SliceDataSpecificator<T> sliceDataSpecificator) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get slice " + columnFamily);

        try {
            MultigetSliceQuery<R, T, ByteBuffer> multigetSliceQuery = HFactory.createMultigetSliceQuery(
                    keyspace,
                    rowSerializer.getRowKeySerializer(),
                    rowSerializer.getTopKeySerializer(),
                    ByteBufferSerializer.get());

            multigetSliceQuery.setColumnFamily(columnFamily);
            multigetSliceQuery.setKeys(rowKeys);

            sliceDataSpecificator.fillMultigetSliceQuery(multigetSliceQuery);

            return buildQueryResult(rowSerializer, multigetSliceQuery.execute().get());
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getRangeSlice(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer,
                                                               R rowFrom, R rowTo, Integer rowCount, SliceDataSpecificator<T> sliceDataSpecificator) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get range slice " + columnFamily);

        try {
            RangeSlicesQuery<R, T, ByteBuffer> rangeSlicesQuery = HFactory.createRangeSlicesQuery(
                    keyspace,
                    rowSerializer.getRowKeySerializer(),
                    rowSerializer.getTopKeySerializer(),
                    ByteBufferSerializer.get());
            rangeSlicesQuery.setColumnFamily(columnFamily);
            rangeSlicesQuery.setRowCount(getBoundedRowCount(rowCount));
            rangeSlicesQuery.setKeys(rowFrom, rowTo);

            sliceDataSpecificator.fillRangeSliceQuery(rangeSlicesQuery);

            return buildQueryResult(rowSerializer, rangeSlicesQuery.execute().get());
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> List<R> getKeyRange(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer,
                                      R from, R to, Integer count) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get key range " + columnFamily);

        try {
            RangeSlicesQuery<R, T, ByteBuffer> query = HFactory.createRangeSlicesQuery(keyspace,
                    rowSerializer.getRowKeySerializer(),
                    rowSerializer.getTopKeySerializer(),
                    ByteBufferSerializer.get());

            query.setColumnFamily(columnFamily);
            query.setKeys(from, to);
            query.setRowCount(getBoundedRowCount(count));
            query.setReturnKeysOnly();

            QueryResult<OrderedRows<R, T, ByteBuffer>> result = query.execute();
            OrderedRows<R, T, ByteBuffer> rows = (result != null) ? result.get() : null;

            List<R> keys = new ArrayList<R>(rows == null ? 0 : rows.getCount());

            if (rows != null) {
                for (Row<R, T, ByteBuffer> row : rows) {
                    keys.add(row.getKey());
                }
            }

            return keys;
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> void insert(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R rowKey, T topKey, Object value) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Insert " + columnFamily);

        try {
            Serializer<R> rowKeySerializer = rowSerializer.getRowKeySerializer();

            ByteBuffer serializedRowKey = rowKeySerializer.toByteBuffer(rowKey);
            if (serializedRowKey == null) {
                return;
            }

            Mutator<ByteBuffer> mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
            if (value == null) {
                mutator.delete(serializedRowKey, columnFamily, topKey, rowSerializer.getTopKeySerializer());
            } else {
                mutator.insert(serializedRowKey, columnFamily, HFactory.createColumn(topKey, value, rowSerializer.getTopKeySerializer(), rowSerializer.getValueSerializer(topKey)));
            }
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> void insert(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values) {
        Map<R, Map<T, Object>> valuesToInsert = new HashMap<R, Map<T, Object>>();
        valuesToInsert.put(rowKey, values);

        insert(keyspace, columnFamily, rowSerializer, valuesToInsert);
    }

    @Override
    public <R, T> void insert(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, Map<R, Map<T, Object>> values) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Insert " + columnFamily);

        try {
            Serializer<R> rowKeySerializer = rowSerializer.getRowKeySerializer();

            Mutator<ByteBuffer> mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());

            for (R rowKey : values.keySet()) {
                ByteBuffer serializedRowKey = rowKeySerializer.toByteBuffer(rowKey);
                if (serializedRowKey == null) {
                    continue;
                }

                for (Map.Entry<T, Object> entry : values.get(rowKey).entrySet()) {
                    T topKey = entry.getKey();
                    Object value = entry.getValue();

                    Serializer serializer = rowSerializer.hasValueSerializer(topKey) ? rowSerializer.getValueSerializer(topKey) : getSerializerForObject(value);

                    if (value == null) {
                        mutator.addDeletion(serializedRowKey, columnFamily, topKey, rowSerializer.getTopKeySerializer());
                    } else {
                        HColumn column = HFactory.createColumn(topKey, value, rowSerializer.getTopKeySerializer(), serializer);
                        mutator.addInsertion(serializedRowKey, columnFamily, column);
                    }
                }
            }

            mutator.execute();
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> void delete(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R rowKey) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Delete " + columnFamily);

        try {
            Mutator<R> mutator = HFactory.createMutator(keyspace, rowSerializer.getRowKeySerializer());
            mutator.delete(rowKey, columnFamily, null, rowSerializer.getTopKeySerializer());
        } finally {
            monitor.stop();
        }
    }

    @Override
    public <R, T> void delete(Keyspace keyspace, String columnFamily, RowSerializer<R, T> rowSerializer, R rowKey, Iterable<T> topKeys) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Delete " + columnFamily);

        try {
            Mutator<ByteBuffer> mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
            ByteBuffer serializedRowKey = rowSerializer.getRowKeySerializer().toByteBuffer(rowKey);
            if (serializedRowKey == null) {
                return;
            }

            for (T topKey : topKeys) {
                mutator.addDeletion(serializedRowKey, columnFamily, topKey, rowSerializer.getTopKeySerializer());
            }
            mutator.execute();
        } finally {
            monitor.stop();
        }
    }

    private <R, T> HerculesMultiQueryResult<R, T> buildQueryResult(RowSerializer<R, T> rowSerializer, Rows<R, T, ByteBuffer> rows) {
        if (rows.getCount() == 0) {
            return new HerculesMultiQueryResult<R, T>();
        }

        LinkedHashMap<R, LinkedHashMap<T, Object>> result = new LinkedHashMap<R, LinkedHashMap<T, Object>>();

        R lastKey = null;
        for (Row<R, T, ByteBuffer> row : rows) {
            lastKey = row.getKey();

            List<HColumn<T, ByteBuffer>> columns = row.getColumnSlice().getColumns();

            if (columns.size() > 0) {
                LinkedHashMap<T, Object> valueMap = new LinkedHashMap<T, Object>();

                for (HColumn<T, ByteBuffer> column : columns) {
                    if (!rowSerializer.hasValueSerializer(column.getName())) continue;

                    Serializer serializer = rowSerializer.getValueSerializer(column.getName());
                    valueMap.put(column.getName(), serializer.fromByteBuffer(column.getValue()));
                }

                result.put(row.getKey(), valueMap);
            }
        }

        return result.size() > 0 ? new HerculesMultiQueryResult<R, T>(result, lastKey) : new HerculesMultiQueryResult<R, T>(lastKey);
    }

    private int getBoundedRowCount(Integer rowCount) {
        return rowCount == null || rowCount > MAX_ROW_COUNT ? MAX_ROW_COUNT : rowCount;
    }

    private int getBoundedTopCount(Integer topCount) {
        return topCount == null || topCount > MAX_TOP_COUNT ? MAX_TOP_COUNT : topCount;
    }
}
