package com.appmetr.hercules.driver;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesMonitoringGroup;
import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.serializers.EnumSerializer;
import com.appmetr.hercules.serializers.InformerSerializer;
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

    @Override public Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections) {
        CassandraHostConfigurator configurator = new CassandraHostConfigurator(host);
        configurator.setMaxActive(maxActiveConnections);

        return HFactory.getOrCreateCluster(clusterName, configurator);
    }

    @Override public void shutdownCluster(Cluster cluster) {
        HFactory.shutdownCluster(cluster);
    }

    @Override public Keyspace getOrCreateKeyspace(String keyspaceName, int replicationFactor, Cluster cluster) {
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

        logger.info("Create column family " + cfName);
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
    public <R, T> int getRowCount(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R from, R to, Integer count) {

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

        QueryResult<OrderedRows<R, T, ByteBuffer>> result;
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get row count " + columnFamily);

        try {
            result = rangeSlicesQuery.execute();
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }

        return result.get().getCount();
    }

    @Override
    public <R, T> int getTopCount(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T from, T to, Integer count) {
        CountQuery<R, T> query = HFactory.createCountQuery(keyspace, rowSerializer.getRowKeySerializer(), rowSerializer.getTopKeySerializer());
        query.setColumnFamily(columnFamily);

        query.setKey(rowKey);
        query.setRange(from, to, getBoundedTopCount(count));

        QueryResult<Integer> result;
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get top count " + columnFamily);

        try {
            result = query.execute();
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }

        return result.get();
    }

    @Override
    public <R, T> HerculesQueryResult<T> getRow(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey) {
        HerculesMultiQueryResult<R, T> queryResult = getSlice(keyspace, columnFamily, dataOperationsProfile, rowSerializer, Arrays.<R>asList(rowKey), new SliceDataSpecificator<T>(null, null, false, null));

        return queryResult.hasResult && queryResult.getEntries().containsKey(rowKey) ? new HerculesQueryResult<T>(queryResult.getEntries().get(rowKey)) : new HerculesQueryResult<T>();
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getRows(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Iterable<R> rowKeys) {
        return getSlice(keyspace, columnFamily, dataOperationsProfile, rowSerializer, rowKeys, new SliceDataSpecificator<T>(null, null, false, null));
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getAllRows(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer) {
        return getRangeSlice(keyspace, columnFamily, dataOperationsProfile, rowSerializer, null, null, null, new SliceDataSpecificator<T>(null, null, false, null));
    }

    @Override
    public <R, T> HerculesQueryResult<T> getSlice(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        HerculesMultiQueryResult<R, T> queryResult = getSlice(keyspace, columnFamily, dataOperationsProfile, rowSerializer, Arrays.asList(rowKey), sliceDataSpecificator);

        if (queryResult.hasResult() && queryResult.containsKey(rowKey)) {
            return new HerculesQueryResult<T>(queryResult.getEntries().get(rowKey));
        }

        return new HerculesQueryResult<T>();
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getSlice(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                                          Iterable<R> rowKeys, SliceDataSpecificator<T> sliceDataSpecificator) {
        MultigetSliceQuery<R, T, ByteBuffer> multigetSliceQuery = HFactory.createMultigetSliceQuery(
                keyspace,
                rowSerializer.getRowKeySerializer(),
                rowSerializer.getTopKeySerializer(),
                ByteBufferSerializer.get());

        multigetSliceQuery.setColumnFamily(columnFamily);
        multigetSliceQuery.setKeys(rowKeys);

        sliceDataSpecificator.fillMultigetSliceQuery(multigetSliceQuery);

        QueryResult<Rows<R, T, ByteBuffer>> result;
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get slice " + columnFamily);

        try {
            result = multigetSliceQuery.execute();
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }

        return buildQueryResult(dataOperationsProfile, rowSerializer, result.get());
    }

    @Override
    public <R, T> HerculesMultiQueryResult<R, T> getRangeSlice(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                                               R rowFrom, R rowTo, Integer rowCount, SliceDataSpecificator<T> sliceDataSpecificator) {

        RangeSlicesQuery<R, T, ByteBuffer> rangeSlicesQuery = HFactory.createRangeSlicesQuery(
                keyspace,
                rowSerializer.getRowKeySerializer(),
                rowSerializer.getTopKeySerializer(),
                ByteBufferSerializer.get());
        rangeSlicesQuery.setColumnFamily(columnFamily);
        rangeSlicesQuery.setRowCount(getBoundedRowCount(rowCount));
        rangeSlicesQuery.setKeys(rowFrom, rowTo);

        sliceDataSpecificator.fillRangeSliceQuery(rangeSlicesQuery);

        QueryResult<OrderedRows<R, T, ByteBuffer>> result;
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get range slice " + columnFamily);

        try {
            result = rangeSlicesQuery.execute();
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }

        return buildQueryResult(dataOperationsProfile, rowSerializer, result.get());
    }

    @Override
    public <R, T> List<R> getKeyRange(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                      R from, R to, Integer count) {
        RangeSlicesQuery<R, T, ByteBuffer> query = HFactory.createRangeSlicesQuery(keyspace,
                rowSerializer.getRowKeySerializer(),
                rowSerializer.getTopKeySerializer(),
                ByteBufferSerializer.get());

        query.setColumnFamily(columnFamily);
        query.setKeys(from, to);
        query.setRowCount(getBoundedRowCount(count));
        query.setReturnKeysOnly();

        QueryResult<OrderedRows<R, T, ByteBuffer>> result;
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get key range " + columnFamily);

        try {
            result = query.execute();
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }
        OrderedRows<R, T, ByteBuffer> rows = (result != null) ? result.get() : null;

        List<R> keys = new ArrayList<R>(rows == null ? 0 : rows.getCount());

        if (rows != null) {
            for (Row<R, T, ByteBuffer> row : rows) {
                keys.add(row.getKey());
            }
        }

        return keys;
    }

    @Override
    public <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T topKey, Object value, int ttl) {
        Serializer<R> rowKeySerializer = rowSerializer.getRowKeySerializer();

        ByteBuffer serializedRowKey = rowKeySerializer.toByteBuffer(rowKey);
        if (serializedRowKey == null) {
            return;
        }

        Mutator<ByteBuffer> mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());

        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Insert " + columnFamily);

        int serializedDataSize = 0;
        try {
            if (value == null) {
                mutator.delete(serializedRowKey, columnFamily, topKey, rowSerializer.getTopKeySerializer());
            } else {
                Serializer serializer = rowSerializer.hasValueSerializer(topKey) ? rowSerializer.getValueSerializer(topKey) : getSerializerForObject(value);
                ByteBuffer serializedValue = serializer.toByteBuffer(value);
                serializedDataSize += serializedValue.remaining();

                HColumn<T, ByteBuffer> column;
                if (ttl > 0) {
                    column = HFactory.createColumn(topKey, serializedValue, ttl, rowSerializer.getTopKeySerializer(), ByteBufferSerializer.get());
                } else {
                    column = HFactory.createColumn(topKey, serializedValue, rowSerializer.getTopKeySerializer(), ByteBufferSerializer.get());
                }
                mutator.insert(serializedRowKey, columnFamily, column);
            }
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.bytes += serializedDataSize;
                dataOperationsProfile.dbQueries++;
            }
        }
    }

    @Override
    public <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, final Map<T, Integer> ttls) {
        Map<R, Map<T, Object>> valuesToInsert = new HashMap<R, Map<T, Object>>();
        valuesToInsert.put(rowKey, values);

        insert(keyspace, columnFamily, dataOperationsProfile, rowSerializer, valuesToInsert, new TTLProvider<R, T>() {
            @Override public int get(R row, T top) {
                if (ttls == null) {
                    return DataDriver.EMPTY_TTL;
                }
                Integer ttl = ttls.get(top);
                if (ttl == null) {
                    return DataDriver.EMPTY_TTL;
                }
                return ttl;
            }
        });
    }

    @Override
    public <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, final int ttl) {
        Map<R, Map<T, Object>> valuesToInsert = new HashMap<R, Map<T, Object>>();
        valuesToInsert.put(rowKey, values);

        insert(keyspace, columnFamily, dataOperationsProfile, rowSerializer, valuesToInsert, new TTLProvider<R, T>() {
            @Override public int get(R row, T top) {
                return ttl;
            }
        });
    }

    @Override
    public <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Map<T, Object>> values, final Map<R, Map<T, Integer>> ttls) {
        insert(keyspace, columnFamily, dataOperationsProfile, rowSerializer, values, new TTLProvider<R, T>() {
            @Override public int get(R row, T top) {
                if (ttls == null) {
                    return DataDriver.EMPTY_TTL;
                }
                Map<T, Integer> rowTtl = ttls.get(row);
                if (rowTtl == null) {
                    return DataDriver.EMPTY_TTL;
                }
                Integer ttl = rowTtl.get(top);
                if (ttl == null) {
                    return DataDriver.EMPTY_TTL;
                }
                return ttl;
            }
        });
    }

    private <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Map<T, Object>> values, TTLProvider<R, T> ttlProvider) {
        Serializer<R> rowKeySerializer = rowSerializer.getRowKeySerializer();

        Mutator<ByteBuffer> mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());

        int serializedDataSize = 0;
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
                    ByteBuffer serializedValue = serializer.toByteBuffer(value);
                    serializedDataSize += serializedValue.remaining();

                    int ttl = DataDriver.EMPTY_TTL;
                    if (ttlProvider != null) {
                        ttl = ttlProvider.get(rowKey, topKey);
                    }
                    HColumn column;
                    if (ttl > 0) {
                        column = HFactory.createColumn(topKey, serializedValue, ttl, rowSerializer.getTopKeySerializer(), ByteBufferSerializer.get());
                    } else {
                        column = HFactory.createColumn(topKey, serializedValue, rowSerializer.getTopKeySerializer(), ByteBufferSerializer.get());
                    }
                    mutator.addInsertion(serializedRowKey, columnFamily, column);
                }
            }
        }

        executeMutator(columnFamily, dataOperationsProfile, mutator, serializedDataSize);
    }

    private void executeMutator(String columnFamily, DataOperationsProfile dataOperationsProfile, Mutator<ByteBuffer> mutator, int serializedDataSize) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Insert " + columnFamily);
        try {
            mutator.execute();
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.bytes += serializedDataSize;
                dataOperationsProfile.dbQueries++;
            }
        }
    }

    @Override
    public <R, T> void delete(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey) {
        Mutator<R> mutator = HFactory.createMutator(keyspace, rowSerializer.getRowKeySerializer());

        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Delete " + columnFamily);
        try {
            mutator.delete(rowKey, columnFamily, null, rowSerializer.getTopKeySerializer());
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }
    }

    @Override
    public <R, T> void delete(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Iterable<T> topKeys) {
        Mutator<ByteBuffer> mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
        ByteBuffer serializedRowKey = rowSerializer.getRowKeySerializer().toByteBuffer(rowKey);
        if (serializedRowKey == null) {
            return;
        }

        for (T topKey : topKeys) {
            mutator.addDeletion(serializedRowKey, columnFamily, topKey, rowSerializer.getTopKeySerializer());
        }
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Delete " + columnFamily);

        try {
            mutator.execute();
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }
    }

    private <R, T> HerculesMultiQueryResult<R, T> buildQueryResult(DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Rows<R, T, ByteBuffer> rows) {
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

                    Serializer serializer = new InformerSerializer(rowSerializer.getValueSerializer(column.getName()), dataOperationsProfile);
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

    private static interface TTLProvider<R, T> {
        int get(R row, T top);
    }
}
