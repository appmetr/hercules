package com.appmetr.hercules.driver;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesMonitoringGroup;
import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.StopWatch;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.google.inject.Inject;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.dropTable;

public class CqlDataDriver implements DataDriver {

    //    @Inject private com.datastax.driver.core.Cluster cluster;
    @Inject private Monitoring monitoring;
    @Inject private Hercules hercules;
    private Logger logger = LoggerFactory.getLogger(CqlDataDriver.class);

    public Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections) {
        return getOrCreateCluster(clusterName, host, maxActiveConnections, 10_000);
    }

    public Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections, long maxConnectTimeMillis) {
        return Cluster.builder()
                .addContactPoint(host)
                .withQueryOptions(new QueryOptions().setFetchSize(1000))
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(maxActiveConnections))
                .withClusterName(clusterName)
                .build();
    }

    public void shutdownCluster(Cluster cluster) {
        cluster.close();
    }

    public String getOrCreateKeyspace(String keyspaceName, int replicationFactor, Cluster cluster) {
        Map<String, Object> replication = new HashMap<>();
        replication.put("class", "SimpleStrategy");
        replication.put("replication_factor", replicationFactor);  //todo: refactor
        Statement statement = new CreateKeyspace(keyspaceName)
                .ifNotExists()
                .with()
                .replication(replication)
                .setConsistencyLevel(QUORUM);

        execute(statement);
        return keyspaceName;
    }

    @Override public boolean checkAndCreateColumnFamily(Cluster cluster, String keyspaceName, String cfName) {
        if (hercules.isSchemaModificationDisabled()) return false;

        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(keyspaceName);

        if (shouldNotCreate(cfName, keyspace)) return false;

        logger.info("Create column family {} in keyspace {}", cfName, keyspaceName);

        MutableBoolean created = new MutableBoolean(false);
        execute(createTable(keyspaceName, cfName), rc -> {
            created.setTrue();
        });

        return created.booleanValue();
    }

    private boolean shouldNotCreate(String cfName, KeyspaceMetadata keyspace) {
        if (keyspace != null) {
            boolean found = keyspace.getTables()
                    .stream()
                    .map(AbstractTableMetadata::getName)
                    .anyMatch(s -> s.equals(cfName));

            if (found) {
                return true;
            }
        } else {
            return true;
        }
        return false;
    }

    public boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName, boolean awaitAgreement) {
        if (!hercules.isSchemaModificationEnabled()) return false;

        MutableBoolean deleted = new MutableBoolean(false);
        execute(dropTable(keyspaceName, quote(cfName)).ifExists(), rs -> {
            deleted.setTrue();
        });
        return deleted.booleanValue();
    }

    public boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName) {
        return deleteColumnFamily(cluster, keyspaceName, cfName, true);
    }

    @Override public <T> TypeCodec<T> getSerializerForObject(Object obj) {
        return getSerializerForClass(obj.getClass());
    }

    @Override public <T> TypeCodec<T> getSerializerForClass(Class valueClass) {
        TypeCodec serializer = null;
        if (valueClass == BigInteger.class) {
            serializer = TypeCodec.varint();
        } else if (valueClass.equals(Boolean.class) || valueClass.equals(boolean.class)) {
            serializer = TypeCodec.cboolean();
        } else if (valueClass.equals(byte[].class)) {
            serializer = TypeCodec.blob();
        } else if (valueClass.equals(ByteBuffer.class)) {
            serializer = TypeCodec.blob();
        } else if (valueClass.equals(Character.class)) {
            serializer = TypeCodec.varchar();
        } else if (valueClass.equals(Date.class)) {
            serializer = TypeCodec.timestamp();
        } else if (valueClass.equals(Double.class) || valueClass.equals(double.class)) {
            serializer = TypeCodec.cdouble();
        } else if (valueClass.equals(Float.class) || valueClass.equals(float.class)) {
            serializer = TypeCodec.cfloat();
        } else if (valueClass.equals(Integer.class) || valueClass.equals(int.class)) {
            serializer = TypeCodec.cint();
        } else if (valueClass.equals(Long.class) || valueClass.equals(long.class)) {
            serializer = TypeCodec.bigint();
        } else if (valueClass.equals(Short.class) || valueClass.equals(short.class)) {
            serializer = TypeCodec.smallInt();
        } else if (valueClass.equals(String.class)) {
            serializer = TypeCodec.varchar();
        } else if (valueClass.equals(UUID.class)) {
            serializer = TypeCodec.uuid();
        } else {
            serializer = hercules.getCodecByClassType().get(valueClass);
        }
        // Add other serializers here

        return serializer;

    }

    public <R, T> int getRowCount(String keyspace, String columnFamily,
                                  DataOperationsProfile dataOperationsProfile,
                                  RowSerializer<R, T> rowSerializer,
                                  R from, R to, Integer count) {
        Select select = select().from(keyspace, quote(columnFamily));
        if (from != null) {
            select.where(gte(token("key"), token(from)));
        }
        if (to != null) {
            select.where(lte(token("key"), token(to)));
        }
        MutableInt totalRows = new MutableInt();
        execute(select, rs -> {
            totalRows.add(rs.one().get("count", Integer.class));
        });
        return totalRows.intValue();
    }

    public <R, T> int getTopCount(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T from, T to, Integer count) {
        throw new UnsupportedOperationException("not used in appmetr");
    }

    public <R, T> HerculesQueryResult<T> getRow(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey) {
        Statement stmt = createSelectStatement(keyspace, columnFamily, rowKey);


        HerculesMultiQueryResult<R, T> results = new HerculesMultiQueryResult<>();
        execute(stmt, rs -> {
            buildQueryResult(rs, Integer.MAX_VALUE, rowSerializer, results, dataOperationsProfile);

        });

        //todo: refactor
        HerculesQueryResult<T> result = new HerculesQueryResult<>();
        LinkedHashMap<T, Object> entries = results.getEntries().get(rowKey);
        if (entries != null) {
            result.setEntries(entries);
        }
        return result;
    }

    private void execute(Statement stmt) {
        execute(stmt, rc -> {
        });
    }

    private <R> Statement createSelectStatement(String keyspace, String columnFamily, R rowKey) {
        return select()
                .from(keyspace, quote(columnFamily))
                .where(eq(primaryKey(keyspace, columnFamily), rowKey));
    }

    private String primaryKey(String keyspace, String columnFamily) {
        List<ColumnMetadata> primaryKey = hercules.getCluster().getMetadata()
                .getKeyspace(keyspace)
                .getTable(quote(columnFamily))
                .getPrimaryKey();
        return quote(primaryKey.get(0).getName());
    }

    public <R, T> HerculesMultiQueryResult<R, T> getRows(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Iterable<R> rowKeys) {
        //todo: required
        return null;
    }

    public <R, T> HerculesMultiQueryResult<R, T> getAllRows(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer) {
        //todo: required
        return null;
    }

    public <R, T> HerculesQueryResult<T> getSlice(String keyspace,
                                                  String columnFamily,
                                                  DataOperationsProfile dataOperationsProfile,
                                                  RowSerializer<R, T> rowSerializer,
                                                  R rowKey,
                                                  SliceDataSpecificator<T> sliceDataSpecificator) {
        return null;
    }

    public <R, T> HerculesMultiQueryResult<R, T> getSlice(String keyspace,
                                                          String columnFamily,
                                                          DataOperationsProfile dataOperationsProfile,
                                                          RowSerializer<R, T> rowSerializer,
                                                          Iterable<R> rowKeys,
                                                          SliceDataSpecificator<T> sliceDataSpecificator) {


        Select select = select().from(keyspace, quote(columnFamily));

        if (rowKeys.iterator().hasNext()) {
            select.where(in(primaryKey(keyspace, quote(columnFamily)), rowKeys));
        }

        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get range slice " + columnFamily);

        HerculesMultiQueryResult<R, T> result = new HerculesMultiQueryResult<>();
        try {
            execute(select, rs -> buildQueryResult(rs, Integer.MAX_VALUE, rowSerializer, result, dataOperationsProfile));
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }

        return result;
    }

    public <R, T> HerculesMultiQueryResult<R, T> getRangeSlice(String keyspace, String columnFamily,
                                                               DataOperationsProfile dataOperationsProfile,
                                                               RowSerializer<R, T> rowSerializer, R rowFrom, R rowTo, Integer rowCount, SliceDataSpecificator<T> sliceDataSpecificator) {
        Select select = select().from(keyspace, quote(columnFamily));
        if (rowFrom != null) {
            select.where(gte(token("key"), token(rowFrom)));
        }
        if (rowTo != null) {
            select.where(lte(token("key"), token(rowTo)));
        }


        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get range slice " + columnFamily);

        HerculesMultiQueryResult<R, T> result = new HerculesMultiQueryResult<>();
        try {
            execute(select, rs -> buildQueryResult(rs, rowCount, rowSerializer, result, dataOperationsProfile));
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                dataOperationsProfile.dbQueries++;
            }
        }

        return result;
    }

    public <R, T> List<R> getKeyRange(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R from, R to, Integer count) {
        return null;
    }

    public <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T topKey, Object value, int ttl) {
        int serializedDataSize = 0;
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Insert " + columnFamily);

        try {
            if (value == null) {
                execute(QueryBuilder.delete()
                        .from(keyspace, quote(columnFamily))
                        .where(eq("key", rowKey)));
            } else {
                execute(insertInto(keyspace, columnFamily)
                        .value("key", rowKey)
                        .value("column1", topKey)
                        .value("value", value));
            }
        } finally {
            long time = monitor.stop();
            if (dataOperationsProfile != null) {
                dataOperationsProfile.ms += time;
                //dataOperationsProfile.bytes += serializedDataSize;
                dataOperationsProfile.dbQueries++;
            }
        }
    }

    public <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, Map<T, Integer> ttls) {


    }

    public <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, int ttl) {
        Map<R, Map<T, Object>> valuesToInsert = Collections.singletonMap(rowKey, values);

        insert(keyspace, columnFamily, dataOperationsProfile, rowSerializer, valuesToInsert, (row, top) -> ttl);
    }

    public <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Map<T, Object>> values, Map<R, Map<T, Integer>> ttls) {

    }

    public <R, T> void delete(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey) {
        execute(QueryBuilder
                .delete()
                .from(keyspace, quote(columnFamily))
                .where(eq("key", rowKey)));
    }

    public <R, T> void delete(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Iterable<T> topKeys) {
        delete(keyspace, columnFamily, dataOperationsProfile, rowSerializer, Collections.singletonMap(rowKey, topKeys));
    }

    public <R, T> void delete(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Iterable<T>> topKeysInRows) {
        for (R rowKey : topKeysInRows.keySet()) {
            Delete.Where delete = QueryBuilder
                    .delete()
                    .from(keyspace, quote(columnFamily))
                    .where(eq("key", rowKey))
                    .and(in("column1", topKeysInRows.get(rowKey)));
            execute(delete);
        }
        //todo: make batch, group by row key
    }

    private <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Map<T, Object>> values, TTLProvider<R, T> ttlProvider) {
        TypeCodec<R> rowKeySerializer = rowSerializer.getRowKeySerializer();

        //Mutator<ByteBuffer> mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());

        List<Statement> statements = new ArrayList<>();
        Batch batch = batch();

        int serializedDataSize = 0;
        for (R rowKey : values.keySet()) {
            for (Map.Entry<T, Object> entry : values.get(rowKey).entrySet()) {
                Insert insert =
                        insertInto(keyspace, "\"" + columnFamily + "\"");


                insert.value("key", rowKeySerializer.serialize(rowKey, protocol()));
                T topKey = entry.getKey();
                Object value = entry.getValue();

                if (value == null) {
                    //  mutator.addDeletion(serializedRowKey, columnFamily, topKey, rowSerializer.getTopKeySerializer());
                } else {

                    insert.value("column1", topKey);
                    insert.value("value", rowSerializer.getValueSerializer(topKey).serialize(value, protocol()));

                    int ttl = DataDriver.EMPTY_TTL;
                    if (ttlProvider != null) {
                        ttl = ttlProvider.get(rowKey, topKey);
                    }

                    if (ttl > 0) {
                        insert.using(timestamp(ttl));
                    }
                    SimpleStatement stmt = new SimpleStatement(insert.getQueryString());
                    batch.add(stmt);
                }
            }
        }
        execute(batch);

    }

    private <R, T> void buildQueryResult(ResultSet rows, Integer rowCount, RowSerializer<R, T> rowSerializer, HerculesMultiQueryResult<R, T> results, DataOperationsProfile dataOperationsProfile) {
        LinkedHashMap<R, LinkedHashMap<T, Object>> result = new LinkedHashMap<>();  // row key -> ( column key value -> column value)
        ColumnDefinitions columnDefinitions = rows.getColumnDefinitions();

        R lastKey = null;

        for (Row row : rows) {
            /*if (rows.getAvailableWithoutFetching() == 100 && !rows.isFullyFetched())
                rows.fetchMoreResults(); // this is asynchronous
            // Process the row ...
*/

            T columnName = null;
            Object columnValue = null;
            LinkedHashMap<T, Object> valueMap = new LinkedHashMap<>();

            for (ColumnDefinitions.Definition column : columnDefinitions) {
                switch (column.getName()) {
                    case "key":
                        lastKey = rowSerializer.getRowKeySerializer().deserialize(
                                row.get(column.getName(), codecForColumn(column)),
                                protocol());
                        break;
                    case "column1":
                        columnName = (T) row.get("column1", String.class);
                        break;
                    case "value":
                        TypeCodec valueSerializer = rowSerializer.getValueSerializer(columnName);
                        columnValue = valueSerializer != null ? valueSerializer.deserialize(
                                row.get(column.getName(), TypeCodec.blob()),
                                protocol()) : null; //todo: remove this temp
                        break;
                    default:
                        throw new UnsupportedOperationException("unsupported column " + column.getName());
                }

            }
            valueMap.put(columnName, columnValue);

            if (result.keySet().size() > rowCount) {
                break;
            }
            result.computeIfAbsent(lastKey, v -> new LinkedHashMap<>()).putAll(valueMap);
        }
        results.setEntries(result);
        results.setLastKey(lastKey);
    }

    private ProtocolVersion protocol() {
        return hercules.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    }

    private <T> TypeCodec<T> codecForColumn(ColumnDefinitions.Definition column) {
        return hercules.getCluster().getConfiguration().getCodecRegistry().codecFor(column.getType());
    }

    private interface TTLProvider<R, T> {
        int get(R row, T top);
    }

    private void execute(Statement stmt, Consumer<ResultSet> consumer) {
        logger.trace("trying to execute stmt {}", stmt);
        stmt.setConsistencyLevel(QUORUM);
        try (Session session = hercules.getCluster().newSession()) {
            ResultSet rs = session.execute(stmt);
            consumer.accept(rs);
        } catch (Exception e) {
            logger.error("cannot execute stmt: " + stmt.toString(), e);
        }
    }


}

