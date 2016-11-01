package com.appmetr.hercules.partition.dated;

import com.appmetr.hercules.partition.TopKeyPartiotionProvider;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;
import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

public class DatePartitionProvider<T extends DatedColumn> extends TopKeyPartiotionProvider<T> {

    private DatePartsConfig partsConfig;

    public DatePartitionProvider(DatePartsConfig partsConfig) {
        this.partsConfig = partsConfig;
    }

    @Override public String getPartition(T topKey) {
        return getDatePartition(topKey.getDate(), partsConfig);
    }

    @Override public List<Object> getPartitionedRowKeys(Object rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {

        if(rowKey instanceof DatePartitionedKey){
            final DatePartitionedKey datePartitionedKey = (DatePartitionedKey) rowKey;
            final LinkedList<Object> keys = new LinkedList<Object>();
            final DateTime fromDateTime = new DateTime(sliceDataSpecificator.getLowEnd().getDate());
            final DateTime toDateTime = new DateTime(sliceDataSpecificator.getHighEnd().getDate());

            DateTime curDate = new DateTime(fromDateTime);
            while (curDate.getMillis() <= toDateTime.getMillis()){
                final DateTime ceilDateToPeriod = ceilDateToPeriod(curDate, partsConfig.getPartsField(), 1);
                keys.add(datePartitionedKey.copyWithPartitionKey(ceilDateToPeriod.getMillis()));
                curDate = curDate.plusDays(1);
            }
            return keys;
        }
        return super.getPartitionedRowKeys(rowKey, sliceDataSpecificator);
    }

    @Override public Object getPartitionedRowKey(Object rowKey, T topKey) {
        if(rowKey instanceof DatePartitionedKey){
            final DatePartitionedKey datePartitionedKey = (DatePartitionedKey) rowKey;
            final DateTime date = ceilDateToPeriod(new DateTime(topKey.getDate()),partsConfig.getPartsField(),1);
            return datePartitionedKey.copyWithPartitionKey(date.getMillis());
        }
        return super.getPartitionedRowKey(rowKey, topKey);
    }

    @Override
    public List<SliceDataSpecificatorByCF<T>> getPartitionedQueries(SliceDataSpecificator<T> sliceDataSpecificator) {

        List<SliceDataSpecificatorByCF<T>> queries = getDatePartitionedQueries(sliceDataSpecificator, partsConfig);

        List<SliceDataSpecificatorByCF<T>> rowQueries = new ArrayList<SliceDataSpecificatorByCF<T>>();
        for (SliceDataSpecificatorByCF<T> query : queries) {

            rowQueries.add(new SliceDataSpecificatorByCF<T>(query.getPartitionName(), query.getSliceDataSpecificator()));
        }
        return rowQueries;
    }

    @Override public List<String> getPartitionsForCreation() {
        List<String> foundPartitions = new ArrayList<String>();

        List<DatePartition> partitions = getPartitionsList(partsConfig);
        for (DatePartition partition : partitions) {

            long middlePoint = partition.middlePoint();

            String cfPartition = getDatePartition(middlePoint, partsConfig);
            foundPartitions.add(cfPartition);
        }

        return foundPartitions;
    }

    private <T extends DatedColumn<T>> List<SliceDataSpecificatorByCF<T>> getDatePartitionedQueries(
            SliceDataSpecificator<T> sliceDataSpecificator, DatePartsConfig partsConfig) {

        List<SliceDataSpecificatorByCF<T>> parts = new ArrayList<SliceDataSpecificatorByCF<T>>();

        List<DatePartition> partitions = getPartitionsList(partsConfig);

        if (sliceDataSpecificator.getType() == SliceDataSpecificator.SliceDataSpecificatorType.RANGE) {

            DateSegment<T> sliceSegment = new DateSegment<T>(sliceDataSpecificator);

            for (DatePartition partition : partitions) {

                DateSegment<T> partitionSegment = new DateSegment<T>(partition.getFrom().getMillis(), partition.getTo().getMillis());
                DateSegment<T> intersectSegment = partitionSegment.intersection(sliceSegment);
                if (intersectSegment.isValid()) {

                    SliceDataSpecificator<T> intersectSlice = intersectSegment.toSliceDataSpecificator(sliceDataSpecificator.isOrderDesc(), 0);
                    parts.add(new SliceDataSpecificatorByCF<T>(
                            getPartitionName(partition),
                            intersectSlice
                    ));
                }
            }

            if (sliceDataSpecificator.isOrderDesc()) {
                List<SliceDataSpecificatorByCF<T>> reverseParts = new ArrayList<SliceDataSpecificatorByCF<T>>(parts.size());
                for (int i = parts.size() - 1; i >= 0; i--) {
                    reverseParts.add(parts.get(i));
                }
                parts = reverseParts;
            }

        } else if (sliceDataSpecificator.getType() == SliceDataSpecificator.SliceDataSpecificatorType.COLUMNS) {

            TreeSet<T> columns = sliceDataSpecificator.getColumnsArray() != null ?
                    new TreeSet<T>(Arrays.asList(sliceDataSpecificator.getColumnsArray())) :
                    new TreeSet<T>(sliceDataSpecificator.getColumnsCollection());

            for (DatePartition partition : partitions) {

                if (columns.size() == 0) {
                    break;
                }

                Set<T> partColumns = new TreeSet<T>();
                for (T column : columns) {
                    if (column.getDate() >= partition.getFrom().getMillis() && column.getDate() <= partition.getTo().getMillis()) {
                        partColumns.add(column);
                    }
                }

                if (partColumns.size() > 0) {

                    columns.removeAll(partColumns);
                    parts.add(new SliceDataSpecificatorByCF<T>(
                            getPartitionName(partition),
                            new SliceDataSpecificator<T>(partColumns)
                    ));
                }
            }

        } else {
            throw new IllegalStateException("Invalid type: " + sliceDataSpecificator.getType());
        }

        return parts;
    }

    private List<DatePartition> getPartitionsList(DatePartsConfig partsConfig) {

        DateTime endByCurrTime = ceilDateToPeriod(new DateTime(DateTimeZone.UTC), partsConfig.getPartsField(), 1);
        endByCurrTime = endByCurrTime.property(partsConfig.getPartsField()).addToCopy(partsConfig.getPartsForward());

        DateTime from = new DateTime(0, DateTimeZone.UTC);
        DateTime to = partsConfig.getPartsStart().withZone(DateTimeZone.UTC);

        List<DatePartition> partitions = new ArrayList<DatePartition>();

        while (!to.isAfter(endByCurrTime)) {
            partitions.add(new DatePartition(from, to.minusMillis(1)));

            from = to;
            to = ceilDateToPeriodPlus(to, partsConfig.getPartsField(), 1);
        }

        return partitions;
    }

    private String getDatePartition(long column, DatePartsConfig partsConfig) {
        List<DatePartition> partitions = getPartitionsList(partsConfig);

        for (DatePartition partition : partitions) {

            DateSegment segment = new DateSegment(partition.getFrom().getMillis(), partition.getTo().getMillis());

            try {
                if (segment.contains(column)) {
                    return getPartitionName(partition);
                }
            } catch (RuntimeException e) {
                throw new RuntimeException("" + column + " >> " + segment, e);
            }
        }
        throw new IllegalArgumentException("Column is out of partitions range: " + new DateTime(column, DateTimeZone.UTC));
    }

    protected String getPartitionName(DatePartition partition) {
        if (partition.getFrom().withZone(DateTimeZone.UTC).getMillis() == 0) {
            return "";
        }

        return dateTimeToStr(partition.getFrom()) + dateTimeToStr(partition.getTo().plusMillis(1));
    }

    protected String dateTimeToStr(DateTime dateTime) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyMMdd");

        return formatter.print(dateTime.withZone(DateTimeZone.UTC));
    }

    public static DateTime ceilDateToPeriod(ReadableDateTime time, DateTimeFieldType fieldType, int count) {
        MutableDateTime roundUp = new MutableDateTime(time).property(fieldType).roundCeiling();
        int mod = time.get(fieldType) % count;
        if (mod > 0) {
            roundUp.add(fieldType.getDurationType(), count - mod);
        }
        return roundUp.toDateTime();
    }

    public static DateTime ceilDateToPeriodPlus(ReadableDateTime time, DateTimeFieldType fieldType, int count) {
        MutableDateTime roundUp = new MutableDateTime(time).property(fieldType).roundCeiling();
        int mod = time.get(fieldType) % count;
        roundUp.add(fieldType.getDurationType(), count - mod);
        return roundUp.toDateTime();
    }

}
