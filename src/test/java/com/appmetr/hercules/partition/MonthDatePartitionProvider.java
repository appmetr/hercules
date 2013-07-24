package com.appmetr.hercules.partition;

import com.appmetr.hercules.partition.dated.DatePartiotionProvider;
import com.appmetr.hercules.partition.dated.DatePartsConfig;
import com.appmetr.hercules.partition.dated.DatedColumn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;

public class MonthDatePartitionProvider<T extends DatedColumn> extends DatePartiotionProvider<T> {
    public MonthDatePartitionProvider() {
        super(new DatePartsConfig(
                new DateTime(2013, DateTimeConstants.JANUARY, 1, 0, 0, DateTimeZone.UTC),
                DateTimeFieldType.monthOfYear(),
                3
        ));
    }
}
