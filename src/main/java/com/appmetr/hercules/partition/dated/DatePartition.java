package com.appmetr.hercules.partition.dated;

import org.joda.time.DateTime;

public class DatePartition {

    private DateTime from;
    private DateTime to;

    public DatePartition() {
    }

    public DatePartition(DateTime from, DateTime to) {
        this.from = from;
        this.to = to;
    }

    public DateTime getFrom() {
        return from;
    }

    public void setFrom(DateTime from) {
        this.from = from;
    }

    public DateTime getTo() {
        return to;
    }

    public void setTo(DateTime to) {
        this.to = to;
    }

    public long middlePoint() {
        return (long) ((from.getMillis() + to.getMillis()) * 0.5 + 0.5);
    }

    @Override public String toString() {
        return "DateTimePartition{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }
}
