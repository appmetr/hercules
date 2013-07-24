package com.appmetr.hercules.column;

import com.appmetr.hercules.partition.dated.DatedColumn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class TestDatedColumn implements DatedColumn<TestDatedColumn> {

    protected long date;

    public TestDatedColumn(long date) {
        this.date = date;
    }

    @Override public long getDate() {
        return date;
    }

    @Override public String render() {
        return new DateTime(date, DateTimeZone.UTC).toString();
    }

    @Override public TestDatedColumn max(TestDatedColumn col) {
        return new TestDatedColumn(Math.max(date, col.date));
    }

    @Override public TestDatedColumn min(TestDatedColumn col) {
        return new TestDatedColumn(Math.min(date, col.date));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestDatedColumn that = (TestDatedColumn) o;

        if (date != that.date) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (date ^ (date >>> 32));
    }

    @Override public int compareTo(TestDatedColumn col) {
        if (date == col.date) {
            return 0;
        }

        return (date < col.date) ? -1 : 1;
    }

    @Override public String toString() {
        return "SDC-" + new DateTime(date, DateTimeZone.UTC).toString();
    }
}