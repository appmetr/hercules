package com.appmetr.hercules.partition.dated;

import com.appmetr.hercules.wide.SliceDataSpecificator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DateSegment<TDatedColumn extends DatedColumn<TDatedColumn>> {

    private Long from;
    private TDatedColumn fromColumn;
    private Long to;
    private TDatedColumn toColumn;

    public DateSegment() {
    }

    public DateSegment(Long from, Long to) {
        this.from = from;
        this.to = to;
    }

    public DateSegment(SliceDataSpecificator<TDatedColumn> sliceDataSpecificator) {
        from = sliceDataSpecificator.getLowEnd() != null ? sliceDataSpecificator.getLowEnd().getDate() : null;
        fromColumn = sliceDataSpecificator.getLowEnd();
        to = sliceDataSpecificator.getHighEnd() != null ? sliceDataSpecificator.getHighEnd().getDate() : null;
        toColumn = sliceDataSpecificator.getHighEnd();
    }

    public DateSegment<TDatedColumn> intersection(DateSegment<TDatedColumn> segment) {

        DateSegment<TDatedColumn> intersection = new DateSegment<TDatedColumn>();

        if (from == null && segment.from == null) {
            intersection.from = null;
            intersection.fromColumn = null;
        } else if (from == null && segment.from != null) {
            intersection.from = segment.from;
            intersection.fromColumn = segment.fromColumn;
        } else if (from != null && segment.from == null) {
            intersection.from = from;
            intersection.fromColumn = fromColumn;
        } else {
            intersection.from = Math.max(from, segment.from);
            if (from < segment.from) {
                intersection.fromColumn = segment.fromColumn;
            } else if (from > segment.from) {
                intersection.fromColumn = fromColumn;
            } else {
                if (fromColumn == null && segment.fromColumn == null) {
                    intersection.fromColumn = null;
                } else if (fromColumn == null && segment.fromColumn != null) {
                    intersection.fromColumn = segment.fromColumn;
                } else if (fromColumn != null && segment.fromColumn == null) {
                    intersection.fromColumn = fromColumn;
                } else {
                    intersection.fromColumn = fromColumn.max(segment.fromColumn);
                }
            }
        }

        if (to == null && segment.to == null) {
            intersection.to = null;
            intersection.toColumn = null;
        } else if (to == null && segment.to != null) {
            intersection.to = segment.to;
            intersection.toColumn = segment.toColumn;
        } else if (to != null && segment.to == null) {
            intersection.to = to;
            intersection.toColumn = toColumn;
        } else {
            intersection.to = Math.min(to, segment.to);
            if (to > segment.to) {
                intersection.toColumn = segment.toColumn;
            } else if (to < segment.to) {
                intersection.toColumn = toColumn;
            } else {
                if (toColumn == null && segment.toColumn == null) {
                    intersection.toColumn = null;
                } else if (toColumn == null && segment.toColumn != null) {
                    intersection.toColumn = segment.toColumn;
                } else if (toColumn != null && segment.toColumn == null) {
                    intersection.toColumn = toColumn;
                } else {
                    intersection.toColumn = toColumn.min(segment.toColumn);
                }
            }
        }

        return intersection;
    }

    public boolean isValid() {
        if (from != null && to != null) {
            return from <= to;
        }
        return true;
    }

    public boolean contains(Long point) {
        return intersection(new DateSegment<TDatedColumn>(point, point)).isValid();
    }

    public SliceDataSpecificator<TDatedColumn> toSliceDataSpecificator(boolean orderDesc, int limit) {
        return new SliceDataSpecificator<TDatedColumn>(fromColumn, toColumn, orderDesc, limit);
    }

    public Long getFrom() {
        return from;
    }

    public void setFrom(Long from) {
        this.from = from;
    }

    public Long getTo() {
        return to;
    }

    public void setTo(Long to) {
        this.to = to;
    }

    @Override public String toString() {
        return "Segment{" +
                ((from == null) ? "null" : new DateTime(from, DateTimeZone.UTC).toString()) +
                " X " +
                ((to == null) ? "null" : new DateTime(to, DateTimeZone.UTC).toString()) +
                " | columns: " +
                ((fromColumn == null) ? "null" : fromColumn.render()) +
                " X " +
                ((toColumn == null) ? "null" : toColumn.render()) +
                "}";
    }
}
