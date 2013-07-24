package com.appmetr.hercules.partition.dated;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;

public class DatePartsConfig {

    private DateTime partsStart;
    private DateTimeFieldType partsField;
    private int partsForward;

    public DatePartsConfig() {
    }

    public DatePartsConfig(DateTime partsStart, DateTimeFieldType partsField, int partsForward) {
        this.partsStart = partsStart;
        this.partsField = partsField;
        this.partsForward = partsForward;
    }

    public DateTime getPartsStart() {
        return partsStart;
    }

    public void setPartsStart(DateTime partsStart) {
        this.partsStart = partsStart;
    }

    public DateTimeFieldType getPartsField() {
        return partsField;
    }

    public void setPartsField(DateTimeFieldType partsField) {
        this.partsField = partsField;
    }

    public int getPartsForward() {
        return partsForward;
    }

    public void setPartsForward(int partsForward) {
        this.partsForward = partsForward;
    }

    @Override public String toString() {
        return "PartsConfig{" +
                "partsStart=" + partsStart +
                ", partsField=" + partsField +
                ", partsForward=" + partsForward +
                '}';
    }
}
