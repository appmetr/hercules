package com.appmetr.hercules.driver;

import java.util.LinkedHashMap;

public class HerculesQueryResult<T> {
    private LinkedHashMap<T, Object> entries = new LinkedHashMap<T, Object>();
    boolean hasResult = false;

    public HerculesQueryResult() {
    }

    public HerculesQueryResult(LinkedHashMap<T, Object> entries) {
        this.entries = entries;
        this.hasResult = true;
    }

    public LinkedHashMap<T, Object> getEntries() {
        return entries;
    }

    public boolean hasResult() {
        return hasResult;
    }
}
