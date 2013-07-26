package com.appmetr.hercules.utils;

import java.io.Serializable;

public class Tuple2<T1, T2> implements Serializable {

    public T1 e1;
    public T2 e2;

    public Tuple2() {
    }

    public Tuple2(T1 e1, T2 e2) {
        this.e1 = e1;
        this.e2 = e2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple2 tuple2 = (Tuple2) o;

        if (e1 != null ? !e1.equals(tuple2.e1) : tuple2.e1 != null) return false;
        if (e2 != null ? !e2.equals(tuple2.e2) : tuple2.e2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = e1 != null ? e1.hashCode() : 0;
        result = 31 * result + (e2 != null ? e2.hashCode() : 0);
        return result;
    }

    @Override public String toString() {
        return "Tuple2{" +
                "e1=" + e1 +
                ", e2=" + e2 +
                '}';
    }
}

