package com.appmetr.hercules.wide;

public class SliceDataSpecificatorByCF<T> {

    private String partitionName;
    private SliceDataSpecificator<T> sliceDataSpecificator;

    public SliceDataSpecificatorByCF() {
    }

    public SliceDataSpecificatorByCF(String partitionName, SliceDataSpecificator<T> sliceDataSpecificator) {
        this.partitionName = partitionName;
        this.sliceDataSpecificator = sliceDataSpecificator;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public SliceDataSpecificator<T> getSliceDataSpecificator() {
        return sliceDataSpecificator;
    }

    public void setSliceDataSpecificator(SliceDataSpecificator<T> sliceDataSpecificator) {
        this.sliceDataSpecificator = sliceDataSpecificator;
    }

    @Override public String toString() {
        return "SliceDateSpecificatorByCF{" +
                "partitionName='" + partitionName + '\'' +
                ", sliceDataSpecificator=" + sliceDataSpecificator +
                '}';
    }

}
