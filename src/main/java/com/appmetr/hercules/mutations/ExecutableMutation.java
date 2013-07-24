package com.appmetr.hercules.mutations;

public abstract class ExecutableMutation implements Comparable<ExecutableMutation> {

    public static enum MutationType {
        CREATE, DELETE
    }

    private MutationType type;
    private int retryCount = 0;
    private int priority = MutationsQueue.DEFAULT_PRIORITY;
    private String cfName;

    protected ExecutableMutation(MutationType type, String cfName) {
        this.type = type;
        this.cfName = cfName;
    }

    protected ExecutableMutation(MutationType type, String cfName, int priority) {
        this.type = type;
        this.cfName = cfName;
        this.priority = priority;
    }

    public abstract void execute() throws Exception;

    public abstract void skipped();

    public ExecutableMutation retry() {
        priority++;
        retryCount++;
        return this;
    }

    @Override
    public int compareTo(ExecutableMutation anotherPending) {
        int thisVal = this.priority;
        int anotherVal = anotherPending.priority;
        return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public MutationType getType() {
        return type;
    }

    public String getCfName() {
        return cfName;
    }

    @Override public String toString() {
        return "ExecutableMutation{" +
                "type=" + type +
                ", retryCount=" + retryCount +
                ", priority=" + priority +
                ", cfName='" + cfName + '\'' +
                '}';
    }
}
