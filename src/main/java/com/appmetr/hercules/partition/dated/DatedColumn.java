package com.appmetr.hercules.partition.dated;

public interface DatedColumn<TDatedColumn extends DatedColumn> extends Comparable<TDatedColumn> {

    long getDate();
    
    String render();

    TDatedColumn max(TDatedColumn col);

    TDatedColumn min(TDatedColumn col);
}
