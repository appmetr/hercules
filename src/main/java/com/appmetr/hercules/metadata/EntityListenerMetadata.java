package com.appmetr.hercules.metadata;

import java.lang.reflect.Method;

public class EntityListenerMetadata {
    private Class entityListenerClass;
    private Object entityListener;

    private Method preLoadMethod;
    private Method postLoadMethod;
    private Method prePersistMethod;
    private Method postPersistMethod;

    public Class getEntityListenerClass() {
        return entityListenerClass;
    }

    public void setEntityListenerClass(Class entityListenerClass) {
        this.entityListenerClass = entityListenerClass;
    }

    public Object getEntityListener() {
        return entityListener;
    }

    public void setEntityListener(Object entityListener) {
        this.entityListener = entityListener;
    }

    public Method getPreLoadMethod() {
        return preLoadMethod;
    }

    public void setPreLoadMethod(Method preLoadMethod) {
        this.preLoadMethod = preLoadMethod;
    }

    public Method getPostLoadMethod() {
        return postLoadMethod;
    }

    public void setPostLoadMethod(Method postLoadMethod) {
        this.postLoadMethod = postLoadMethod;
    }

    public Method getPrePersistMethod() {
        return prePersistMethod;
    }

    public void setPrePersistMethod(Method prePersistMethod) {
        this.prePersistMethod = prePersistMethod;
    }

    public Method getPostPersistMethod() {
        return postPersistMethod;
    }

    public void setPostPersistMethod(Method postPersistMethod) {
        this.postPersistMethod = postPersistMethod;
    }
}
