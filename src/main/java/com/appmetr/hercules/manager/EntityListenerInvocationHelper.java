package com.appmetr.hercules.manager;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.metadata.EntityListenerMetadata;
import com.google.inject.Inject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class EntityListenerInvocationHelper {

    private @Inject Hercules hercules;

    public <E> E invokePreLoadListener(EntityListenerMetadata listenerMetadata, E entity) {
        Method method = listenerMetadata.getPreLoadMethod();
        if (method != null) {
            return invokeListenerMethod(listenerMetadata, method, entity);
        }

        return null;
    }

    public <E> E invokePostLoadListener(EntityListenerMetadata listenerMetadata, E entity) {
        Method method = listenerMetadata.getPostLoadMethod();
        if (method != null) {
            return invokeListenerMethod(listenerMetadata, method, entity);
        }

        return null;
    }

    public <E> E invokePrePersistListener(EntityListenerMetadata listenerMetadata, E entity) {
        Method method = listenerMetadata.getPrePersistMethod();
        if (method != null) {
            return invokeListenerMethod(listenerMetadata, method, entity);
        }

        return null;
    }

    public <E> void invokePostPersistListener(EntityListenerMetadata listenerMetadata, E entity) {
        Method method = listenerMetadata.getPostPersistMethod();
        if (method != null) {
            invokeListenerMethod(listenerMetadata, method, entity);
        }
    }

    private <E> E invokeListenerMethod(EntityListenerMetadata listenerMetadata, Method method, E entity) {
        Object obj = getEntityListener(listenerMetadata);

        if (obj == null) {
            obj = entity;
        }

        Object result;
        try {
            if (method.getParameterTypes().length == 0) {
                result = method.invoke(obj);
            } else if (method.getParameterTypes().length == 1) {
                result = method.invoke(obj, entity);
            } else {
                throw new RuntimeException("Wrong " + method.getName() + " method signature for listener " + obj.getClass().getSimpleName() + ". Method should receive 1 or 0 params ");
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        if (!void.class.equals(method.getReturnType())) {
            return (E) result;
        }

        return null;
    }

    private <E> Object getEntityListener(EntityListenerMetadata listenerMetadata) {
        if (listenerMetadata.getEntityListener() != null) {
            return listenerMetadata.getEntityListener();
        } else if (listenerMetadata.getEntityListenerClass() != null) {
            listenerMetadata.setEntityListener(hercules.getInjector().getInstance(listenerMetadata.getEntityListenerClass()));

            return listenerMetadata.getEntityListener();
        }

        return null;
    }
}
