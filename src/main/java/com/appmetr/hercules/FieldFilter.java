package com.appmetr.hercules;

import java.lang.reflect.Field;

public interface FieldFilter {

    boolean accept(Field field);
}
