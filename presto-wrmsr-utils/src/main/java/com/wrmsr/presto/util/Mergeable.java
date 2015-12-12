package com.wrmsr.presto.util;

import com.google.common.base.Throwables;

public interface Mergeable<T extends Mergeable<T>>
{
    T merge(T other);

    default T merge(Iterable<T> others)
    {
        @SuppressWarnings({"unchecked"})
        T t = (T) this;
        for (T o : others) {
            t = t.merge(o);
        }
        return t;
    }

    default T merge(T first, T... rest)
    {
        @SuppressWarnings({"unchecked"})
        T t = (T) this;
        t = t.merge(first);
        for (int i = 0; i < rest.length; ++i) {
            t = t.merge(rest[i]);
        }
        return t;
    }

    static <T extends Mergeable<T>> T unit(Class<T> cls)
    {
        try {
            return cls.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }
}
