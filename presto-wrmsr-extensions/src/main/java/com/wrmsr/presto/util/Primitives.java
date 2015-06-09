package com.wrmsr.presto.util;

public class Primitives
{
    private Primitives()
    {
    }

    public static boolean toBool(Object o)
    {
        if (o == null) {
            return false;
        }
        else if (o instanceof Boolean) {
            return (Boolean) o;
        }
        else if (o instanceof String) {
            return Boolean.valueOf((String) o);
        }
        else {
            return false;
        }
    }
}
