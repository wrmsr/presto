package com.wrmsr.presto.wrapper;

import com.wrmsr.presto.util.Repositories;

public class PrestoWrapperBootstrap
{
    public static void main(String[] args) throws Throwable
    {
        Repositories.setupClassLoaderForModule(PrestoWrapperBootstrap.class.getClassLoader(), "presto-wrmsr-wrapper");
        Class<?> cls = Class.forName("com.wrmsr.presto.wrapper.PrestoWrapperMain");
        cls.getDeclaredMethod("main", String[].class).invoke(null, new Object[]{args});
    }
}
