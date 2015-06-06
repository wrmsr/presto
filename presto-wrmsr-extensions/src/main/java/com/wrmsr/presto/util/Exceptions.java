package com.wrmsr.presto.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Function;
import java.util.function.Supplier;

public class Exceptions
{
    private Exceptions()
    {
    }

    public static String getStackTraceString(Throwable yelpException)
    {
        StringWriter trace = new StringWriter();
        yelpException.printStackTrace(new PrintWriter(trace));
        return trace.toString();
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R> {

        R apply(T t) throws Exception;
    }

    public static <T, R> Function<T, R> runtimeThrowing(ThrowingFunction<T, R> fn)
    {
        return t -> {
            try {
                return fn.apply(t);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @FunctionalInterface
    public interface ThrowingSupplier<R> {

        R get() throws Exception;
    }

    public static <R> Supplier<R> runtimeThrowing(ThrowingSupplier<R> fn)
    {
        return () -> {
            try {
                return fn.get();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
