package com.wrmsr.presto.js;

import com.google.common.base.Preconditions;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import jdk.nashorn.internal.runtime.ScriptObject;
import jdk.nashorn.internal.runtime.linker.JavaAdapterServices;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class ScriptObjectAccess
{

    private ScriptObjectAccess()
    {
    }

    /*
    private static final Method getScriptObjectMethod;
    private static final Method getCallMethodHandleMethod;

    static {
        try {
            getScriptObjectMethod = ScriptObjectMirror.class.getDeclaredMethod("getScriptObject");
            getScriptObjectMethod.setAccessible(true);

            Class<?> scriptFunctionClass = Class.forName("jdk.nashorn.internal.runtime.ScriptFunction");
            getCallMethodHandleMethod = scriptFunctionClass.getDeclaredMethod("getCallMethodHandle", MethodType.class, String.class);
            getCallMethodHandleMethod.setAccessible(true);
        } catch (NoSuchMethodException | IllegalAccessError | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static ScriptObject getScriptObjectMirrorTarget(ScriptObjectMirror smo)
    {
        try {
            return (ScriptObject) getScriptObjectMethod.invoke(smo);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static ScriptObject getScriptObjectMirrorTarget(Object o)
    {
        return getScriptObjectMirrorTarget((ScriptObjectMirror) o);
    }

    public static MethodHandle getScriptFunctionObjectHandle(Object sfn, String name, MethodType mto)
    {
        try {
            return (MethodHandle) getCallMethodHandleMethod.invoke(sfn, mto, name);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
    */
}
