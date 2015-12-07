package com.wrmsr.presto.scripting;

import org.apache.commons.lang.NotImplementedException;

import javax.script.ScriptEngine;

public class Scripting
{
    private final String name;
    private final ScriptEngine scriptEngine;

    public Scripting(String name, ScriptEngine scriptEngine)
    {
        this.name = name;
        this.scriptEngine = scriptEngine;
    }

    // http://www.drdobbs.com/jvm/jsr-223-scripting-for-the-java-platform/215801163?pgno=2
    public Object invokeFunction(String name, Object... args)
    {
        throw new NotImplementedException();
    }
}
