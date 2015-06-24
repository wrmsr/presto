package com.facebook.presto.spi;

import javax.script.ScriptEngine;

public interface ScriptEngineProvider
{
    String getName();

    ScriptEngine getScriptEngine();
}
