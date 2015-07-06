package com.wrmsr.presto.server;

import com.google.inject.Module;

import java.util.function.Function;

public interface ModuleProcessor extends Function<Module, Module>
{
}
