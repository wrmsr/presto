package com.wrmsr.presto.scripting;

import com.facebook.presto.metadata.SqlFunction;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryProvider;
import com.google.inject.multibindings.Multibinder;

public class ScriptingModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        Multibinder<SqlFunction> functionBinder = Multibinder.newSetBinder(binder, SqlFunction.class);

        // binder.bind(ScriptFunction.Factory.class).toProvider(FactoryProvider.newFactory(ScriptFunction.Factory.class, ScriptFunction.class));
        // binder.bind(ScriptFunction.Registration.MaxArity.class).toInstance(new ScriptFunction.Registration.MaxArity(3));
        // binder.bind(ScriptFunction.Registration.class).asEagerSingleton();
        // functionBinder.addBinding().to(ScriptFunction.class);
    }
}
