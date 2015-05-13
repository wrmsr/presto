package com.wrmsr.presto.js;

import bsh.org.objectweb.asm.CodeVisitor;
import com.facebook.presto.Session;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableMap;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.testng.annotations.Test;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;
import static org.objectweb.asm.Opcodes.ACC_ABSTRACT;
import static org.objectweb.asm.Opcodes.ACC_ANNOTATION;
import static org.objectweb.asm.Opcodes.ACC_BRIDGE;
import static org.objectweb.asm.Opcodes.ACC_ENUM;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_INTERFACE;
import static org.objectweb.asm.Opcodes.ACC_NATIVE;
import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PROTECTED;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_STRICT;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ACC_SYNCHRONIZED;
import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;
import static org.objectweb.asm.Opcodes.ACC_TRANSIENT;
import static org.objectweb.asm.Opcodes.ACC_VARARGS;
import static org.objectweb.asm.Opcodes.ACC_VOLATILE;
import static org.objectweb.asm.Opcodes.V1_7;

public class TestJSPlugin
        extends AbstractTestQueryFramework
{
    public TestJSPlugin()
    {
        super(createLocalQueryRunner());
    }

    public interface Int2Int {
        int f(int x);
    }


    @Test
    public void testSanity()
            throws Throwable
    {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        engine.eval("print('Hello World!');");
        Object o = engine.eval("function f(x) { return x + 1 }");
        System.out.println(o);
        Invocable inv = (Invocable) engine;
        // Int2Int iface;// = inv.getInterface(engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE).get("f"), Int2Int.class);

        String className = "dyn/Int2Int";
        ClassWriter cw = new ClassWriter(0);
        cw.visit(
                V1_7,
                ACC_PUBLIC+ACC_ABSTRACT+ACC_INTERFACE,
                className,    // class name
                null,
                "java/lang/Object", // super class
                null               // interfaces
                );   // source file

        MethodVisitor mv;
        mv = cw.visitMethod(
                ACC_PUBLIC+ACC_ABSTRACT,
                "f",                // method name
                "(I)I", // method descriptor
                null,                    // exceptions
                null);                   // method attributes

        cw.visitEnd();

        byte[] bytecode = cw.toByteArray();
        Class ifaceCls = (new DynamicClassLoader()).defineClass("dyn.Int2Int", bytecode);


        Object iface = inv.getInterface(ifaceCls);
        Method m = ifaceCls.getDeclaredMethod("f", int.class);
        System.out.println(m.invoke(iface, 10));
        MethodHandle mh = MethodHandles.lookup().unreflect(m);
        // FIXME this.class + invokeExact
        MethodHandle mh2 = mh.asType(MethodType.methodType(int.class, int.class));
        System.out.println(mh2.invoke(100));
        /*
        o = engine.getContext().getBindings(javax.script.ScriptContext.ENGINE_SCOPE).get("f");
        MethodHandle mh = ScriptObjectAccess.getScriptFunctionObjectHandle(
                ScriptObjectAccess.getScriptObjectMirrorTarget(o), "f", MethodType.methodType(int.class, int.class));
        System.out.println(mh.invoke(20));
        */


        Object y = inv.invokeFunction("f", 10);
        System.out.println(y);
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog(),
                new TpchConnectorFactory(localQueryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        JSPlugin plugin = new JSPlugin();
        plugin.setTypeManager(localQueryRunner.getTypeManager());
        for (Type type : plugin.getServices(Type.class)) {
            localQueryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getServices(ParametricType.class)) {
            localQueryRunner.getTypeManager().addParametricType(parametricType);
        }
        // localQueryRunner.getMetadata().getFunctionRegistry().addFunctions(Iterables.getOnlyElement(plugin.getServices(FunctionFactory.class)).listFunctions());

        return localQueryRunner;
    }
}
