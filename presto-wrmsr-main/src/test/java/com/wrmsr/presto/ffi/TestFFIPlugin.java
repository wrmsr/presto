/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.ffi;

import com.facebook.presto.Session;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.wrmsr.presto.MainPlugin;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.objectweb.asm.ClassWriter;
import org.testng.annotations.Test;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.objectweb.asm.Opcodes.ACC_ABSTRACT;
import static org.objectweb.asm.Opcodes.ACC_INTERFACE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.V1_7;

public class TestFFIPlugin
        extends AbstractTestQueryFramework
{
    public TestFFIPlugin()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testSanity()
            throws Throwable
    {
        // queryRunner.execute("select true");
        //queryRunner.execute("select * from lineitem inner join orders on orders.orderkey = lineitem.orderkey inner join customer on orders.custkey = customer.custkey limit 10");

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
                ACC_PUBLIC + ACC_ABSTRACT + ACC_INTERFACE,
                className,    // class name
                null,
                "java/lang/Object", // super class
                null               // interfaces
        );   // source file

        cw.visitMethod(
                ACC_PUBLIC + ACC_ABSTRACT,
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
        Session defaultSession = Session.builder(new SessionPropertyManager())
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

        MainPlugin plugin = new MainPlugin();
        plugin.setTypeRegistry(localQueryRunner.getTypeManager());
        for (Type type : plugin.getServices(Type.class)) {
            localQueryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getServices(ParametricType.class)) {
            localQueryRunner.getTypeManager().addParametricType(parametricType);
        }
        // localQueryRunner.getMetadata().getFunctionRegistry().addFunctions(Iterables.getOnlyElement(plugin.getServices(FunctionFactory.class)).listFunctions());

        return localQueryRunner;
    }

    // need thread per process :/
    // *or* jnr-process https://github.com/jnr/jnr-process
    // https://github.com/jnr/jnr-enxio/blob/master/src/main/java/jnr/enxio/example/TCPServer.java
    // lolll https://github.com/jnr/jnr-invoke
    @Test
    public void testSubprocess()
            throws Throwable
    {
        ProcessBuilder pb = new ProcessBuilder("/usr/bin/env", "sh", "-c", "sleep 3 ; echo hi");
        pb.redirectError();
        Process p;
        try {
            p = pb.start();
        }
        catch (IOException e) {
            throw new RuntimeException("failed to start", e);
        }
        // wait for command to exit
        int exitCode = p.waitFor();
        System.out.println(exitCode);
        String s = CharStreams.toString(new InputStreamReader(p.getInputStream(), UTF_8));
        System.out.println(s);
    }

    public static class Main
    {
        static final ByteBuffer buf = ByteBuffer.allocate(4096);

        public static void main(String[] args)
        {
            long timeout = 1000 * 5;

            try {
                InputStream in = extract(System.in);
                if (!(in instanceof FileInputStream)) {
                    throw new RuntimeException(
                            "Could not extract a FileInputStream from STDIN.");
                }

                try {
                    int ret = maybeAvailable((FileInputStream) in, timeout);
                    System.out.println(
                            Integer.toString(ret) + " bytes were read.");

                }
                finally {
                    in.close();
                }

            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        /* unravels all layers of FilterInputStream wrappers to get to the
         * core InputStream
         */
        public static InputStream extract(InputStream in)
                throws NoSuchFieldException, IllegalAccessException
        {
            Field f = FilterInputStream.class.getDeclaredField("in");
            f.setAccessible(true);

            while (in instanceof FilterInputStream) {
                in = (InputStream) f.get((FilterInputStream) in);
            }

            return in;
        }

        /* Returns the number of bytes which could be read from the stream,
         * timing out after the specified number of milliseconds.
         * Returns 0 on timeout (because no bytes could be read)
         * and -1 for end of stream.
         */
        public static int maybeAvailable(final FileInputStream in, long timeout)
                throws IOException, InterruptedException
        {
            final int[] dataReady = {0};
            final IOException[] maybeException = {null};
            final Thread reader = new Thread()
            {
                public void run()
                {
                    try {
                        dataReady[0] = in.getChannel().read(buf);
                    }
                    catch (ClosedByInterruptException e) {
                        System.err.println("Reader interrupted.");
                    }
                    catch (IOException e) {
                        maybeException[0] = e;
                    }
                }
            };

            Thread interruptor = new Thread()
            {
                public void run()
                {
                    reader.interrupt();
                }
            };

            reader.start();
            while (true) {
                reader.join(timeout);
                if (!reader.isAlive()) {
                    break;
                }

                interruptor.start();
                interruptor.join(1000);
                reader.join(1000);
                if (!reader.isAlive()) {
                    break;
                }

                System.err.println("We're hung");
                System.exit(1);
            }

            if (maybeException[0] != null) {
                throw maybeException[0];
            }

            return dataReady[0];
        }
    }

    @Test
    public void testThing() throws Throwable
    {
        /*
        CompressorOutputStream gzippedOut = new CompressorStreamFactory()
                .createCompressorOutputStream(CompressorStreamFactory.GZIP, myOutputStream);
        ArchiveInputStream input = new ArchiveStreamFactory()
                .createArchiveInputStream(originalInput);
        CompressorInputStream input = new CompressorStreamFactory()
                .createCompressorInputStream(originalInput);
        */

        /*
        QueryExplainer explainer = new QueryExplainer(session, planOptimizers, metadata, sqlParser, experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(explainer), experimentalSyntaxEnabled);
        Analysis analysis = analyzer.analyze(statement);

        final List<ViewDefinition.ViewColumn> columns;
        try {
            columns = analysis.getOutputDescriptor()
                    .getVisibleFields().stream()
                    .map(field -> {
                        checkState(field.getName().isPresent(), String.format("view '%s' columns must be named", name));
                        return new ViewDefinition.ViewColumn(field.getName().get(), field.getType());
                    })
                    .collect(toImmutableList());
        }
        catch (SemanticException e) {
            if (e.getCode() == SemanticErrorCode.MISSING_TABLE) {
                return null;
            }
            else {
                throw e;
            }
        }
        */
    }
}
