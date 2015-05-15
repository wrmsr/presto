package com.wrmsr.presto.decoder;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;

public class TestDecoderPlugin
        extends AbstractTestQueryFramework
{
    public TestMaterializerPlugin()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testSanity()
            throws Exception
    {
        queryRunner.execute("select * from lineitem inner join orders on orders.orderkey = lineitem.orderkey inner join customer on orders.custkey = customer.custkey limit 10");
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

        LocalQueryRunner queryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        queryRunner.createCatalog(
                defaultSession.getCatalog(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        DecoderPlugin plugin = new DecoderPlugin();
        plugin.setTypeManager(queryRunner.getTypeManager());
        /*
        for (Type type : plugin.getServices(Type.class)) {
            queryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getServices(ParametricType.class)) {
            queryRunner.getTypeManager().addParametricType(parametricType);
        }
        */
        // queryRunner.getMetadata().getFunctionRegistry().addFunctions(Iterables.getOnlyElement(plugin.getServices(FunctionFactory.class)).listFunctions());

        return queryRunner;
    }
}
