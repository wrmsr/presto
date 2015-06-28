package com.wrmsr.presto.functions;

import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;

public class PropertiesFunction
    extends StringVarargsFunction
{
    private final TypeManager typeManager;

    public PropertiesFunction(TypeManager typeManager)
    {
        super(
                "properties",
                "create a new properties map",
                ImmutableList.of(),
                2,
                "properties",
                "newProperties",
                ImmutableList.of(Context.class)
        );
        this.typeManager = typeManager;
    }

    private static class Context
    {
        private final TypeManager typeManager;

        public Context(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }
    }

    @Override
    protected MethodHandle bindMethodHandle()
    {
        return super.bindMethodHandle().bindTo(new Context(typeManager));
    }

    public static Slice newProperties(Context context, Object... strs)
    {
        checkArgument(strs.length % 2 == 0);
        List<StructManager.StructDefinition.Field> fields = newArrayList();
        for (int i = 0; i < strs.length; i += 2) {
            fields.add(new StructManager.StructDefinition.Field(((Slice) strs[i]).toStringUtf8(), ((Slice) strs[i + 1]).toStringUtf8()));
        }
        StructManager.StructDefinition def = new StructManager.StructDefinition(name.toStringUtf8(), fields);
        context.structManager.registerStruct(context.structManager.buildRowType(def));
        return name;
    }
}
