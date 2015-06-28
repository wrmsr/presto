package com.wrmsr.presto.functions;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;

public class DefineStructFunction
        extends StringVarargsFunction
{
    private final StructManager structManager;

    public DefineStructFunction(StructManager structManager)
    {
        super(
                "define_struct",
                "define a new struct type",
                ImmutableList.of("varchar"),
                2,
                "varchar",
                "defineStruct",
                ImmutableList.of(DefineStructContext.class, Slice.class)
        );
        this.structManager = structManager;
    }

    private static class DefineStructContext
    {
        private final StructManager structManager;

        public DefineStructContext(StructManager structManager)
        {
            this.structManager = structManager;
        }
    }

    @Override
    protected MethodHandle bindMethodHandle()
    {
        return super.bindMethodHandle().bindTo(new DefineStructContext(structManager));
    }

    public static Slice defineStruct(DefineStructContext context, Slice name, Object... strs)
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
