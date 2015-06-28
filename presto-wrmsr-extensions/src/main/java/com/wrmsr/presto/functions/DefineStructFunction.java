package com.wrmsr.presto.functions;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.Lists.listOf;

public class DefineStructFunction
            extends ParametricScalar
{
    private static final Signature SIGNATURE = new Signature("define_struct", ImmutableList.of(comparableTypeParameter("varchar"), typeParameter("E")), "varchar", ImmutableList.of("varchar", "E"), true, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(DefineStructFunction.class, "defineStruct", DefineStructContext.class, Slice.class, Object[].class);

    private final StructManager structManager;

    public DefineStructFunction(StructManager structManager)
    {
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
    public Signature getSignature()
                               {
                                  return SIGNATURE;
                                                                         }

    @Override
    public boolean isHidden()
                         {
                            return false;
                                                         }

    @Override
    public boolean isDeterministic()
                                {
                                   return true;
                                                                      }

    @Override
    public String getDescription()
    {
        return "define a new struct type";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        checkArgument(type.getJavaType() == Slice.class);
        checkArgument(arity % 2 == 1);
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        builder.add(Slice.class);
        for (int i = 1; i < arity; i++) {
            builder.add(type.getJavaType());
        }

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(new DefineStructContext(structManager)).asVarargsCollector(Object[].class);
        return new FunctionInfo(
                new Signature(
                        "define_struct",
                        parseTypeSignature(StandardTypes.VARCHAR),
                        listOf(arity, type.getTypeSignature())),
                getDescription(),
                isHidden(),
                methodHandle,
                isDeterministic(),
                false,
                listOf(arity, false));
    }

    public static Slice defineStruct(DefineStructContext context, Slice name, Object... strs)
    {
        checkArgument(strs.length % 2 == 0);
        List<StructManager.StructDefinition.Field> fields = newArrayList();
        for (int i = 0; i < strs.length; i += 2) {
            fields.add(new StructManager.StructDefinition.Field(((Slice) strs[i]).toStringUtf8(), ((Slice) strs[i+1]).toStringUtf8()));
        }
        StructManager.StructDefinition def = new StructManager.StructDefinition(name.toStringUtf8(), fields);
        context.structManager.registerStruct(context.structManager.buildRowType(def));
        return name;
    }
}
