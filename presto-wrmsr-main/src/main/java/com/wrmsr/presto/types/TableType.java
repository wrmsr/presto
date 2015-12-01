package com.wrmsr.presto.types;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TableType
{
    public static final String NAME = "table";

    private final Type returnType;
    private final List<Type> argumentTypes;

    public TableType(List<Type> argumentTypes, Type returnType)
    {
        super(parameterizedTypeName(NAME, typeParameters(argumentTypes, returnType)), MethodHandle.class);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    private static TypeSignature[] typeParameters(List<Type> argumentTypes, Type returnType)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        argumentTypes.stream()
                .map(Type::getTypeSignature)
                .forEach(builder::add);
        builder.add(returnType.getTypeSignature());
        List<TypeSignature> signatures = builder.build();
        return signatures.toArray(new TypeSignature[signatures.size()]);
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.<Type>builder().addAll(argumentTypes).add(returnType).build();
    }

    @Override
    public String getDisplayName()
    {
        ImmutableList<String> names = getTypeParameters().stream()
                .map(Type::getDisplayName)
                .collect(toImmutableList());
        return "function<" + Joiner.on(",").join(names) + ">";
    }
}
