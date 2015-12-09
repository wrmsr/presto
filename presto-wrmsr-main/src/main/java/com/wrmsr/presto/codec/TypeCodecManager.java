package com.wrmsr.presto.codec;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.type.ParametricType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class TypeCodecManager
{
    private final Set<TypeCodec> initialTypeCodecs;
    private final Metadata metadata;
    private final TypeRegistry typeRegistry;

    private final Map<String, TypeCodec> typeCodecs = new MapMaker().makeMap();

    @Inject
    public TypeCodecManager(Set<TypeCodec> initialTypeCodecs, Metadata metadata, TypeRegistry typeRegistry)
    {
        this.initialTypeCodecs = initialTypeCodecs;
        this.metadata = metadata;
        this.typeRegistry = typeRegistry;
    }

    @PostConstruct
    private void registerInitialTypeCodecs()
    {
        for (TypeCodec tc : initialTypeCodecs) {
            addTypeCodec(tc);
        }
    }

    public synchronized void addTypeCodec(TypeCodec typeCodec)
    {
        checkArgument(!typeCodecs.containsKey(typeCodec.getName()));
        ParametricType pt = new EncodedParametricType(typeCodec);
        typeRegistry.addParametricType(pt);
        SqlFunction ef = new EncodeFunction(typeCodec);
        SqlFunction df = new DecodeFunction(typeCodec);
        SqlFunction cf = new VarbinaryToEncodedCast(typeCodec);
        metadata.addFunctions(ImmutableList.of(ef, df, cf));
        typeCodecs.put(typeCodec.getName(), typeCodec);
    }

    public Optional<TypeCodec> getTypeCodec(String name)
    {
        throw new UnsupportedOperationException();
    }
}
