package com.wrmsr.presto.util.config.mergeable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wrmsr.presto.util.Mergeable;
import com.wrmsr.presto.util.Serialization;

import java.util.Map;

public interface MergeableConfigNode<N extends MergeableConfigNode>
        extends Mergeable
{
    @SuppressWarnings({"unchecked"})
    @Override
    default Mergeable merge(Mergeable other)
    {
        ObjectMapper mapper = Serialization.OBJECT_MAPPER.get();
        Map newMap = Serialization.roundTrip(mapper, this, Map.class);
        Map<Object, Object> otherMap = Serialization.roundTrip(mapper, other, Map.class);
        for (Map.Entry<Object, Object> entry : otherMap.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue());
        }
        return Serialization.roundTrip(mapper, otherMap, (Class<N>) getClass());
    }
}
