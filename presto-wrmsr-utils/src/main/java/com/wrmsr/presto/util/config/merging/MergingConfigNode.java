package com.wrmsr.presto.util.config.merging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wrmsr.presto.util.Mergeable;
import com.wrmsr.presto.util.Serialization;

import java.util.Map;

public interface MergingConfigNode<N extends MergingConfigNode> extends Mergeable<N>
{
    @SuppressWarnings({"unchecked"})
    default N merge(N other)
    {
        ObjectMapper mapper = Serialization.OBJECT_MAPPER.get();
        Map newMap = Serialization.roundTrip(mapper, this, Map.class);
        Map<Object, Object> otherMap = Serialization.roundTrip(mapper, other, Map.class);
        for (Map.Entry<Object, Object> entry : otherMap.entrySet()) {
            otherMap.put(entry.getKey(), entry.getValue());
        }
        return Serialization.roundTrip(mapper, otherMap, (Class<N>) getClass());
    }
}
