package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.ColumnDomain;
import com.wrmsr.presto.util.ImmutableCollectors;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkArgument;

@FunctionalInterface
public interface Partitioner
{
    class Partition
    {
        private final String prtitionId;
        private final TupleDomain<String> tupleDomain;

        public Partition(String prtitionId, TupleDomain<String> tupleDomain)
        {
            this.prtitionId = prtitionId;
            this.tupleDomain = tupleDomain;
        }

        public String getPrtitionId()
        {
            return prtitionId;
        }

        public TupleDomain<String> getTupleDomain()
        {
            return tupleDomain;
        }
    }

    List<Partition> getPartitionsConnector(SchemaTableName table, TupleDomain<String> tupleDomain);

    static List<Partition> generateDensePartitions(LinkedHashMap<String, ColumnDomain> columnDomains, int numPartitions)
    {
        checkArgument(columnDomains.size() == 1); // FIXME
        Map.Entry<String, ColumnDomain> e = columnDomains.entrySet().stream().findFirst().get();
        String column = e.getKey();
        ColumnDomain columnDomain = e.getValue();
        int base = (Integer) columnDomain.getMin();
        int step = ((Integer) columnDomain.getMax() - base) / numPartitions;
        return LongStream.range(0, numPartitions).boxed().map((i) -> new Partition(i.toString(), TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.create(SortedRangeSet.of(Range.range(i * step + base, true, (i + 1) * step + base, false)), false))))).collect(ImmutableCollectors.toImmutableList());
    }
}
