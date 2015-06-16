package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.wrmsr.presto.util.ColumnDomain;

import java.util.LinkedHashMap;
import java.util.List;

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
        throw new UnsupportedOperationException();
    }
}
