package com.wrmsr.presto.reactor;

import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;

public class InsertRewriter
        extends DefaultTraversalVisitor<Void, Void>
{
    private final Analysis analysis;

    public InsertRewriter(Analysis analysis)
    {
        this.analysis = analysis;
    }
}
