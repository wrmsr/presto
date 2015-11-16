package com.wrmsr.presto.reactor;

import com.facebook.presto.sql.planner.plan.PlanNode;

public final class UnsupportedPlanNodeException
        extends RuntimeException
{
    private final PlanNode node;

    public UnsupportedPlanNodeException(PlanNode node)
    {
        this.node = node;
    }

    @Override
    public String toString()
    {
        return "UnsupportedPlanNodeException{" +
                "node=" + node +
                '}';
    }
}
