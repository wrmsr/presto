package com.wrmsr.presto.reactor;

import org.testng.annotations.Test;

import java.util.List;

public class TestCrud
{
    public interface CrudConnectorAdapter
    {
        List select();

        void insert();

        void update();

        void delete();
    }

    public class CrudConnectorAdapterImpl implements CrudConnectorAdapter
    {
        @Override
        public List select()
        {
            return null;
        }

        @Override
        public void insert()
        {

        }

        @Override
        public void update()
        {

        }

        @Override
        public void delete()
        {

        }
    }

    @Test
    public void test() throws Throwable
    {

    }
}
