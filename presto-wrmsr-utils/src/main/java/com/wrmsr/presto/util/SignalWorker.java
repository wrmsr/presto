package com.wrmsr.presto.util;

import java.util.concurrent.locks.LockSupport;

/**
 * Simple wakeable / stoppable thread worker. Will invoke onSignal upon receiving signals but not
 * once per signal -- duplicate signals are ignored.
 */
public abstract class SignalWorker
{
    public final long sleepMillis;
    public final Thread thread;

    protected final Object sleepObject = new Object();
    protected volatile boolean shouldRun = true;

    protected SignalWorker(long sleepMillis)
    {
        this.sleepMillis = sleepMillis;
        thread = new Thread()
        {
            @Override
            public void run()
            {
                threadProc();
            }
        };
    }

    protected SignalWorker()
    {
        this(0);
    }

    public void start()
    {
        thread.start();
    }

    public boolean isAlive()
    {
        return thread.isAlive();
    }

    protected void threadProc()
    {
        while (shouldRun) {
            LockSupport.park();
            onSignal();
        }
    }

    protected abstract void onSignal();

    protected void sleep()
    {
        if (sleepMillis < 1) {
            return;
        }
        try {
            synchronized (sleepObject) {
                sleepObject.wait(sleepMillis);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void signal()
    {
        LockSupport.unpark(thread);
    }

    public synchronized void stop(long millis)
    {
        if (!thread.isAlive()) {
            return;
        }
        shouldRun = false;
        LockSupport.unpark(thread);
        synchronized (sleepObject) {
            sleepObject.notifyAll();
        }
        try {
            thread.join(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop()
    {
        stop(0);
    }
}
