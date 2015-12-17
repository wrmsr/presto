/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.launcher.util;

import jnr.posix.POSIX;
import jnr.posix.util.Platform;

import java.io.File;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;

public class DaemonProcess
{
    public static final int LSB_NOT_RUNNING = 3;
    public static final int LSB_STATUS_UNKNOWN = 4;

    public final File path;
    public final int pidFile;
    public final POSIX posix;

    public boolean locked;

    public final int EWOULDBLOCK;
    public final int SIGTERM;
    public final int SIGKILL;

    public DaemonProcess(File path)
    {
        final int openFlags;
        if (Platform.IS_MAC) {
            openFlags =
                    jnr.constants.platform.darwin.OpenFlags.O_RDWR.intValue() |
                    jnr.constants.platform.darwin.OpenFlags.O_CREAT.intValue();
            EWOULDBLOCK = -1; // jnr.constants.platform.darwin.Errno.EWOULDBLOCK.intValue();
            SIGTERM = jnr.constants.platform.darwin.Signal.SIGTERM.intValue();
            SIGKILL = jnr.constants.platform.darwin.Signal.SIGKILL.intValue();
        }
        else if (Platform.IS_LINUX) {
            openFlags =
                    jnr.constants.platform.linux.OpenFlags.O_RDWR.intValue() |
                    jnr.constants.platform.linux.OpenFlags.O_CREAT.intValue();
            EWOULDBLOCK = jnr.constants.platform.linux.Errno.EWOULDBLOCK.intValue();
            SIGTERM = jnr.constants.platform.linux.Signal.SIGTERM.intValue();
            SIGKILL = jnr.constants.platform.linux.Signal.SIGKILL.intValue();
        }
        else {
            throw new IllegalStateException("Unsupported platform");
        }
        posix = POSIXUtils.getPOSIX();
        this.path = path.getAbsoluteFile();
        if (path.getParentFile() != null) {
            path.getParentFile().mkdirs();
        }
        pidFile = posix.open(this.path.getAbsolutePath(), openFlags, 0600);
        checkState(pidFile >= 0);
        refresh();
    }

    public synchronized void refresh()
    {
        int ret = posix.flock(pidFile, 2 | 4); // LOCK_EX | LOCK_NB
        if (ret == 0) {
            locked = true;
        } else if (ret == EWOULDBLOCK) {
            locked = false;
        }
        else {
            throw new RuntimeException("flock failed: " + ret);
        }
    }

    public synchronized int clearPid()
    {
        checkState(locked, "pid file not locked by us");
        // FIXME err check
        posix.lseek(pidFile, 0, 0);
        posix.ftruncate(pidFile, 0);
        return 0; // FIXME
    }

    public synchronized void writePid(int pid)
    {
        clearPid();
        byte[] bytes = String.format("%d\n", pid).getBytes();
        posix.write(pidFile, bytes, bytes.length);
        posix.fsync(pidFile);
    }

    public synchronized void writePid()
    {
        int pid = POSIXUtils.getPOSIX().getpid();
        checkState(pid > 0);
        writePid(pid);
    }

    public synchronized boolean alive()
    {
        refresh();
        if (locked) {
            return false;
        }
        int pid = readPid();
        try {
            posix.kill(pid, 0);
            return true;
        }
        catch (Exception e) {
            throw new IllegalStateException(String.format("Signaling pid %s failed: %s", pid, e));
        }
    }

    public synchronized int readPid()
    {
        refresh();
        checkState(!locked, "pid file is locked by us");
        posix.lseek(pidFile, 0, 0);
        byte[] buf = new byte[1024];
        int len = posix.read(pidFile, buf, buf.length);
        checkState(len > 0 && len < buf.length);
        int pid = Integer.valueOf(new String(Arrays.copyOf(buf, len)).trim());
        checkState(pid > 0);
        return pid;
    }

    public synchronized int kill(int signal)
    {
        int pid = readPid();
        return posix.kill(pid, signal);
    }

    public synchronized int stop()
    {
        return kill(SIGTERM);
    }

    public synchronized int kill()
    {
        return kill(SIGKILL);
    }
}
