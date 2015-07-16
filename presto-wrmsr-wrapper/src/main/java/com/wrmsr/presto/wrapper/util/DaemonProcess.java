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
package com.wrmsr.presto.wrapper.util;

import jnr.posix.POSIX;
import jnr.posix.util.Platform;

import java.io.File;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;

public class DaemonProcess
{
    private final File path;
    private final int pidFile;
    private final POSIX posix;

    private boolean locked;

    public DaemonProcess(File path)
    {
        final int openFlags;
        if (Platform.IS_MAC) {
            openFlags =
                    jnr.constants.platform.darwin.OpenFlags.O_RDWR.intValue() |
                    jnr.constants.platform.darwin.OpenFlags.O_CREAT.intValue();
        }
        else if (Platform.IS_LINUX) {
            openFlags =
                    jnr.constants.platform.linux.OpenFlags.O_RDWR.intValue() |
                    jnr.constants.platform.linux.OpenFlags.O_CREAT.intValue();
        }
        else {
            throw new IllegalStateException("Unsupported platform");
        }
        posix = POSIXUtils.getPOSIX();
        this.path = path;
        path.getParentFile().mkdirs();
        pidFile = posix.open(path.getAbsolutePath(), openFlags, 0600);
        refresh();
    }

    private void refresh()
    {
        try {
            posix.flock(pidFile, 2 | 4); // LOCK_EX | LOCK_NB
            locked = true;
        }
        catch (Exception e) {
            locked = false;
        }
    }

    private void clearPid()
    {
        checkState(locked, "pid file not locked by us");
        posix.lseek(pidFile, 0, 0);
        posix.ftruncate(pidFile, 0);
    }

    private void writePid(int pid)
    {
        clearPid();
        byte[] bytes = String.format("%d\n", pid).getBytes();
        posix.write(pidFile, bytes, bytes.length);
        posix.fsync(pidFile);
    }

    private boolean alive()
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

    private int readPid()
    {
        checkState(!locked, "pid file is locked by us");
        posix.lseek(pidFile, 0, 0);
        byte[] buf = new byte[1024];
        int len = posix.read(pidFile, buf, buf.length);
        checkState(len > 0 && len < buf.length);
        int pid = Integer.valueOf(new String(Arrays.copyOf(buf, len)));
        checkState(pid > 0);
        return pid;
    }
}
