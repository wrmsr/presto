package com.wrmsr.presto.wrapper.util;

import com.google.common.collect.Lists;
import com.kenai.jffi.PageManager;
import com.kenai.jffi.Platform;
import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.provider.jffi.NativeLibrary;
import jnr.x86asm.Asm;
import jnr.x86asm.Assembler;
import jnr.x86asm.CPU;
import morgoth.common.NativeUtils;
import org.apache.commons.lang.ArrayUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


public class AtExitExec {

    private AtExitExec() {
    }

    public static abstract class AbstractAtExitExec extends Exec.AbstractExec {

        protected final int ptrSize = Platform.getPlatform().addressSize() >> 3;

        protected final NativeLibrary clib;
        protected final jnr.ffi.Runtime runtime;
        protected final PageManager pageManager;

        public AbstractAtExitExec() {
            clib = NativeUtils.newNativeLibrary(Lists.newArrayList("c"));
            runtime = jnr.ffi.Runtime.getSystemRuntime();
            pageManager = PageManager.getInstance();
        }

        public long getExecAddr() {
            return NativeUtils.getSymbolAddress(clib, "execve");
        }

        public Pointer allocAndWriteString(String str) {
            byte[] byteArr = ArrayUtils.add(str.getBytes(), (byte) 0);
            int numPages = (int) (pageManager.pageSize() % byteArr.length) + 1;
            Pointer base = Pointer.wrap(runtime, PageManager.getInstance().allocatePages(
                    numPages, NativeUtils.PROT_READ | NativeUtils.PROT_WRITE));
            base.put(0, byteArr, 0, byteArr.length);
            return base;
        }

        public void writeByteArrTable(Pointer base, byte[][] byteArrs) {
            long ofs = (byteArrs.length + 1) * ptrSize;
            for (int i = 0; i < byteArrs.length; ++i) {
                byte[] byteArr = byteArrs[i];
                base.putAddress(i * ptrSize, base.address() + ofs);
                base.put(ofs, byteArr, 0, byteArr.length);
                ofs += byteArr.length;
            }
            base.putAddress(byteArrs.length * ptrSize, 0L);
        }

        public Pointer allocAndWriteStringTable(String[] strs) {
            byte[][] byteArrs = new byte[strs.length][];
            long totalSize = (strs.length + 1) * ptrSize;
            for (int i = 0; i < strs.length; ++i) {
                byteArrs[i] = ArrayUtils.add(strs[i].getBytes(), (byte) 0);
                totalSize += byteArrs[i].length;
            }
            int numPages = (int) (pageManager.pageSize() % totalSize) + 1;
            Pointer base = Pointer.wrap(
                    runtime, PageManager.getInstance().allocatePages(
                            numPages, NativeUtils.PROT_READ | NativeUtils.PROT_WRITE));
            writeByteArrTable(base, byteArrs);
            return base;
        }
    }

    public static abstract class Abstratx64AtExitExec extends AbstractAtExitExec {

        public byte[] generateCallback(Pointer filename, Pointer argv, Pointer envp,
                                       Pointer execve, Pointer callback) {
            Assembler asm = new Assembler(CPU.X86_64);
            asm.mov(Asm.rdi, Asm.imm(filename.address()));
            asm.mov(Asm.rsi, Asm.imm(argv.address()));
            asm.mov(Asm.rdx, Asm.imm(envp.address()));
            asm.mov(Asm.r8, Asm.imm(execve.address()));
            asm.call(Asm.r8);
            asm.int3();

            ByteBuffer buf = ByteBuffer.allocate(asm.codeSize());
            asm.relocCode(buf, callback.address());
            buf.flip();
            buf.rewind();
            return buf.array();
        }
    }

    public static class MacOSx64AtExitExec extends Abstratx64AtExitExec {

        public static interface AtExitLibC {

            public int atexit(Pointer function);
        }

        @Override
        public void exec(String path, String[] params, @Nullable Map<String, String> env) throws IOException {
            Pointer filenamePtr = allocAndWriteString(path);
            Pointer paramsPtr = allocAndWriteStringTable(params);
            if (env == null)
                env = System.getenv();
            Pointer envPtr = allocAndWriteStringTable(convertEnv(env));

            Pointer callbackBase = Pointer.wrap(
                    runtime, PageManager.getInstance().allocatePages(
                            1, NativeUtils.PROT_READ | NativeUtils.PROT_WRITE | NativeUtils.PROT_EXEC));
            byte[] callbackByteArr = generateCallback(
                    filenamePtr, paramsPtr, envPtr, Pointer.wrap(runtime, getExecAddr()), callbackBase);
            callbackBase.put(0, callbackByteArr, 0, callbackByteArr.length);

            AtExitLibC libc = LibraryLoader.create(AtExitLibC.class).load("c");
            libc.atexit(Pointer.wrap(runtime, callbackBase.address()));
        }
    }

    public static class Linuxx64AtExitExec extends Abstratx64AtExitExec {

        public static interface AtExitLibC {

            public int __cxa_atexit(Pointer function, Pointer arg, Pointer dso_handle);
        }

        @Override
        public void exec(String path, String[] params, @Nullable Map<String, String> env) throws IOException {
            Pointer filenamePtr = allocAndWriteString(path);
            Pointer paramsPtr = allocAndWriteStringTable(params);
            if (env == null)
                env = System.getenv();
            Pointer envPtr = allocAndWriteStringTable(convertEnv(env));

            Pointer callbackBase = Pointer.wrap(
                    runtime, PageManager.getInstance().allocatePages(
                            1, NativeUtils.PROT_READ | NativeUtils.PROT_WRITE | NativeUtils.PROT_EXEC));
            byte[] callbackByteArr = generateCallback(
                    filenamePtr, paramsPtr, envPtr, Pointer.wrap(runtime, getExecAddr()), callbackBase);
            callbackBase.put(0, callbackByteArr, 0, callbackByteArr.length);

            AtExitLibC libc = LibraryLoader.create(AtExitLibC.class).load("c");
            libc.__cxa_atexit(Pointer.wrap(runtime, callbackBase.address()), Pointer.wrap(runtime, 0), Pointer.wrap(runtime, 0));
        }
    }
}
