package com.wrmsr.presto.cluster;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.leacox.process.FinalizedProcess;
import com.leacox.process.FinalizedProcessBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class SshRemoteRunner
        extends RemoteRunner
{
    private List<String> getTargetArgs(Target target)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (target.getAuth() instanceof IdentityFileAuth) {
            builder.add("-i", ((IdentityFileAuth) target.getAuth()).getIdentityFile().getAbsolutePath());
        }
        else {
            throw new IllegalArgumentException(Objects.toString(target.getAuth()));
        }
        return builder.build();
    }

    private final long timeout;

    public SshRemoteRunner(long timeout)
    {
        this.timeout = timeout;
    }

    public void syncDirectoriesScp(Target target, File local, String remote)
    {
        checkArgument(!remote.contains("..") && !remote.startsWith("/"));
        runCommand(target, "rm", "-rf", remote);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("scp", "-r");
        builder.addAll(getTargetArgs(target));
        builder.add(local.getAbsolutePath());
        builder.add(String.format("%s@%s:%s", target.getUser(), target.getHost(), remote));

        FinalizedProcessBuilder pb = new FinalizedProcessBuilder(builder.build())
                .gobbleInputStream(true)
                .gobbleErrorStream(true);
        try (FinalizedProcess process = pb.start()) {
            if (process.waitFor(timeout) != 0) {
                throw new RuntimeException("Failed to send file");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void syncDirectoriesRsync(Target target, File local, String remote)
    {
        checkArgument(!remote.contains("..") && !remote.startsWith("/"));
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("rsync", "-r");
        builder.addAll(getTargetArgs(target));
        builder.add(local.getAbsolutePath());
        builder.add(String.format("%s@%s:%s", target.getUser(), target.getHost(), remote));

        FinalizedProcessBuilder pb = new FinalizedProcessBuilder(builder.build())
                .gobbleInputStream(true)
                .gobbleErrorStream(true);
        try (FinalizedProcess process = pb.start()) {
            if (process.waitFor(timeout) != 0) {
                throw new RuntimeException("Failed to send file");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void syncDirectories(Target target, File local, String remote)
    {
        syncDirectoriesScp(target, local, remote);
    }

    @Override
    public int runCommand(Target target, String command, String... args)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("ssh");
        builder.add(String.format("%s@%s", target.getUser(), target.getHost()));
        builder.addAll(getTargetArgs(target));
        builder.add(command);
        builder.addAll(Arrays.asList(args));

        FinalizedProcessBuilder pb = new FinalizedProcessBuilder(builder.build())
                .gobbleInputStream(true)
                .gobbleErrorStream(true);
        try (FinalizedProcess process = pb.start()) {
            return process.waitFor(timeout);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int runCommand(Handler handler, Target target, String command, String... args)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("ssh");
        builder.add(String.format("%s@%s", target.getUser(), target.getHost()));
        builder.addAll(getTargetArgs(target));
        builder.add(command);
        builder.addAll(Arrays.asList(args));

        FinalizedProcessBuilder pb = new FinalizedProcessBuilder(builder.build());
        try (FinalizedProcess process = pb.start()) {
            handler.handle(process.getOutputStream(), process.getInputStream(), process.getErrorStream());
            return process.waitFor(timeout);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        new SshRemoteRunner(60 * 1000).runCommand(new Target("dev8-devc", 22, "wtimoney", new IdentityFileAuth(new File(System.getProperty("user.home") + "/.ssh/id_rsa"))), "echo", "hi");
    }
}
