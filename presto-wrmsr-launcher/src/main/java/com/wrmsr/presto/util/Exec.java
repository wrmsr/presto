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
/**
 * This program will demonstrate remote exec.
 * $ CLASSPATH=.:../build javac Exec.java
 * $ CLASSPATH=.:../build java Exec
 * You will be asked username, hostname, displayname, passwd and command.
 * If everything works fine, given command will be invoked
 * on the remote side and outputs will be printed out.
 */
package com.wrmsr.presto.util;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface Exec
{
    void exec(String path, String[] params, Map<String, String> env) throws IOException;

    default void exec(String path, String[] params) throws IOException
    {
        exec(path, params, null);
    }

    abstract class AbstractExec implements Exec
    {
        public static String[] convertEnv(Map<String, String> env)
        {
            if (env == null) {
                return null;
            }
            ArrayList<String> ret = Lists.newArrayList(Iterables.transform(
                    env.entrySet(), entry -> String.format("%s=%s", entry.getKey(), entry.getValue())));
            return ret.toArray(new String[ret.size()]);
        }
    }

    class ProcessBuilderExec extends AbstractExec
    {

        public static final Method environment;

        static {
            try {
                environment = ProcessBuilder.class.getDeclaredMethod("environment", String[].class);
                environment.setAccessible(true);

            }
            catch (NoSuchMethodException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void exec(String path, String[] params, Map<String, String> env) throws IOException
        {
            ProcessBuilder pb = new ProcessBuilder();
            String[] envArr = convertEnv(env);
            try {
                environment.invoke(pb, new Object[]{envArr});
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            List<String> command = Lists.newArrayList(path);
            command.addAll(Arrays.asList(params));
            pb.command(command);
            pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process process = pb.start();
            int ret;
            try {
                ret = process.waitFor();
            }
            catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            System.exit(ret);
        }
    }

    static void main(String[] arg)
    {
        try {
            JSch jsch = new JSch();

            String host = null;
            if (arg.length > 0) {
                host = arg[0];
            }
            else {
                // host = JOptionPane.showInputDialog("Enter username@hostname",
                //         System.getProperty("user.name") + "@localhost"
                // );
                host = "ec2-user@10.40.14.217";
            }
            String user = host.substring(0, host.indexOf('@'));
            host = host.substring(host.indexOf('@') + 1);
            jsch.addIdentity(System.getProperty("user.home") + "/wtimoney_dev.pem");

            Session session = jsch.getSession(user, host, 22);

            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            /*

http://www.jcraft.com/jsch/examples/

      String config =
        "Port 22\n"+
        "\n"+
        "Host foo\n"+
        "  User "+user+"\n"+
        "  Hostname "+host+"\n"+
        "Host *\n"+
        "  ConnectTime 30000\n"+
        "  PreferredAuthentications keyboard-interactive,password,publickey\n"+
        "  #ForwardAgent yes\n"+
        "  #StrictHostKeyChecking no\n"+
        "  #IdentityFile ~/.ssh/id_rsa\n"+
        "  #UserKnownHostsFile ~/.ssh/known_hosts";

      System.out.println("Generated configurations:");
      System.out.println(config);

      ConfigRepository configRepository =
        com.jcraft.jsch.OpenSSHConfig.parse(config);
        //com.jcraft.jsch.OpenSSHConfig.parseFile("~/.ssh/config");

      jsch.setConfigRepository(configRepository);
             */

            /*
            session.setConfig("cipher.s2c", "aes128-cbc,3des-cbc,blowfish-cbc");
            session.setConfig("cipher.c2s", "aes128-cbc,3des-cbc,blowfish-cbc");
            session.setConfig("CheckCiphers", "aes128-cbc");
            */

            /*
            String xhost="127.0.0.1";
            int xport=0;
            String display=JOptionPane.showInputDialog("Enter display name",
                                                       xhost+":"+xport);
            xhost=display.substring(0, display.indexOf(':'));
            xport=Integer.parseInt(display.substring(display.indexOf(':')+1));
            session.setX11Host(xhost);
            session.setX11Port(xport+6000);
            */

            // username and password will be given via UserInfo interface.
            UserInfo ui = new MyUserInfo();
            session.setUserInfo(ui);
            session.connect();

            String command = JOptionPane.showInputDialog("Enter command", "set|grep SSH");

            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            // X Forwarding
            // channel.setXForwarding(true);

            //channel.setInputStream(System.in);
            channel.setInputStream(null);

            //channel.setOutputStream(System.out);

            //FileOutputStream fos=new FileOutputStream("/tmp/stderr");
            //((ChannelExec)channel).setErrStream(fos);
            ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();

            channel.connect();

            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) {
                        break;
                    }
                    System.out.print(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if (in.available() > 0) {
                        continue;
                    }
                    System.out.println("exit-status: " + channel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                }
                catch (Exception ee) {
                }
            }
            channel.disconnect();
            session.disconnect();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    class MyUserInfo implements UserInfo, UIKeyboardInteractive
    {
        public String getPassword()
        {
            return passwd;
        }

        public boolean promptYesNo(String str)
        {
            Object[] options = {"yes", "no"};
            int foo = JOptionPane.showOptionDialog(null,
                    str,
                    "Warning",
                    JOptionPane.DEFAULT_OPTION,
                    JOptionPane.WARNING_MESSAGE,
                    null, options, options[0]);
            return foo == 0;
        }

        String passwd;
        JTextField passwordField = (JTextField) new JPasswordField(20);

        public String getPassphrase()
        {
            return null;
        }

        public boolean promptPassphrase(String message)
        {
            return true;
        }

        public boolean promptPassword(String message)
        {
            Object[] ob = {passwordField};
            int result =
                    JOptionPane.showConfirmDialog(null, ob, message,
                            JOptionPane.OK_CANCEL_OPTION);
            if (result == JOptionPane.OK_OPTION) {
                passwd = passwordField.getText();
                return true;
            }
            else {
                return false;
            }
        }

        public void showMessage(String message)
        {
            JOptionPane.showMessageDialog(null, message);
        }

        final GridBagConstraints gbc =
                new GridBagConstraints(0, 0, 1, 1, 1, 1,
                        GridBagConstraints.NORTHWEST,
                        GridBagConstraints.NONE,
                        new Insets(0, 0, 0, 0), 0, 0);
        private Container panel;

        public String[] promptKeyboardInteractive(String destination,
                                                  String name,
                                                  String instruction,
                                                  String[] prompt,
                                                  boolean[] echo)
        {
            panel = new JPanel();
            panel.setLayout(new GridBagLayout());

            gbc.weightx = 1.0;
            gbc.gridwidth = GridBagConstraints.REMAINDER;
            gbc.gridx = 0;
            panel.add(new JLabel(instruction), gbc);
            gbc.gridy++;

            gbc.gridwidth = GridBagConstraints.RELATIVE;

            JTextField[] texts = new JTextField[prompt.length];
            for (int i = 0; i < prompt.length; i++) {
                gbc.fill = GridBagConstraints.NONE;
                gbc.gridx = 0;
                gbc.weightx = 1;
                panel.add(new JLabel(prompt[i]), gbc);

                gbc.gridx = 1;
                gbc.fill = GridBagConstraints.HORIZONTAL;
                gbc.weighty = 1;
                if (echo[i]) {
                    texts[i] = new JTextField(20);
                }
                else {
                    texts[i] = new JPasswordField(20);
                }
                panel.add(texts[i], gbc);
                gbc.gridy++;
            }

            if (JOptionPane.showConfirmDialog(null, panel,
                    destination + ": " + name,
                    JOptionPane.OK_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE)
                    == JOptionPane.OK_OPTION) {
                String[] response = new String[prompt.length];
                for (int i = 0; i < prompt.length; i++) {
                    response[i] = texts[i].getText();
                }
                return response;
            }
            else {
                return null;  // cancel
            }
        }
    }
}
