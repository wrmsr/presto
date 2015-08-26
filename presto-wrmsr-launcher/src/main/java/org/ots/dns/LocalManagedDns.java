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
// http://rkuzmik.blogspot.com/2006/08/local-managed-dns-java_11.html
package org.ots.dns;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sun.net.spi.nameservice.NameService;
import sun.net.spi.nameservice.dns.DNSNameService;

/**
 *
 * @version $Id$
 * @author Roman Kuzmik
 */
public class LocalManagedDns implements NameService {
    private static final Log log = LogFactory.getLog(LocalManagedDns.class);

    private final NameService defaultDnsImpl;

    public LocalManagedDns()
    {
        try {
            this.defaultDnsImpl = new DNSNameService();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * @see sun.net.spi.nameservice.NameService#getHostByAddr(byte[])
     */
    @Override
    public String getHostByAddr(byte[] ip) throws UnknownHostException {
        log.debug("");

        return defaultDnsImpl.getHostByAddr(ip);
    }

    /**
     * @see sun.net.spi.nameservice.NameService#lookupAllHostAddr(java.lang.String)
     */
    /*
    public byte[][] lookupAllHostAddr(String name) throws UnknownHostException {
        log.debug("");

        String ipAddress = NameStore.getInstance().get(name);
        if (!StringUtils.isEmpty(ipAddress)){
            log.debug("\tmatch");
            byte[] ip = Util.textToNumericFormat(ipAddress);
            return new byte[][]{ip};
        } else {
            log.debug("\tmiss");
            return defaultDnsImpl.lookupAllHostAddr(name);
        }
    }
    */

    @Override
    public InetAddress[] lookupAllHostAddr(String s)
            throws UnknownHostException
    {
        String ipAddress = NameStore.getInstance().get(s);
        if (!StringUtils.isEmpty(ipAddress)){
            log.debug("\tmatch");
            return InetAddress.getAllByName(ipAddress);
        } else {
            log.debug("\tmiss");
            return defaultDnsImpl.lookupAllHostAddr(s);
        }
    }
}
