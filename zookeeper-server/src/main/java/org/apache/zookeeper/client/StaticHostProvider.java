/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.client;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Most simple HostProvider, resolves on every next() call.
 *
 * Please be aware that although this class doesn't do any DNS caching, there're multiple levels of caching already
 * present across the stack like in JVM, OS level, hardware, etc. The best we could do here is to get the most recent
 * address from the underlying system which is considered up-to-date.
 *
 */
@InterfaceAudience.Public
public final class StaticHostProvider implements HostProvider {
    public interface Resolver {
        InetAddress[] getAllByName(String name) throws UnknownHostException;
    }

    private static final Logger LOG = LoggerFactory
            .getLogger(StaticHostProvider.class);

    private final List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(5);

    private int lastIndex = -1;

    private int currentIndex = -1;

    private Resolver resolver;

    /**
     * Constructs a SimpleHostSet.
     *
     * @param serverAddresses
     *            possibly unresolved ZooKeeper server addresses
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses) {
        //resolver的默认实现
        this.resolver = new Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) throws UnknownHostException {
                return InetAddress.getAllByName(name);
            }
        };
        init(serverAddresses);
    }

    /**
     * Introduced for testing purposes. getAllByName() is a static method of InetAddress, therefore cannot be easily mocked.
     * By abstraction of Resolver interface we can easily inject a mocked implementation in tests.
     *
     * @param serverAddresses
     *            possibly unresolved ZooKeeper server addresses
     * @param resolver
     *            custom resolver implementation
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses, Resolver resolver) {
        this.resolver = resolver;
        init(serverAddresses);
    }

    /**
     * Common init method for all constructors.  初始化地址 ,解析所有未解析的服务地址,并把他们放入list中去 使用了 Collections.shuffle进行随机打散
     * Resolve all unresolved server addresses, put them in a list and shuffle.
     */
    private void init(Collection<InetSocketAddress> serverAddresses) {
        if (serverAddresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "A HostProvider may not be empty!");
        }

        this.serverAddresses.addAll(serverAddresses);
        Collections.shuffle(this.serverAddresses);
    }

    /**
     * Evaluate to a hostname if one is available and otherwise it returns the
     * string representation of the IP address.
     *
     * In Java 7, we have a method getHostString, but earlier versions do not support it.
     * This method is to provide a replacement for InetSocketAddress.getHostString().
     *
     * @param addr
     * @return Hostname string of address parameter
     */
    private String getHostString(InetSocketAddress addr) {
        String hostString = "";

        if (addr == null) {
            return hostString;
        }
        //如果没有解析
        if (!addr.isUnresolved()) {
            InetAddress ia = addr.getAddress();

            // If the string starts with '/', then it has no hostname
            // and we want to avoid the reverse lookup, so we return
            // the string representation of the address.
            if (ia.toString().startsWith("/")) {
                hostString = ia.getHostAddress();
            } else {
                hostString = addr.getHostName();
            }
        } else {
            // According to the Java 6 documentation, if the hostname is
            // unresolved, then the string before the colon is the hostname.
            String addrString = addr.toString();
            hostString = addrString.substring(0, addrString.lastIndexOf(':'));
        }

        return hostString;
    }

    public int size() {
        return serverAddresses.size();
    }

    /**
     * 获取下一个服务地址
     * @param spinDelay 延迟获取
     * @return
     */
    public InetSocketAddress next(long spinDelay) {
        //判断该当前服务地址
        currentIndex = ++currentIndex % serverAddresses.size();
        //如果当前的和上次的一样 就证明只有一个服务地  且需要延迟
        if (currentIndex == lastIndex && spinDelay > 0) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
            }
        } else if (lastIndex == -1) {
            // We don't want to sleep on the first ever connect attempt. 第一次初始化
            lastIndex = 0;
        }
        //获取服务地址
        InetSocketAddress curAddr = serverAddresses.get(currentIndex);
        try {
            //当前的ip或者域名
            String curHostString = getHostString(curAddr);
            //如果是ip直接返回,是域名需解析返回
            List<InetAddress> resolvedAddresses = new ArrayList<InetAddress>(Arrays.asList(this.resolver.getAllByName(curHostString)));
            //如果未找到对应的 返回serverAddress中的
            if (resolvedAddresses.isEmpty()) {
                return curAddr;
            }
            //随机打散
            Collections.shuffle(resolvedAddresses);
            //返回第一个
            return new InetSocketAddress(resolvedAddresses.get(0), curAddr.getPort());
        } catch (UnknownHostException e) {
            return curAddr;
        }
    }

    @Override
    public void onConnected() {
        lastIndex = currentIndex;
    }
}
