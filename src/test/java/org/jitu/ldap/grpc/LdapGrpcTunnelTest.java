/**
 * Copyright 2022 Jitendra Kotamraju.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jitu.ldap.grpc;

import java.util.Properties;

/*
 * @author Jitendra Kotamraju
 */
public class LdapGrpcTunnelTest {

    public static void main(String ... args) throws Exception {
        Properties properties = new Properties();
        properties.load(LdapGrpcTunnelTest.class.getResourceAsStream("env.properties"));
        System.out.println(properties);

        LdapProxyGrpcServer ldapProxyGrpcServer = new LdapProxyGrpcServer();
        ldapProxyGrpcServer.start();

        LdapProxyGrpcClient ldapProxyGrpcClient = new LdapProxyGrpcClient();
        ldapProxyGrpcClient.start();

        // wait until grpc client and server come up
        Thread.sleep(2000);

        LdapQuery query = new LdapQuery(properties);
        query.test();
    }

}
