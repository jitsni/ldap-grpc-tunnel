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

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.SizeLimitExceededException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class LdapQuery {
    private static final Logger LOGGER = Logger.getLogger(LdapQuery.class.getName());

    private static final String CUSTOM_SOCKET_FACTORY_PROPERTY_NAME = "java.naming.ldap.factory.socket";

    private static final String USER_NODE_OBJECTCLASS_FILTER = "(objectClass=user)";
    private static final String DISTINGUISHED_NAME_ATTRIBUTE_NAME = "distinguishedName";
    private static final String LOGIN_NAME = "sAMAccountName";

    private static final int LDAP_QUERY_PAGE_SIZE = 1000;
    private final Properties properties;

    private String baseDn;
    private LdapContext dirContext;

    LdapQuery(Properties properties) {
        this.properties = properties;
    }

    void test() throws Exception {
        env();
        fetchUsers(dirContext, null);
    }

    void env() throws Exception {
        Hashtable<String, String> env = new Hashtable<>();

        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        String url = (String) properties.get("url");
        env.put(Context.PROVIDER_URL, url);
        env.put(Context.SECURITY_PRINCIPAL, (String) properties.get("user"));
        env.put(Context.SECURITY_CREDENTIALS, (String) properties.get("password"));
        baseDn = (String) properties.get("baseDn");

        if (url.startsWith("ldaps://") || url.startsWith("LDAPS://")) {
            env.put(CUSTOM_SOCKET_FACTORY_PROPERTY_NAME, GrpcSSLSocketFactory.class.getName());
        } else {
            env.put(CUSTOM_SOCKET_FACTORY_PROPERTY_NAME, GrpcSocketFactory.class.getName());
        }

        //initializing active directory LDAP connection
        dirContext = new InitialLdapContext(env, null);
    }

    private String[] getGroupNodeAttributes() {
        return new String[] { DISTINGUISHED_NAME_ATTRIBUTE_NAME, LOGIN_NAME };
    }

    private int fetchUsers(LdapContext ctx, String pattern) throws Exception {
        LOGGER.info("----------- USERS -----------");

        String filter;
        if (pattern != null) {
            String nameFilter = "(name=" + "*" + pattern + "*" + ")";
            filter = "(&" + USER_NODE_OBJECTCLASS_FILTER + nameFilter + ")";
        } else {
            filter = USER_NODE_OBJECTCLASS_FILTER;
        }

        SearchControls sc = new SearchControls(SearchControls.SUBTREE_SCOPE,
                0, 0, getGroupNodeAttributes(), false, false);

        return executeQuery(ctx, baseDn, filter, null, sc, null);
    }


    private int executeQuery(LdapContext ctx, String baseDN, String filter, Object[] filterArgs, SearchControls sc, Function<List<String>, Void> action)
            throws NamingException, IOException {


        int count = 0;
        byte[] cookie = null;
        ctx.setRequestControls(new Control[] {
                new PagedResultsControl(LDAP_QUERY_PAGE_SIZE, Control.CRITICAL)
        });
        List<String> entries = new ArrayList<>();

        do {
            NamingEnumeration<SearchResult> results = ctx.search(baseDN, filter, filterArgs, sc);
            try {
                while (results != null && results.hasMore()) {
                    SearchResult result = results.next();
                    LOGGER.info("result " + result);
                }
            } catch (PartialResultException | SizeLimitExceededException e) {
//                System.out.println("Got " + e);
            } finally {
                if (results != null) {
                    results.close();
                }
            }
            Control[] controls = ctx.getResponseControls();
            if (controls != null) {
                for (Control control : controls) {
                    if (control instanceof PagedResultsResponseControl) {
                        PagedResultsResponseControl prrc = (PagedResultsResponseControl) control;
                        cookie = prrc.getCookie();
                    }
                }
            }
            ctx.setRequestControls(new Control[] {
                    new PagedResultsControl(LDAP_QUERY_PAGE_SIZE, cookie, Control.CRITICAL)
            });
            if (action != null) {
                action.apply(entries);
                entries.clear();
            }
        } while (cookie != null);

        return count;
    }

    public void close() throws NamingException {
        if (dirContext != null) {
            dirContext.close();
        }
    }
}
