package com.lucidworks.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Security {
  public static final String LWW_JAAS_FILE = "lww.jaas.file";
  public static final String LWW_JAAS_APPNAME = "lww.jaas.appname";

  private static Logger log = LoggerFactory.getLogger(Security.class);

  public static void setSecurityConfig(Configuration job) {
    final String jaasFile = job.get(LWW_JAAS_FILE);
    if (jaasFile != null) {
      log.debug("Using kerberized Solr.");
      System.setProperty("java.security.auth.login.config", jaasFile);
      final String appname = job.get(LWW_JAAS_APPNAME, "Client");
      System.setProperty("solr.kerberos.jaas.appname", appname);
      HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
    }
  }
}
