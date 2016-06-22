package com.lucidworks.hadoop.fusion;

/**
 * Fusion constants
 */
public final class Constants {
  private Constants(){} // can't construct
  public static final String FUSION_AUTHENABLED = "fusion.authenabled";
  public static final String FUSION_USER = "fusion.user";
  public static final String FUSION_PASS = "fusion.pass";
  public static final String FUSION_REALM = "fusion.realm";
  public static final String FUSION_INDEX_ENDPOINT = "fusion.endpoints";
  public static final String FUSION_QUERY_ENDPOINT = "fusion.query.endpoints";
  public static final String FUSION_QUERY = "fusion.query";

  // Kerberos
  public static final String FUSION_LOGIN_CONFIG = "java.security.auth.login.config";
  public static final String FUSION_LOGIN_APP_NAME = "fusion.jaas.appname";

}
