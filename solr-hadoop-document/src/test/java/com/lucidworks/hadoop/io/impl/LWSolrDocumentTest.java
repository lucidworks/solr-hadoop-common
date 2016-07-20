package com.lucidworks.hadoop.io.impl;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class LWSolrDocumentTest {

  @Test(expected = RuntimeException.class)
  public void test() throws Exception {
    JobConf conf = new JobConf();
    conf.set(LWSolrDocument.TIKA_PROCESS, "true");
    // The ClassNotFoundException is thrown as a RuntimeException due hadoop
    new LWSolrDocument().configure(conf);
  }
}
