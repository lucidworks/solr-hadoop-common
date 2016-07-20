package com.lucidworks.hadoop.tika;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.impl.LWSolrDocument;

import junit.framework.Assert;

public class TikaParsingTest {

  @Test
  public void test() throws Exception {
    JobConf conf = new JobConf();
    conf.set(LWSolrDocument.TIKA_PROCESS, "true");
    LWSolrDocument doc = new LWSolrDocument();
    // load tika parsing
    doc.configure(conf);

    String expected = "this is a String";
    doc.addField(LWSolrDocument.RAW_CONTENT, expected.getBytes());

    LWDocument[] docs = doc.process();
    // one doc
    String body = (String) docs[0].getFirstFieldValue("body");
    Assert.assertTrue(expected.equals(body.trim()));
  }
}

