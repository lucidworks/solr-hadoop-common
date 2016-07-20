package com.lucidworks.hadoop.tika;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.lucidworks.hadoop.io.LWDocument;
import com.lucidworks.hadoop.io.LWDocumentProvider;
import com.lucidworks.hadoop.io.impl.LWSolrDocument;

import junit.framework.Assert;

public class TikaParsingTest {

  @Test
  public void test() throws Exception {
    LWSolrDocument doc = new LWSolrDocument();
    // load tika parsing
    String expected = "this is a String";
    doc.setContent(expected.getBytes());
    LWDocument[] docs = new TikaParsing().tikaParsing(doc);
    // one doc
    String body = (String) docs[0].getFirstFieldValue("body");
    Assert.assertTrue(expected.equals(body.trim()));
  }

  @Test
  public void testWithLoader() throws Exception {
    JobConf conf = new JobConf();
    conf.set(LWDocumentProvider.TIKA_PROCESS, "true");
    LWSolrDocument doc = new LWSolrDocument();
    // load tika parsing
    String expected = "this is a String";
    doc.setContent(expected.getBytes());
    LWDocument[] docs = LWDocumentProvider.processIfNeeded(doc, conf);

    // one doc
    String body = (String) docs[0].getFirstFieldValue("body");
    Assert.assertTrue(expected.equals(body.trim()));
  }
}

