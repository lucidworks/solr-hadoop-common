package com.lucidworks.hadoop.io.impl;

public interface TikaProcess {
  public void parseLWSolrDocument(LWSolrDocument document, byte[] data);
}
