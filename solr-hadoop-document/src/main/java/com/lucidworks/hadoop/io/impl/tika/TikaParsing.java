package com.lucidworks.hadoop.io.impl.tika;

import com.lucidworks.hadoop.io.impl.LWSolrDocument;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

// basic tika parsing
public class TikaParsing {
  private transient static Logger log = LoggerFactory.getLogger(TikaParsing.class);

  private static Parser parser = new AutoDetectParser();
  private static ParseContext context = new ParseContext();

  public static void parseLWSolrDocument(LWSolrDocument document, byte[] data) {
    ContentHandler text = new BodyContentHandler();
    InputStream input = new ByteArrayInputStream(data);
    Metadata metadata = new Metadata();
    LinkContentHandler links = new LinkContentHandler();
    ContentHandler handler = new TeeContentHandler(links, text);

    try {
      parser.parse(input, handler, metadata, context);
    } catch (IOException e) {
      e.printStackTrace();
      log.warn("IOException " + document.getId());
    } catch (SAXException e) {
      e.printStackTrace();
      log.warn("SAXException " + document.getId());
    } catch (TikaException e) {
      e.printStackTrace();
      log.warn("TikaException " + document.getId());
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    if (metadata != null) {
      for (String name : metadata.names()) {
        String value = metadata.get(name);
        document.addField(name, value);
      }
    }
    if (text != null) {
      document.addField("body", text.toString());
    }
  }

}
