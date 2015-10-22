package com.lucidworks.hadoop.cache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//This class provides methods to access the distributed cache
@SuppressWarnings("deprecation")
public class DistributedCacheHandler {
  private transient static Logger log = LoggerFactory.getLogger(DistributedCacheHandler.class);

  // Adding files to Cache
  public static void addFileToCache(JobConf conf, Path path, String propertyName) throws Exception {
    log.info("Adding {} to cache w/ name: {}", path, propertyName);
    try {
      FileSystem fileSystem = path.getFileSystem(conf);
      if (fileSystem instanceof LocalFileSystem) {
        FileSystem defFS = FileSystem.get(conf);
        Path dst = new Path("/tmp/" + path.getName());
        try {
          defFS.mkdirs(new Path("/tmp"));// just in case
        } catch (IOException e) {
        }
        defFS.copyFromLocalFile(path, dst);
        conf.set(propertyName, dst.toUri().toString());
        DistributedCache.addCacheFile(dst.toUri(), conf);
      } else {
        conf.set(propertyName, path.toUri().toString());
        DistributedCache.addCacheFile(path.toUri(), conf);
      }

    } catch (Exception e) {
      log.error("Error while creating distributed cache", e);
    }
  }

  private static void addFileToCache(JobConf conf, Path path, FileSystem fs, String nameToStore) {
    conf.set(nameToStore, path.getName());
    log.info("storing in conf:\n key: " + nameToStore + "\nvalue: " + path.getName());
    try {
      DistributedCache.addFileToClassPath(path, conf, fs);
    } catch (Exception e) {
      log.error("Error while creating distributed cache", e);
    }
    log.info("File added to cache " + path.toString());
  }

  // Reading files from Cache
  public static String getFileFromCache(JobConf conf, String fileName) {
    BufferedReader fis = null;
    StringBuilder response = new StringBuilder();
    try {
      for (Path p : DistributedCache.getLocalCacheFiles(conf)) {
        String path = p.toString();
        if (conf.get(fileName).contains(p.getName())) {
          String line = null;
          fis = new BufferedReader(new FileReader(p.toString()));
          while ((line = fis.readLine()) != null) {
            line = line.trim();
            response.append(line).append('\n');
          }
        }
      }
      return response.toString();
    } catch (Exception e) {
      log.error("Error reading distributed cache, " + e.getMessage(), e);
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          log.error("Exception", e);
        }
      }
    }
    return null;
  }

}