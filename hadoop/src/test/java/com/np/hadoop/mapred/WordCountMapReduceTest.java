package com.np.hadoop.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.welflex.hadoop.hdfs.HdfsService;
import com.welflex.hadoop.hdfs.HdfsService.ReadCallback;
import com.welflex.hadoop.hdfs.HdfsServiceImpl;

public class WordCountMapReduceTest {
  private static final String INPUT_HDFS_FILE = "/users/junittest/comments";

  private static final String OUTPUT_HDFS_DIR = "/users/junittest/wcresults";
  private static Map<String, String> EXPECTED_COUNT_MAP 
    = new HashMap<String, String>();
  
  static {
    EXPECTED_COUNT_MAP.put("2012", "1828");
    EXPECTED_COUNT_MAP.put("bond", "4490");
    EXPECTED_COUNT_MAP.put("cave", "2769");
    EXPECTED_COUNT_MAP.put("james", "3631");
    EXPECTED_COUNT_MAP.put("walther", "921");
  };
  
  HdfsService hdfsAdminService = null;

  @Before
  public void before() throws Exception {
    hdfsAdminService = new HdfsServiceImpl();
    File file = new File(WordCountMapReduceTest.class.getResource("/comments").toString());
    String path = StringUtils.strip(file.getPath(), "file:");
    FileSystem hdfs = hdfsAdminService.getFileSystem();
    hdfs.copyFromLocalFile(false, true, new Path(path), new Path(INPUT_HDFS_FILE));
    hdfs.delete(new Path(OUTPUT_HDFS_DIR), true);
  }
  
  @After
  public void after() throws Exception {
    hdfsAdminService.close();
  }
  
  @Test
  public void javaMapReduce() throws Exception {
     assertTrue(new CommentWordMapReduce().mapred(INPUT_HDFS_FILE, OUTPUT_HDFS_DIR));
     validateResult();
  }
  
  private void validateResult() {
    FileStatus[] fsStatus = hdfsAdminService.list(OUTPUT_HDFS_DIR);
    assertTrue(fsStatus.length > 0);

    for (FileStatus status : fsStatus) {
      if (status.getPath().toString().endsWith("_SUCCESS")) {
        continue;
      }
      
      Map<String, String> actual = hdfsAdminService.read(new ReadCallback<Map<String, String>>() {

        @Override
        public Map<String, String> read(BufferedReader reader) throws IOException {
          Map<String, String> results = new HashMap<String, String>();
          String line = null;
          while ((line = reader.readLine()) != null) {
            String keyVal[] = StringUtils.split(line);
            results.put(keyVal[0], keyVal[1]);
          }
          return results;
        }
        
      }, status.getPath());
      System.out.println("Reduced Map:" + actual);
      assertEquals(EXPECTED_COUNT_MAP, actual);
    }
  }
  
  private Set<String> getJarsForPig() {
    Set<String> jars = new HashSet<String>();
    String path = System.getProperty("maven.surefire.classpath") != null ? System.getProperty("maven.surefire.classpath")
        : System.getProperty("java.class.path");

    String[] classpathJars = StringUtils.split(path, ":");

    for (String c : classpathJars) {
      if (!c.endsWith(".jar") || c.endsWith("tools.jar")) {
        continue;
      }
      jars.add(c);
    }
    return jars;
  }

  
  @Test
  public void pigMapReduce() throws Exception {
    Set<String> jars = getJarsForPig();

    // Set ExecType.MAPREDUCE if you want to run non local
    PigServer pigServer = new PigServer(ExecType.MAPREDUCE);

    for (String jar : jars) {
      // Register the jars for Pig      
      pigServer.registerJar(jar);
    }
    pigServer.registerScript(WordCountMapReduceTest.class.getResource("/wordcount.pig").toURI().toURL().getFile());
    // Post validation if any..
    // For example read from hdfs file system and verify data
    validateResult();
  }
}
