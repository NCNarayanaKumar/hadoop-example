package com.np.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public interface HdfsService {
  public FileSystem getFileSystem();
  public FileStatus[] list(String path);
  public void close();
  
  /**
   * Used to read from a file
   * @author saacharya
   * @param <T> Return type
   */
  public static interface ReadCallback<T> {
    public T read(BufferedReader reader) throws IOException;
  }
  
  public <KeyType extends Writable, T extends Writable> void save(Path path, Map<KeyType, T> records);
  
  public <ReadResultType> ReadResultType read(ReadCallback<ReadResultType> callback, Path path);
}
