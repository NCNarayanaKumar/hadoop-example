package com.np.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class HdfsServiceImpl implements HdfsService {
  private final FileSystem fileSystem;

  public HdfsServiceImpl() {

    try {
      fileSystem = FileSystem.get(new Configuration());
    }
    catch (IOException e) {
      throw new RuntimeException("Error Getting File System", e);
    }
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }
  
  public FileStatus[] list(String path) {
    try {
      return fileSystem.listStatus(new Path(path));
    }
    catch (IOException e) {
      throw new RuntimeException("Error reading directory", e);
    }
  }
  
  public void close() {   
    try {
      fileSystem.close();
    }
    catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public <ReadResultType> ReadResultType read(ReadCallback<ReadResultType> callback, Path path) {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
      return callback.read(reader);
    } catch (IOException e) {
      throw new RuntimeException("Error reading stream", e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public <KeyType extends Writable, T extends Writable> void save(Path path, Map<KeyType, T> records) {
    SequenceFile.Writer writer = null;
    try {
      if (records.size() == 0) {return;}
      Map.Entry<KeyType, T> entry = records.entrySet().iterator().next();
        
      writer  = new SequenceFile.Writer(fileSystem, fileSystem.getConf(), path, entry.getKey().getClass(), 
        entry.getValue().getClass());
      
      for (Map.Entry<KeyType, T> record : records.entrySet()) {
        writer.append(record.getKey(), record.getValue());
      }
  
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    
  }
}
