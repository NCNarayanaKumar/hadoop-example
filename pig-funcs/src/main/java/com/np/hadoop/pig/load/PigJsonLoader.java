package com.np.hadoop.pig.load;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.parser.ParseException;

/**
 * Original Author https://gist.github.com/601331 Kim Vogt
 * This loader can be used by Pig to Load JSON lines.
 */
public class PigJsonLoader extends LoadFunc {
  private static final Logger LOG = Logger.getLogger(PigJsonLoader.class);

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  
  private final JsonLineParser parser;
  
  public PigJsonLoader() {
    this(new JsonLineParser.Impl());
  }
  
  PigJsonLoader(JsonLineParser parser) {
    this.parser = parser;
  }
  
  private LineRecordReader in = null;

  @Override
  public InputFormat<?, ?> getInputFormat() throws IOException {
    return new PigTextInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {    
    if (!in.nextKeyValue()) {
      return null;
    }

    Text val = in.getCurrentValue();

    if (val == null) {
      return null;
    }

    String line = val.toString();
    return line.length() > 0 ? parseStringToTuple(line) : null;  
  }

  protected Tuple parseStringToTuple(String line) {
    try {      
      Map<String, Object> values = parser.parse(line);
      return tupleFactory_.newTuple(values);
    }
    catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      return null;
    }
    catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + line, e);
      return null;
    }

  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    in = (LineRecordReader) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    PigFileInputFormat.setInputPaths(job, location);
  }
}
