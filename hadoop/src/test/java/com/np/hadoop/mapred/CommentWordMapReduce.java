package com.np.hadoop.mapred;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;   
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CommentWordMapReduce {

  public static class WordMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);

    private Text word = new Text();

    private static final Set<String> WORDS_TO_GET_COUNT_OF = new HashSet<String>(Arrays
        .asList(new String[] { "james", "2012", "cave", "walther", "bond" }));

    private final JSONParser parser = new JSONParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
      JSONObject jsonObj;
      try {
        jsonObj = (JSONObject) parser.parse(value.toString());
      }
      catch (ParseException e) {
        e.printStackTrace();
        return;
      }
      String comment = jsonObj.get("comment").toString();
      StringTokenizer tokenizer = new StringTokenizer(comment);

      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken().toLowerCase();

        if (WORDS_TO_GET_COUNT_OF.contains(token)) {

          word.set(token);
          context.write(word, ONE);
        }
      }
    }

  }

  public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
      InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public boolean mapred(String inputPath, String outputPath) throws IOException,
    InterruptedException,
    ClassNotFoundException {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "process word count");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(WordMap.class);
    job.setReducerClass(WordReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(1);
    
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    job.waitForCompletion(true);

    return job.isSuccessful();
  }
}
