package com.djulo.genreCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Count the number of movies by genre
 */
public class GenreCount {

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
    Path input = new Path(files[0]);
    Path output = new Path(files[1]);

    Job job = Job.getInstance(conf, "Movie per genre counter");
    job.setJarByClass(GenreCount.class);
    job.setMapperClass(GenreCountMapper.class);
    job.setReducerClass(GenreCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
