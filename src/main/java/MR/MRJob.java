package MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;


public class MRJob {

    private Job job;

    public MRJob(String jobName, Configuration conf, Class<?> jarClass, String inputPath, Boolean inputDirRecursive,
                 String outputPath, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, Integer numReduceTasks,
                 Class<?> outputKeyClass, Class<?> outputValueClass, Class<?> mapOutputKeyClass, Class<?> mapOutputValueClass) throws IOException {

        // Create new job.
        job = new Job();
        if (jobName != null) job.setJobName(jobName);
        job = (conf != null ? Job.getInstance(conf) : Job.getInstance());
        if (jarClass != null) job.setJarByClass(jarClass);

        // Specify HDFS input and output paths.
        if (inputPath != null) FileInputFormat.setInputPaths(job, new Path(inputPath));
        if (inputDirRecursive != null) FileInputFormat.setInputDirRecursive(job, inputDirRecursive);
        if (outputPath != null) FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Specify mapper, reducer classes.
        if (mapperClass != null) job.setMapperClass(mapperClass);
        if (reducerClass != null) job.setReducerClass(reducerClass);
        if (numReduceTasks != null) job.setNumReduceTasks(numReduceTasks);

        // Specify key value types.
        if (outputKeyClass != null) job.setOutputKeyClass(outputKeyClass);
        if (outputValueClass != null) job.setOutputValueClass(outputValueClass);

        // Specify mapper output key value types if different than reducer key value types
        if (mapOutputKeyClass != null) job.setMapOutputKeyClass(mapOutputKeyClass);
        if (mapOutputValueClass != null) job.setMapOutputValueClass(mapOutputValueClass);
    }

    public boolean Run() throws InterruptedException, IOException, ClassNotFoundException {
        // Submit job from client into cluster.
        return job.waitForCompletion(true);
    }
}
