package MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MRJobBuilder {
    private String jobName = null;
    private Configuration conf = null;
    private Class<?> jarClass = null;
    private String inputPath = null;
    private Boolean inputDirRecursive = null;
    private String outputPath = null;
    private Integer numReduceTasks = null;
    private Class<? extends Mapper> mapperClass = null;
    private Class<? extends Reducer> reducerClass = null;
    private Class<?> outputKeyClass = null;
    private Class<?> outputValueClass = null;
    private Class<?> mapOutputKeyClass = null;
    private Class<?> mapOutputValueClass = null;

    public MRJobBuilder setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public MRJobBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public MRJobBuilder setJarClass(Class<?> jarClass) {
        this.jarClass = jarClass;
        return this;
    }

    public MRJobBuilder setInputPath(String inputPath) {
        this.inputPath = inputPath;
        return this;
    }

    public MRJobBuilder setInputDirRecursive(Boolean inputDirRecursive) {
        this.inputDirRecursive = inputDirRecursive;
        return this;
    }

    public MRJobBuilder setOutputPath(String outputPath) {
        this.outputPath = outputPath;
        return this;
    }

    public MRJobBuilder setNumReduceTasks(Integer numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
        return this;
    }

    public MRJobBuilder setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
        return this;
    }

    public MRJobBuilder setReducerClass(Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass;
        return this;
    }

    public MRJobBuilder setOutputKeyClass(Class<?> outputKeyClass) {
        this.outputKeyClass = outputKeyClass;
        return this;
    }

    public MRJobBuilder setOutputValueClass(Class<?> outputValueClass) {
        this.outputValueClass = outputValueClass;
        return this;
    }

    public MRJobBuilder setMapOutputKeyClass(Class<?> mapOutputKeyClass) {
        this.mapOutputKeyClass = mapOutputKeyClass;
        return this;
    }

    public MRJobBuilder setMapOutputValueClass(Class<?> mapOutputValueClass) {
        this.mapOutputValueClass = mapOutputValueClass;
        return this;
    }

    public MRJob createMRJob() throws IOException {
        return new MRJob(jobName, conf, jarClass, inputPath, inputDirRecursive, outputPath, mapperClass, reducerClass, numReduceTasks, outputKeyClass, outputValueClass, mapOutputKeyClass, mapOutputValueClass);
    }
}