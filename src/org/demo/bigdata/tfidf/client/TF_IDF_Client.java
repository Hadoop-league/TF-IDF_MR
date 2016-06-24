package org.demo.bigdata.tfidf.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.demo.bigdata.tfidf.core.IDFMapReduceCore;
import org.demo.bigdata.tfidf.core.IntegrateCore;
import org.demo.bigdata.tfidf.core.TFMapReduceCore;
import org.demo.bigdata.tfidf.utils.MapReducerUtils;

public class TF_IDF_Client {

    private static String inputPath = "./input";
    private static String outputPath = "./output";
    
    public static void main(String[] args) {
        TF_IDF_Client client = new TF_IDF_Client();
        
        if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        
        try {
            client.execute();
        } catch (Exception e) {
            System.err.println(e);
        }
    }
    
    private void execute() throws Exception {
        String tmpTFPath = outputPath + "_tf";
        String tmpIDFPath = outputPath + "_idf";
        
        runTFJob(inputPath, tmpTFPath);
        runIDFJob(tmpTFPath, tmpIDFPath);
        runIntegrateJob(tmpTFPath, tmpIDFPath, outputPath);
    }
    
    private int runTFJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        MapReducerUtils.removeOutputFolder(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("TF-job");
        job.setJarByClass(TFMapReduceCore.class);

        job.setMapperClass(TFMapReduceCore.TFMapper.class);
        job.setCombinerClass(TFMapReduceCore.TFCombiner.class);
        job.setPartitionerClass(TFMapReduceCore.TFPartitioner.class);
        job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
        job.setReducerClass(TFMapReduceCore.TFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private int runIDFJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        MapReducerUtils.removeOutputFolder(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("IDF-job");
        job.setJarByClass(IDFMapReduceCore.class);
        
        job.setMapperClass(IDFMapReduceCore.IDFMapper.class);
        job.setReducerClass(IDFMapReduceCore.IDFReducer.class);
//        job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
        job.setProfileParams(String.valueOf(getNumReduceTasks(configuration, inputPath)));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private int runIntegrateJob (String inputTFPath, String inputIDFPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        MapReducerUtils.removeOutputFolder(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("Integrate-job");
        job.setJarByClass(IntegrateCore.class);
        
        job.setMapperClass(IntegrateCore.IntegrateMapper.class);
        job.setReducerClass(IntegrateCore.IntegrateReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputTFPath));
        FileInputFormat.addInputPath(job, new Path(inputIDFPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private int getNumReduceTasks(Configuration configuration, String inputPath) throws Exception {
        FileSystem hdfs = FileSystem.get(configuration);
        FileStatus status[] = hdfs.listStatus(new Path(inputPath));
        return status.length;
    }
}
