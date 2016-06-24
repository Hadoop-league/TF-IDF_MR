package org.demo.bigdata.tfidf.core;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <p></p>
 * Create Date: Jun 14, 2016
 * Last Modify: Jun 14, 2016
 * 
 * @author <a href="http://weibo.com/u/5131020927">Q-WHai</a>
 * @see <a href="http://blog.csdn.net/lemon_tree12138">http://blog.csdn.net/lemon_tree12138</a>
 * @version 0.0.1
 */
public class TFMapReduceCore {

    public static class TFMapper extends Mapper<Object, Text, Text, Text> {
        
        private final Text one = new Text("1");
        private Text label = new Text();
        private int allWordCount = 0;
        private String fileName = "";
        
        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            fileName = getInputSplitFileName(context.getInputSplit());
        }
        
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                allWordCount++;
                label.set(String.join(":", tokenizer.nextToken(), fileName));
                context.write(label, one);
            }
        }
        
        @Override
        protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text("!:" + fileName), new Text(String.valueOf(allWordCount)));
        }
        
        private String getInputSplitFileName(InputSplit inputSplit) {
            String fileFullName = ((FileSplit)inputSplit).getPath().toString();
            String[] nameSegments = fileFullName.split("/");
            return nameSegments[nameSegments.length - 1];
        }
    }
    
    public static class TFCombiner extends Reducer<Text, Text, Text, Text> {
        private int allWordCount = 0;
        
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            
            if (values == null) {
                return;
            }
            
            if(key.toString().startsWith("!")) {
                allWordCount = Integer.parseInt(values.iterator().next().toString());
                return;
            }
            
            int sumCount = 0;
            for (Text value : values) {
                sumCount += Integer.parseInt(value.toString());
            }
            
            double tf = 1.0 * sumCount / allWordCount;
            context.write(key, new Text(String.valueOf(tf)));
        }
    }
    
    public static class TFReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
            if (values == null) {
                return;
            }
            
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
    
    public static class TFPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String fileName = key.toString().split(":")[1];
            return Math.abs((fileName.hashCode() * 127) % numPartitions);
        }
    }
}
