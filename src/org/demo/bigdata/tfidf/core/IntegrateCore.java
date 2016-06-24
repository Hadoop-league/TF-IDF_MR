package org.demo.bigdata.tfidf.core;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <p></p>
 * Create Date: Jun 16, 2016
 * Last Modify: Jun 16, 2016
 * 
 * @author <a href="http://weibo.com/u/5131020927">Q-WHai</a>
 * @see <a href="http://blog.csdn.net/lemon_tree12138">http://blog.csdn.net/lemon_tree12138</a>
 * @version 0.0.1
 */
public class IntegrateCore {

    public static class IntegrateMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
        }
    }
    
    public static class IntegrateReducer extends Reducer<Text, Text, Text, Text> {
        
        private double keywordIDF = 0.0d;
        private Text value = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }
            
            if (key.toString().split(":")[1].startsWith("!")) {
                keywordIDF = Double.parseDouble(values.iterator().next().toString());
                return;
            }
            
            value.set(String.valueOf(Double.parseDouble(values.iterator().next().toString()) * keywordIDF));
            
            context.write(key, value);
        }
    }
}
