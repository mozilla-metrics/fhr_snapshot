package com.mozilla.main;

import java.io.IOException;
import java.text.ParseException;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount implements Tool 
{
    /**
     * The map class of WordCount.
     */
    public static class WordCountMapper
    extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private final static String TABLE_NAME = "metrics";
        private final static int minimum = 1;
        private final static int maximum = 10000;
        Random rn;
        int range;
        int randomNum;

        private HTable htable;
        public void setup (Context context) {
            rn = new Random();
            range = maximum - minimum + 1;
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (Math.random() > 0.95) {
                String v = value.toString();
                randomNum =  rn.nextInt(range) + minimum;
                if (StringUtils.contains(v, ",")) {
                    String[] splitComma = v.split(",");
                    context.write(new Text(randomNum + ""), new Text(splitComma[0]));
                } 
                //            if (StringUtils.contains(v, "\t")) {
                //                String[] splitTab = v.split("\t");
                //                context.write(new Text(splitTab[0]), new Text("1"));
                //            } 
                //            
                //            if (StringUtils.contains(v, ",")) {
                //                String[] splitTab = v.split(",");
                //                context.write(new Text(splitTab[0]), new Text("1"));
                //            } 

            }

        }
    }
    /**
     * The reducer class of WordCount
     */
    public static class WordCountReducer
    extends Reducer<Text, Text, Text, Text> {
        public static int CHUNK_SIZE = 1000;
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (Text value : values) {
                sum ++;
            }
            //context.write(key, new IntWritable(sum));
            context.write(key, new Text(sum + ""));
        }
    }
    /**
     * The main entry point.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    /**
     * @param args
     * @return
     * @throws IOException
     * @throws ParseException 
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public Job initJob(String[] args) throws IOException, ParseException, InterruptedException, ClassNotFoundException {
        // Set both start/end time and start/stop row
        Configuration conf = new Configuration();
        Job job = new Job(conf, "WordCount: " + args[1]);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(1000);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);    

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);


        return job;
    }

    @Override
    public Configuration getConf() {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public void setConf(Configuration arg0) {
        // TODO Auto-generated method stub

    }
    @Override
    public int run(String[] args) throws Exception {
        int rc = -1;
        Job job = initJob(args);
        job.waitForCompletion(true);
        if (job.isSuccessful()) {
            rc = 0;
        }

        return rc;

    }
}  