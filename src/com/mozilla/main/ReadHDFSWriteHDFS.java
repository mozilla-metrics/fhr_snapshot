package com.mozilla.main;

import java.io.IOException;
import java.text.ParseException;

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


public class ReadHDFSWriteHDFS implements Tool 
{
    /**
     * The map class of WordCount.
     */
    public static class ReadHDFSWriteHDFSMapper
    extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private final static String TABLE_NAME = "metrics";

        private HTable htable;
        public void setup (Context context) {
            try {
                Configuration hBaseConfig =  HBaseConfiguration.create();
                hBaseConfig.setInt("timeout", 300);
                hBaseConfig.set("hbase.master", "node3.admin.peach.metrics.scl3.mozilla.com:9000");
                hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
                htable = new HTable(hBaseConfig, TABLE_NAME);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String v = value.toString();
            String[] splitComma = StringUtils.split(v, ",");
            Get g = new Get(Bytes.toBytes(splitComma[0]));
            Result r = htable.get(g);
            byte[] columnValue = r.getValue(Bytes.toBytes("data"), Bytes.toBytes("json"));
            if (columnValue != null && columnValue.length > 0) {
                String cValue = new String(columnValue);
                context.write(new Text(splitComma[0]), new Text(cValue));
            }
            
            
            
        }
    }
    /**
     * The reducer class of WordCount
     */
    public static class ReadHDFSWriteHDFSReducer
    extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            //context.write(key, new IntWritable(sum));
            context.write(key, null);
        }
    }
    /**
     * The main entry point.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadHDFSWriteHDFS(), args);
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
        Job job = new Job(conf, "ValidIdRead: " + args[1]);
        job.setJarByClass(ReadHDFSWriteHDFS.class);
        job.setMapperClass(ReadHDFSWriteHDFSMapper.class);
        job.setReducerClass(ReadHDFSWriteHDFSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
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