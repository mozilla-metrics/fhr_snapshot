/**
 * get batch records from the master file 0-1m
 * /home/aphadke/fhr-hadoop-hbase/perl getRecords.pl 0 10000000
 * run batch delete MR job
 * hdfs dfs -rm -r temp_hbase_delete; hadoop jar fhr_delete.jar fhr_small_orphan_id/ temp_hbase_delete/ 
 */
package com.mozilla.main;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
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


public class ReadHDFSDeleteHBase implements Tool 
{
    /**
     * The map class of WordCount.
     */
    public static class ReadHDFSDeleteHBaseMapper
    extends Mapper<Object, Text, Text, Text> {
        private final static int minimum = 1;
        private final static int maximum = 10000;
        Random rn;
        int range;
        int randomNum;

        public void setup (Context context) {
            rn = new Random();
            range = maximum - minimum + 1;
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String v = value.toString();
            String[] splitComma = StringUtils.split(v, ",");
            randomNum =  rn.nextInt(range) + minimum;
            context.write(new Text(randomNum + ""), new Text(splitComma[0]));




        }
    }
    /**
     * The reducer class of WordCount
     */
    public static class ReadHDFSDeleteHBaseReducer extends Reducer<Text, Text, Text, Text> {
        public static int CHUNK_SIZE = 1000;
        private HTable htable;
        private final static String TABLE_NAME = "metrics";
        static enum DELETE_PROGRESS { ERROR_BATCH_DELETE, IDS_TO_DELETE, BATCH_DELETE_INVOKED }; 
        public void setup (Context context) {
            Configuration hBaseConfig =  HBaseConfiguration.create();
            hBaseConfig.setInt("timeout", 300);
            hBaseConfig.set("hbase.master", "node3.admin.peach.metrics.scl3.mozilla.com:9000");
            hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
            try {
                htable = new HTable(hBaseConfig, TABLE_NAME);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int i = 0;
            List<Delete> batch = new ArrayList<Delete>();
            boolean outputCollected = false;
            StringBuilder v = new StringBuilder();
            for (Text value : values) {
                if (i < CHUNK_SIZE) {
                    Delete delete = new Delete(Bytes.toBytes(value.toString()));
                    context.getCounter(DELETE_PROGRESS.IDS_TO_DELETE).increment(1);
                    batch.add(delete);
                    //v.append(value.toString() + "\t");
                    outputCollected = false;
                }
                i++;
                if (i >= CHUNK_SIZE) {
                    outputCollected = true;
                    performBatchDelete(batch, context);
                    batch = new ArrayList<Delete>();
                    //context.write(key, new Text(v.toString().trim()));
                    i = 0;
                    v = new StringBuilder();
                }
            }
            if (!outputCollected) {
                performBatchDelete(batch, context);
                batch = new ArrayList<Delete>();
                //context.write(key, new Text(v.toString().trim()));
            }
        }

        public void performBatchDelete(List<Delete> batch, Context context) {
            Object[] results = new Object[batch.size()];
            String errorString = new String();
            try {
                context.getCounter(DELETE_PROGRESS.BATCH_DELETE_INVOKED).increment(1);
                htable.batch(batch, results);
            } catch (Exception e) {
                context.getCounter(DELETE_PROGRESS.ERROR_BATCH_DELETE).increment(1);
                errorString = e.getMessage();
            }
            for (int i = 0; i < results.length; i++) {
                try {
                    context.write(new Text(results[i] + ""), new Text("") );
                } catch (IOException e) {
                    errorString = e.getMessage();
                } catch (InterruptedException e) {
                    errorString = e.getMessage();

                }
            }
            if (StringUtils.isNotBlank(errorString)) {
                try {
                    context.write(new Text(errorString), new Text("error in iterator"));
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * The main entry point.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadHDFSDeleteHBase(), args);
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
        job.setJarByClass(ReadHDFSDeleteHBase.class);
        job.setMapperClass(ReadHDFSDeleteHBaseMapper.class);
        job.setReducerClass(ReadHDFSDeleteHBaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1000);
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