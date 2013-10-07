/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

/**
 * connect to a given table and write output for data:json as text to HDFS
 * build the jar:
 * perl perl/build.pl t.jar com.mozilla.main.ReadHBaseWriteHdfs cm-hadoop01
 * currently connects to HBase @ cm-hadoop-adm02. Change the config settings as needed or make it a cmd line param
 * rm lucene/* ; hadoop dfs -rmr output ; hadoop dfs -rmr Index/*; hadoop jar t.jar -Dstart.date=20100818 -Dend.date=20100819 output
 */
package com.mozilla.main;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;



/**
 * @author Anurag Phadke
 * hadoop dfs -rmr output; hadoop jar t.jar output
 *
 */
public class ReadHBaseWriteHdfs implements Tool {
    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    public static String TABLE_NAME = "metrics"; //fhr-small or metrics
    //private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ReadHBaseWriteHdfs.class);
    private Configuration conf;
    //private static final String HBASE_HOST = "admin1.research.hadoop.sjc1.mozilla.com";

    public static class ReadHBaseWriteHdfsMapper extends TableMapper<Text, Text> {

        public enum ReportStats { RAW_JSON_COUNT, ERROR_JSON_PARSE, INVALID_FHR_DOCUMENT }
        public Set<String> validIdSet = new HashSet<String>();
        private Path[] localFiles;

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        public void setup(Context context) {
        }


        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        public void map(ImmutableBytesWritable key, Result result, Context context) throws InterruptedException, IOException {
            //if (Math.random() > 0.95) {
            // extract userKey from the compositeKey (userId + counter)
            boolean validFHRDocument = false;

            String rowId = new String(result.getRow());
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("data"));
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                if (Bytes.toString(entry.getKey()).equals("json")) {

                    //comment this block for mango table
                    //                        String mapValue = Bytes.toString(entry.getValue()).replaceAll("\"\"", "\"");
                    //                        mapValue = mapValue.replaceFirst("\"", "");
                    //                        if (mapValue.endsWith("\"")) {
                    //                            mapValue = mapValue.substring(0, mapValue.length() - 1);
                    //                        }


                    String mapValue = Bytes.toString(entry.getValue()); //uncomment this line for metrics table

                    ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally

                    context.getCounter(ReportStats.RAW_JSON_COUNT).increment(1);

                    try {
                        validFHRDocument = false;
                        JsonNode rootNode = mapper.readValue(mapValue, JsonNode.class); // src can be a File, URL, InputStream etc
                        int fhrVersion = rootNode.path("version").getIntValue();
                        Date thisPingDate = null, lastPingDate = null;
                        String updateChannel = null, os = null, country = null;
                        int profileCreation = -1, memoryMB = -1;

                        if (fhrVersion == 2) {
                            validFHRDocument = true;
                            //System.out.println("valid fhr version");
                            try {
                                if (StringUtils.isNotBlank(rootNode.path("thisPingDate").getTextValue())) {
                                    thisPingDate = formatter.parse(rootNode.path("thisPingDate").getTextValue());
                                } else {
                                    validFHRDocument = false;
                                }
                            } catch (ParseException e) {
                                // TODO Auto-generated catch block
                                validFHRDocument = false;
                                e.printStackTrace();
                            }
                            if (!validFHRDocument) {
                                break;
                            }
                            try {
                                if (StringUtils.isNotBlank(rootNode.path("lastPingDate").getTextValue())) {
                                    lastPingDate = formatter.parse(rootNode.path("lastPingDate").getTextValue());
                                }
                            } catch (ParseException e) {
                                // TODO Auto-generated catch block
                                validFHRDocument = false;
                                e.printStackTrace();
                            }
                            if (!validFHRDocument) {
                                break;
                            }
                            updateChannel = rootNode.path("geckoAppInfo").path("updateChannel").getTextValue();
                            if (StringUtils.isBlank(updateChannel)) {
                                updateChannel = rootNode.path("data").path("last").path("org.mozilla.appInfo.appinfo").path("updateChannel").getTextValue();
                                if (StringUtils.isBlank(updateChannel)) {    
                                    validFHRDocument = false;
                                }
                            }
                            if (StringUtils.isNotBlank(updateChannel)) {
                                updateChannel = updateChannel.trim();
                            }

                            os = rootNode.path("geckoAppInfo").path("os").getTextValue();
                            if (StringUtils.isBlank(os)) {
                                os = rootNode.path("data").path("last").path("org.mozilla.appInfo.appinfo").path("os").getTextValue();
                                if (StringUtils.isBlank(os)) {    
                                    validFHRDocument = false;
                                }
                            }
                            if (StringUtils.isNotBlank(os)) {
                                os = os.trim();
                            }

                            profileCreation = rootNode.path("data").path("last").path("org.mozilla.profile.age").path("profileCreation").getIntValue();
                            if (profileCreation == -1) {    
                                validFHRDocument = false;
                            }

                            country = rootNode.path("geoCountry").getTextValue();
                            if (StringUtils.isBlank(country)) {    
                                validFHRDocument = false;
                            }

                            memoryMB = rootNode.path("data").path("last").path("org.mozilla.sysinfo.sysinfo").path("memoryMB").getIntValue();
                            if (memoryMB == -1) {    
                                validFHRDocument = false;
                            }


                            Iterator<String> s = rootNode.path("data").path("days").getFieldNames();
                            List<Date> dataDays = new ArrayList<Date>();

                            while(s.hasNext()){
                                try {
                                    String tempDay = s.next();
                                    dataDays.add(formatter.parse(tempDay));
                                } catch (ParseException e) {
                                    // TODO Auto-generated catch block
                                    validFHRDocument = false;
                                    e.printStackTrace();
                                }

                            }
                            int numAppSessionsPreviousOnThisPingDate = 0;
                            numAppSessionsPreviousOnThisPingDate = rootNode.path("data").path("days").path(formatter.format(thisPingDate)).path("org.mozilla.appSessions.previous").path("main").size();

                            int currentSessionTime = 0;
                            currentSessionTime = rootNode.path("data").path("last").path("org.mozilla.appSessions.current").path("totalTime").getIntValue();

                            List<Date> activeDays = new ArrayList<Date>();
                            activeDays.add(thisPingDate);
                            if (dataDays != null) {
                                activeDays.addAll(dataDays);
                            }
                            if (lastPingDate != null) {
                                activeDays.add(lastPingDate);
                            }
                            Collections.sort(activeDays);
                            Date firstValidFHRDate = null;

                            boolean firstValidFHRDateFound = false;
                            if (dataDays.size() > 0) {
                                for (Date d : activeDays) {
                                    if (firstValidFHRDateFound) {
                                        break;
                                    }
                                    //System.out.println("D: " + formatter.format(d));
                                    Iterator <String> dateIterator = rootNode.path("data").path("days").path(formatter.format(d)).getFieldNames();
                                    while (dateIterator.hasNext()) {
                                        String tempDay = dateIterator.next();
                                        if (!(StringUtils.equals(tempDay, "org.mozilla.crashes.crashes") || StringUtils.equals(tempDay, "org.mozilla.appSessions.previous"))) {
                                            //System.out.println("First Valid FHR date: " + formatter.format(d));
                                            firstValidFHRDate = d;
                                            firstValidFHRDateFound = true;
                                            break;
                                        }
                                    }
                                }

                                if (!firstValidFHRDateFound) {
                                    firstValidFHRDate = activeDays.get(0);
                                    firstValidFHRDateFound = true;
                                }
                            }

                            if (!firstValidFHRDateFound) {
                                firstValidFHRDate = activeDays.get(0);
                                firstValidFHRDateFound = true;
                            }

                            String firstFhrActiveDayData = null;
                            for (Date d : activeDays) {
                                if (d.equals(firstValidFHRDate) || d.after(firstValidFHRDate)) {
                                    firstFhrActiveDayData = rootNode.path("data").path("days").path(formatter.format(d)).toString();
                                    String base64Key = new String(Base64.encodeBase64((os + updateChannel + country + memoryMB + profileCreation + formatter.format(d) + firstFhrActiveDayData).getBytes()));

                                    //TODO: use rowID + base64encodeJson + firstFHRDate in value and parse it out in the reducer before doing comparison
                                    String headRecord = new String(Base64.encodeBase64((mapValue).getBytes()));

                                    context.write(new Text(base64Key + "\t" + formatter.format(d)), new Text(headRecord + "\t" + formatter.format(thisPingDate) + ":" + numAppSessionsPreviousOnThisPingDate + ":" + currentSessionTime + ":" + rowId));

                                    break;
                                }
                            }


                        } else {
                            validFHRDocument = false;
                            //System.err.println("invalid fhr version json: " + rowId + "\t" + fhrVersion);
                        }

                        if (validFHRDocument) {
                            //System.out.println("valid document");
                        } else {
                            //System.err.println("invalid fhr document");
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                        context.getCounter(ReportStats.ERROR_JSON_PARSE).increment(1);
                    } finally {
                        if (!validFHRDocument) {
                            context.getCounter(ReportStats.INVALID_FHR_DOCUMENT).increment(1);
                        }
                    }        
                    //end of mapper

                    //}//math.random
                }

            }

        }
    }

    public static class ReadHBaseWriteHdfsReducer extends Reducer<Text, Text, Text, Text> {

        public enum ReportStats { REDUCER_LINES, DUPLICATE_RECORDS, DUPLICATE_RECORD_COUNT, HEAD_RECORDS, NO_ID_TO_JSON_MAPPING, HEAD_RECORD_COUNT_OUT_OF_DUPLICATE_RECORD};
        public static boolean outputHeadRecords = true;
        public static boolean outputDuplicateRecords = false;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
            List<Date> thisPingDate = new ArrayList<Date>();
            List<Integer> numAppSessions = new ArrayList<Integer>();
            List<Integer> currentSessionTime = new ArrayList<Integer>();

            Map<Date, String> thisPingDateMap = new HashMap<Date, String>();
            Map<Integer, String> numAppSessionsMap = new HashMap<Integer, String>();
            Map<Integer, String> currentSessionTimeMap = new HashMap<Integer, String>();
            Map<String, String> rowIdToJson = new HashMap<String, String>();

            String rowId = null;
            int duplicateRecordCounter = 0;
            for (Text v : values) { 
                duplicateRecordCounter++;
                if (duplicateRecordCounter > 20) {
                    break;
                }
                try {

                    String[] rawValue = StringUtils.split(v.toString(), "\t");

                    context.getCounter(ReportStats.REDUCER_LINES).increment(1);
                    String[] splitColon = StringUtils.split(rawValue[1], ":");
                    rowId = splitColon[3];

                    rowIdToJson.put(rowId, rawValue[0]);

                    thisPingDate.add(formatter.parse(splitColon[0]));
                    thisPingDateMap.put(formatter.parse(splitColon[0]), splitColon[3]);

                    numAppSessions.add(Integer.parseInt(splitColon[1]));
                    numAppSessionsMap.put(Integer.parseInt(splitColon[1]), splitColon[3]);

                    currentSessionTime.add(Integer.parseInt(splitColon[2]));
                    currentSessionTimeMap.put(Integer.parseInt(splitColon[2]), splitColon[3]);

                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    System.err.println("error in reducer: " + e.getLocalizedMessage());
                }
            }

            String collectKey = null;
            if (thisPingDate.size() > 1) {
                context.getCounter(ReportStats.DUPLICATE_RECORDS).increment(1);
                Collections.sort(thisPingDate);
                Collections.sort(numAppSessions);
                Collections.sort(currentSessionTime);

                if (thisPingDate.get(thisPingDate.size() - 1).equals(thisPingDate.get(thisPingDate.size() - 2))) {
                    if (numAppSessions.get(numAppSessions.size() - 1) == (numAppSessions.get(numAppSessions.size() - 2))) {
                        collectKey = currentSessionTimeMap.get(currentSessionTime.get(currentSessionTime.size() - 1));
                    } else {
                        collectKey = numAppSessionsMap.get(numAppSessions.get(numAppSessions.size() - 1));
                    }


                } else {
                    collectKey = thisPingDateMap.get(thisPingDate.get(thisPingDate.size() - 1));
                }
                try {
                    if (rowIdToJson.containsKey(collectKey)) {
                        if (outputHeadRecords) {
                            context.write(new Text(collectKey), new Text(Base64.decodeBase64(rowIdToJson.get(collectKey))));
                        }
                        context.getCounter(ReportStats.HEAD_RECORDS).increment(1);

                        //now lets output the duplicate records as they need to be deleted.
                        for (String ritjKey : rowIdToJson.keySet()) {
                            if (!StringUtils.equals(ritjKey, collectKey)) {
                                context.getCounter(ReportStats.DUPLICATE_RECORD_COUNT).increment(1);
                                if (outputDuplicateRecords) {
                                    context.write(new Text(ritjKey), new Text("DUPLICATE_RECORD_ID_TO_DELETE"));
                                }
                            } else {
                                context.getCounter(ReportStats.HEAD_RECORD_COUNT_OUT_OF_DUPLICATE_RECORD).increment(1);
                            }
                        }
                    } else {
                        context.getCounter(ReportStats.NO_ID_TO_JSON_MAPPING).increment(1);
                        if (outputHeadRecords) {
                            context.write(new Text(collectKey), new Text("1"));
                        }

                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            } else {
                try {
                    if (rowIdToJson.containsKey(rowId)) {
                        if (outputHeadRecords) {
                            context.write(new Text(rowId), new Text(Base64.decodeBase64(rowIdToJson.get(rowId))));
                        }
                        context.getCounter(ReportStats.HEAD_RECORDS).increment(1);
                    } else {
                        context.getCounter(ReportStats.NO_ID_TO_JSON_MAPPING).increment(1);
                        if (outputHeadRecords) {
                            context.write(new Text(rowId), new Text("1"));
                        }

                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }


        }

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    public Configuration getConf() {
        //        this.conf.set("hbase.zookeeper.quorum", HBASE_HOST);
        //        this.conf.set("hbase.zookeeper.property.clientPort", "2181");
        //        this.conf.set("mapred.compress.map.output", "true"); 


        //    this.conf.set("mapred.child.java.opts", "-Xmx2000m");
        return this.conf;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     */
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new ReadHBaseWriteHdfs(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.job.queue.name","prod");
        Job job = new Job(conf, "ReadHBaseWriteHDFS");
        job.setJarByClass(ReadHBaseWriteHdfs.class);
        Scan scan = new Scan();
        scan.addFamily("data".getBytes());

        TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, ReadHBaseWriteHdfsMapper.class, Text.class, Text.class, job);

        job.setReducerClass(ReadHBaseWriteHdfsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1000);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);    
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[0]));


        job.waitForCompletion(true);
        if (job.isSuccessful()) {
            System.out.println("DONE");
        }



        return 0;
    }

}










