package sics;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.lang.Integer;
import java.lang.Math;
import java.lang.System;
import java.util.*;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.HashMap;
import javax.xml.parsers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.w3c.dom.*;

public class TopTen {

    private static DocumentBuilderFactory factory;
    private static DocumentBuilder builder;


    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        private Random random = new Random();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = transformXmlToMap(value.toString());
            String reputation = parsed.getOrDefault("Reputation", "");
            Text user = new Text(parsed.getOrDefault("DisplayName", ""));
            if (reputation.length() <= 0) {
                return;
            }

            // Add this record to our map with the reputation as the key
            repToRecordMap.put(Integer.parseInt(reputation), user);

            // If we have more than ten records, remove the one with the lowest reputation.
            while (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            for (Text t : repToRecordMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        // Overloads the comparator to order the reputations in descending order
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        private Random random = new Random();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer i = 0;
            for (Text value : values) {
                i += 1;
                Map<String, String> parsed = transformXmlToMap(value.toString());
                String reputation = parsed.getOrDefault("Reputation", "");
                if (reputation.length() <= 0) {
                    return;
                }
                repToRecordMap.put(Integer.parseInt(reputation), value);

                // If we have more than ten records, remove the one with the lowest reputation
                while (repToRecordMap.size() > 10) {
                    Integer minReputation = Collections.min(repToRecordMap.keySet());
                    repToRecordMap.remove(minReputation);
                }
            }
            System.out.println();
            System.out.println("Values in reduce count:");
            System.out.println(i);

            for (Text t : repToRecordMap.descendingMap().values()) {
                // Output our ten records to the file system with a null key
                context.write(NullWritable.get(), t);
            }
            System.out.println("Values in repToRecordMap count:");
            System.out.println(repToRecordMap.values().size());
        }
    }

    private static Map<String, String> transformXmlToMap(String xml) {
        Document doc = null;
        if (factory == null || builder == null) {
            try {
                factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(false);
                builder = factory.newDocumentBuilder();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            doc = builder.parse(new ByteArrayInputStream(xml.getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
            return new HashMap<String, String>();
        }

        Map<String, String> map = new HashMap<String, String>();
        NamedNodeMap attributeMap = doc.getDocumentElement().getAttributes();

        for (int i = 0; i < attributeMap.getLength(); ++i) {
            Attr n = (Attr) attributeMap.item(i);
            map.put(n.getName(), n.getValue());
        }

        return map;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top ten");
        job.setJarByClass(TopTen.class);

        job.setMapperClass(TopTenMapper.class);
        // job.setCombinerClass(TopTenReducer.class);
        job.setReducerClass(TopTenReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
