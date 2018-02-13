package edu.seattleu.crawl.warc;

import java.io.IOException;
//import java.net.URI;
//import java.util.StringTokenizer;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.net.URI;
//import java.util.ArrayList;
import java.util.HashMap;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

//import org.apache.commons.io.IOUtils;
//import org.archive.io.ArchiveReader;
//import org.archive.io.ArchiveRecord;
//import org.archive.io.warc.WARCReaderFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.archive.io.ArchiveReader;
//import org.archive.io.ArchiveRecord;
//import org.archive.io.warc.WARCReaderFactory;


import org.apache.hadoop.mapreduce.Reducer;

public  class EmailSumReducer
            extends Reducer<Text,Text,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        // only one key comes in for many values (one email many domains)
        public void reduce(Text key, Iterable<Text> values,  //key email value domain
                           Context context
        ) throws IOException, InterruptedException {
                    
            HashMap<Text, Integer> finalMaps = new HashMap<Text, Integer>();
            for (Text domain : values) {
                finalMaps.put(domain, new Integer(1)); // same domain overwrites
            }

            int sum = 0;
            for (Integer v : finalMaps.values()) {  //domanA 1, domainB 1, domainC 1
                sum += v.intValue(); // add all int values
            }

            result.set(sum);
            context.write(key, result);   //oneemail 3
        }
}
// all the domains that consists of this email e.g. andrew@gmail.com domainA 
// andrew@gmail// domain B  andrew@gmail.com domainA// andrew@gmail domain C

