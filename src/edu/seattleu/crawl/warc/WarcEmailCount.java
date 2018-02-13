package edu.seattleu.crawl.warc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.warc.WARCFileInputFormat;

import java.net.URI;

/**
 * Main Class to extract file from S3 from the Common Crawl dataset.
 * @author Andrew Zhu
 */
public class WarcEmailCount extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WarcEmailCount.class);
	
	/**
	 * Main entry point that uses the ToolRunner class to run the Hadoop job.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WarcEmailCount(), args);
		System.exit(res);
	}

	/**
	 * Builds and runs the Hadoop job.
	 * @return	0 if the Hadoop job completes successfully and 1 otherwise.
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJarByClass(WarcEmailCount.class);
		
		LOG.info("SEATTLE WARC SAMPLE");
		
		//INPUt
		
		// This one runs 1 file
		String inputPath = "s3://commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084886237.6/warc/CC-MAIN-20180116070444-20180116090444-0000?.warc.gz";
		//This one runs the first 10 files of the segment
		//String inputPath = "s3://commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084886237.6/warc/CC-MAIN-20180116070444-20180116090444-00000.warc.gz";

		LOG.info("Input path: '" + inputPath + "'");
		
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// Output path in the file
		String outputPath = arg0[1];

		LOG.info("Clearing the output path at '" + outputPath + "'");
		FileSystem fs = FileSystem.get(new URI(outputPath), conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}

		// path where final output 'part' files will be saved.
		LOG.info("Output path: '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);
				
		job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(EmailCountMap.WordCountMapper.class);
		job.setReducerClass(EmailSumReducer.class);

		//Set number of reducers optional for clean output for smaller files
		// job.setNumReduceTasks(1);
	    
		// allow small number of maps to fail without failing execution
		conf.set("mapreduce.map.failures.maxpercent", "50");
		conf.set("mapreduce.map.maxattempts", "1");
	    
		// Submit the job, then poll for progress until the job is complete
		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}
}
