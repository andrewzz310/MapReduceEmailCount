package edu.seattleu.crawl.warc;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailCountMap {
	private static final Logger LOG = Logger.getLogger(EmailCountMap.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EMPTY_PAGE_TEXT,
		EXCEPTIONS,
		NON_PLAIN_TEXT
	}

	protected static class WordCountMapper extends Mapper<Text, ArchiveReader, Text, Text> {

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			//looop through each record
			for (ArchiveRecord r : value) { //every map instance maps one file and every file has many records
				try {
					if (!r.getHeader().getHeaderFields().get("WARC-Type").equals("response"))
						continue;
					String url = r.getHeader().getUrl();
					if (url == null )
						continue;

					URI uri = new URI(url);
					String host = uri.getHost();
					// extract second-level domain from host by going from left-right
					int pos = host.lastIndexOf('.');
					int pos2 = host.lastIndexOf('.', pos-1);
					String domain = host;
					if (pos2 > 0)
						domain = host.substring(pos2+1, host.length());

					byte[] rawData = rawData = IOUtils.toByteArray(r, r.available());
					String content = new String(rawData);

					//ALL MATCHES HOLDS all emails in the content of the record
					Pattern pattern = Pattern.compile("\\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,4}\\b",
							Pattern.CASE_INSENSITIVE);
					Matcher matcher = pattern.matcher(content);
					ArrayList<String> allMatches = new ArrayList<String>(); // every email found
					while (matcher.find()) {
						allMatches.add(matcher.group()); //when find email put in group
					}
					if (allMatches.size() == 0)
						continue;

					// generate all unique email in the content of that one record (many records to look through
					//that is why there needs to be a reduced step in the future
					HashMap<String, String> firstMaps = new HashMap<String, String>();
					for (String email : allMatches) {
						firstMaps.put(email, domain); // put every unique email found for that domain for that record.
														// next iteration of loop (NEXT RECORD) domain can be different or the same
					}

					for (String email: firstMaps.keySet()) {
						Text keyEmail = new Text();
						Text valueDomain = new Text();
						keyEmail.set(email);
						valueDomain.set(firstMaps.get(email)); //domain
						context.write(keyEmail, valueDomain); // write the emails (many emails for one domain per record, <key,value> pair.
					}//andrew@gmail.com domain a, andrew@gmail.com domain a, bob@gmail domain a, bob@gmail domain b
				}
				catch (Exception ex) {
					LOG.error("Caught Exception", ex);
					context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				}
			}
		}
	}
}
