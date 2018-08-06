// Andrew Yiyun Zhu
// Prcessing Web Data with AWS EMR (Elastic MapReduce)


This project's task is to extrapolate every email address that appears on any web page and output the email address
and the number of domains on which that email address is found. The data is from the common-crawl's public dataset of web-crawl data
made available on AWS-S3. The segment used is s3://commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084886237.6/warc.
The project is done using AWS-EMR(Elastic Map Reduce) combined with Hadoop's Map Reduce framework.


Source Code:
Main Class: edu/seattleu/crawl/warc/WarcEmailCount   

"WarcEmailCount.java"--- This class is the main class to run as argument[0] in EMR. The class essentially runs the job
by finding the WARC file in question and assigning that as the input path (path in S3 where WARC data is stored) 
It then sets the final path output of where the output files (result) will be saved. 
In this case for the second argument[1] on EMR: s3//<bucket>/output



"EmailCountMap.java"  edu/seattleu/crawl/warc/EmailCountMap

This class essentially acts as the mapper for the Map-Reduce job of the data. 
It iterates thorugh each record in the file and finds all the emails in a given domain for that record.
Since there are many records in each file, there can be many duplicate emails for a given domain. 
Example: first record: domain = google.com, there can be andrew@gmail.com bob@gmail.com, second iteration/record:
domain = google.com again, there can be randy@gmail.com, andrew@gmail.com.
As a result there can be many duplicate pairs of <email,domain> as the output rather than
having unique emails for every domain since there are many records in a file. Thus, we move on to the next step (reduce) in order to solve this.


"EmailSumRducer.java" edu/seattleu/crawl/warc/EmailSumReducer

This reducer class takes input of one email and many domains <key,value>. It then removes duplicate domains
for that one email if there are any. It then is able to add up all the counts of unique domains for that one 
email and thus writing to our output and final result which is how many unique domains does a given
email address show up on the WARC dataset sergment in common-crawl.



How to run in mapReduce

1) Make new bucket on S3
2) Place the jar file <> into that bucket
3) create cluster in EMR and create custom jar file
4) point to the jar location to where you created the bucket.
5) arg[0] edu/seattleu/crawl/warc/WarcEmailCount  
6) arg[1] <output path>
 

