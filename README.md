## Kinesis KCL 2 Example 

This program is a very hacked up version of a sample from the AWS Github samples. 
It uses KCL v2 to read a Kinesys queue.

There are no command line arguments.  The Kinesis Stream name is hard-coded.  Credentials and the region are picked up from the credential provider which is "under the hood" in the KCL library. 

I used [StreamSets](https://streamsets.com) Data Collector (download here: http://archives.streamsets.com/) with a test pipeline of Dev Data Generator to Kinesis Producer.  

###Note: 
This program creates an application name of the Kinesis Stream Name with a suffix of the
epoch time in ms.  Using a new application name each time restarts reading the stream from
the beginning, but it leaves a lot of left-over DynamoDB tables, as these are not cleaned up
in the KCL (you man want that info to restart reading.

If you want to test checkpointing, remove the epoch ms from the Application Name,
then the checkpointing should work.

In addition to being charged for using Kinesis, you will incur AWS changes for use of DynamoDB
and for these extra DynamoDB tables; ensure you delete them after testing.  Also, i think you will be charged for publishing to Cloudwatch

###Building the program: 
Download, and build the program (mvn is set up to build a "fat jar"):
```bash
git clone https://github.com/rcprcp/KinesisKCL.git
cd KinesisKCL
mvn clean package 
```
Make sure your AWS credentials and region are setup. 

Run the program: 
```bash
java -jar target/KinesisKCL-1.0-SNAPSHOT-jar-with-dependencies.jar
```
* program logs go to ./kinesisKCL.log[1-5]
* the program's output - the data records are sent to: ./kinesis.output
* "progress" messages will be sent to stdout.
