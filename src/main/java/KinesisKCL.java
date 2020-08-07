
/*
 *  Based on an example from AWS Kinesis examples on Github, that are licensed under Apache License 2.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class KinesisKCL {

  private static final String STREAM_NAME = "bob-test-15";
  private static final Region REGION = Region.US_WEST_2;

  private KinesisKCL() {
  }

  public static void main(String... args) {
    KinesisKCL kkcl = new KinesisKCL();
    kkcl.run();
  }

  private void run() {
    KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()
        .region(REGION));

    /*
     * Sets up configuration for the KCL, including DynamoDB and CloudWatch dependencies. The final argument, a
     * ShardRecordProcessorFactory, is where the logic for record processing lives, and is located in a private
     * class below.
     */
    DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(REGION).build();
    CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(REGION).build();

    ConfigsBuilder configsBuilder = new ConfigsBuilder(STREAM_NAME,
        STREAM_NAME,
        kinesisClient,
        dynamoClient,
        cloudWatchClient,
        // unique stream name to start over from the beginning of the queue.
        STREAM_NAME + UUID.randomUUID().toString(),
        new SampleRecordProcessorFactory()
    );

    Scheduler scheduler = new Scheduler(configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(STREAM_NAME, kinesisClient))
    );

    Thread schedulerThread = new Thread(scheduler);
    schedulerThread.setDaemon(true);
    schedulerThread.start();
  }

  private static class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {
    public ShardRecordProcessor shardRecordProcessor() {
      return new SampleRecordProcessor();
    }
  }

  private static class SampleRecordProcessor implements ShardRecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(SampleRecordProcessor.class);

    private static final String SHARD_ID_MDC_KEY = "ShardId";
    private long totalCount = 0;
    private String shardId;
    private PrintWriter pw = null;

    public void initialize(InitializationInput initializationInput) {
      try {
        pw = new PrintWriter("./kinesis.output");
      } catch (FileNotFoundException ex) {
        log.info("error {}", ex.getMessage(), ex);
        System.exit(1);
      }

      shardId = initializationInput.shardId();
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    /**
     * Handles record processing logic. The Amazon Kinesis Client Library will invoke this method to deliver
     * data records to the application. In this example we simply count and log our records.
     *
     * @param processRecordsInput Provides the records to be processed as well as information and capabilities
     * related to them (e.g. checkpointing).
     */
    public void processRecords(ProcessRecordsInput processRecordsInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Processing {} record(s)", processRecordsInput.records().size());
        for (KinesisClientRecord r : processRecordsInput.records()) {
          pw.println("record: " + StandardCharsets.UTF_8.decode(r.data()).toString());
          totalCount++;
          if (totalCount % 1000 == 0) {
            System.out.println("totalCount " + totalCount);
          }
        }
        pw.flush();
        processRecordsInput.records().forEach(r -> log.info("Processing record pk: {} -- Seq: {}",
            r.partitionKey(),
            r.sequenceNumber()
        ));

      } catch (Exception ex) {
        log.error("Caught exception while processing records.");
        Runtime.getRuntime().halt(1);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    /**
     * Called when the lease tied to this record processor has been lost. Once the lease has been lost,
     * the record processor can no longer checkpoint.
     *
     * @param leaseLostInput Provides access to functions and data related to the loss of the lease.
     */
    public void leaseLost(LeaseLostInput leaseLostInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Lost lease, so terminating.");
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    /**
     * Called when all data on this shard has been processed. Checkpointing must occur in the method for record
     * processing to be considered complete; an exception will be thrown otherwise.
     *
     * @param shardEndedInput Provides access to a checkpointer method for completing processing of the shard.
     */
    public void shardEnded(ShardEndedInput shardEndedInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Reached shard end checkpointing.");
        shardEndedInput.checkpointer().checkpoint();
      } catch (ShutdownException | InvalidStateException e) {
        log.error("Exception while checkpointing at shard end. Giving up.", e);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    /**
     * Invoked when Scheduler has been requested to shut down (i.e. we decide to stop running the app by pressing
     * Enter). Checkpoints and logs the data a final time.
     *
     * @param shutdownRequestedInput Provides access to a checkpointer, allowing a record processor to checkpoint
     * before the shutdown is completed.
     */
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Scheduler is shutting down, checkpointing.");
        shutdownRequestedInput.checkpointer().checkpoint();
      } catch (ShutdownException | InvalidStateException e) {
        log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }
  }
}
