package com.micromata.bigdata.github;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Entry point.
 *
 * @author Michael Lesniak (mlesniak@micromata.de)
 */
public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  private JavaSparkContext sc;
  private SQLContext sql;

  public Main() {
    initializeSpark();
  }

  public static void main(String[] args) throws IOException {
    new Main().run(args);
  }

  private void run(String[] args) throws IOException {
    checkArgs(args);

    LOG.debug("SparkContext: {}", sc);
    LOG.debug("SQLContext: {}", sql);

    String inputPath = args[0];
    String outputPath = args[1];

    FileSystem fs = FilesystemUtils.getFileSystem(inputPath);
    RemoteIterator<LocatedFileStatus> inputFiles = fs.listFiles(new Path(inputPath), false);

    while (inputFiles.hasNext()) {
      LocatedFileStatus status = inputFiles.next();
      convert(outputPath, status.getPath());
    }
  }

  private void convert(String outputPath, Path inputFile) {
    LOG.info("Loading {}", inputFile);
    DataFrame frame = sql.jsonFile(inputFile.toString());
    String outputFile = outputPath + File.separator + inputFile.getName() + FilesystemUtils.PARQUET_SUFFIX;
    LOG.info("Storing as parquet file at {}", outputFile);
    frame.saveAsParquetFile(outputFile);
    LOG.info("Finished");
  }

  private void checkArgs(String[] args) {
    if (args.length < 2) {
      LOG.error("Not enough arguments");
      System.exit(1);
    }
  }

  public void initializeSpark() {
    SparkConf conf = new SparkConf()
        .setAppName("Github conversion example.")
        .setMaster("local[*]");
    sc = new JavaSparkContext(conf);
    sql = new SQLContext(sc);
  }
}
