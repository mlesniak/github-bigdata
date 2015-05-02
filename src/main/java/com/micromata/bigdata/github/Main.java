package com.micromata.bigdata.github;

import org.apache.commons.io.FileUtils;
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
    LOG.debug("SparkContext: {}", sc);
    LOG.debug("SQLContext: {}", sql);

    LOG.info("Loading");
    DataFrame frame = sql.jsonFile("data/raw/2015-01-01-15.json.gz");
    LOG.info("Storing as parquet file.");
    FileUtils.deleteDirectory(new File("data/processed/2015-01-01-15.json.gz.parquet"));
    frame.saveAsParquetFile("data/processed/2015-01-01-15.json.gz.parquet");
    LOG.info("Done.");
  }

  public void initializeSpark() {
    SparkConf conf = new SparkConf()
        .setAppName("Github conversion example.")
        .setMaster("local[*]");
    sc = new JavaSparkContext(conf);
    sql = new SQLContext(sc);
  }
}
