package com.micromata.bigdata.github;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static void main(String[] args) {
    new Main().run(args);
  }

  private void run(String[] args) {
    LOG.debug("SparkContext: {}", sc);
    LOG.debug("SQLContext: {}", sql);
  }

  public void initializeSpark() {
    SparkConf conf = new SparkConf()
        .setAppName("Github conversion example.")
        .setMaster("local[*]");
    sc = new JavaSparkContext(conf);
    sql = new SQLContext(sc);
  }
}
