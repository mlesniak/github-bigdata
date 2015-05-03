package com.micromata.bigdata.github;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple utility functions for the HDFS system.
 *
 * @author Michael Lesniak (mlesniak@micromata.de)
 */
public class FilesystemUtils {
  public static final String PARQUET_SUFFIX = ".parquet";

  /**
   * Only static methods.
   */
  private FilesystemUtils() {
    // Empty.
  }

  /**
   * Helper function to create a path object with correct file separators.
   *
   * @param parts the parts
   * @return the path
   */
  public static Path path(String... parts) {
    return new Path(StringUtils.join(parts, File.separator));
  }

  /**
   * Gets the current file system for HDFS.
   * <p>
   * This works for both file:/// and hdfs:///.
   *
   * @param path the path to use as the base for determining the database schema.
   * @return the file system
   * @throws IOException if the file system could not be configured.
   */
  public static FileSystem getFileSystem(String path) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", path);
    return FileSystem.get(conf);
  }

  /**
   * Ermittelt ein nutzbares Dateisystem anhand des angegebenen Pfades.
   *
   * @param path Pfad
   * @return Dateisystem-Objekt
   * @throws IOException bei IO-Fehler.
   */
  public static FileSystem getFileSystem(Path path) throws IOException {
    return getFileSystem(path.toString());
  }

  /**
   * Gibt eine Liste von Verzeichnissen in einem Pfad zurück.
   *
   * @param parentPath Das Verzeichnis, in dem gesucht werden soll.
   * @return the list of paths.
   * @throws IOException if an IO error occurred.
   */
  public static List<Path> listDirectories(String parentPath) throws IOException {
    FileStatus[] files = FilesystemUtils.getFileSystem(parentPath).listStatus(new Path(parentPath));
    return Arrays.asList(files).stream()
        .filter(FileStatus::isDirectory)
        .map(FileStatus::getPath)
        .collect(Collectors.toList());
  }

  /**
   * Gibt eine Liste von Dateien in einem Pfad zurück.
   *
   * @param path Das Verzeichnis, in dem gesucht werden soll.
   * @return the list of FileStatus containing file information.
   * @throws IOException if an IO error occurred.
   */
  public static List<FileStatus> listFiles(String path) throws IOException {
    return Arrays.asList(FilesystemUtils.getFileSystem(path).listStatus(new Path(path)));
  }

  /**
   * Gibt eine Liste von Dateien in einem Pfad zurück.
   *
   * @param path Das Verzeichnis, in dem gesucht werden soll.
   * @return the list of FileStatus containing file information.
   * @throws IOException if an IO error occurred.
   */
  public static List<FileStatus> listFiles(Path path) throws IOException {
    return Arrays.asList(FilesystemUtils.getFileSystem(path).listStatus(path));
  }
}
