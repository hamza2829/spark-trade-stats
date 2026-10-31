package sparktradestats

import org.apache.spark.sql.SparkSession

object LocalSpark {
  def start(appName: String, master: String = "local[*]"): SparkSession = {
    val builder = SparkSession.builder().appName(appName).master(master)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.ansi.enabled", "true")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .config("spark.sql.csv.parser.columnPruning.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.ui.enabled", "false")
    // Spark's default local filesystem requires native helpers on Windows.
    if (sys.props("os.name").startsWith("Windows")) {
      builder.config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
    }
    builder.getOrCreate()
  }
}
