package sparktradestats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Output {
  /** Pure report construction; exactly one row per trading date. */
  def summary(instruments: DataFrame, members: DataFrame): DataFrame = {
    val daily = instruments.groupBy("date").agg(
      sum("trade_count").as("trade_count"), sum("total_quantity").as("total_quantity"),
      sum("notional").as("notional"), count(lit(1)).as("instrument_count")
    )
    val flags = members.groupBy("date").agg(
      sum(when(col("above_threshold"), 1L).otherwise(0L)).as("flagged_members")
    )
    daily.join(flags, Seq("date")).orderBy("date")
  }

  /** Side effects live here; Spark CSV output is a folder with one part CSV. */
  def write(instruments: DataFrame, members: DataFrame, rejected: DataFrame,
      report: DataFrame, reconciliation: DataFrame, path: String): Unit = {
    instruments.write.mode("overwrite").partitionBy("date").parquet(s"$path/instrument_daily")
    members.write.mode("overwrite").partitionBy("date").parquet(s"$path/clearing_daily")
    rejected.write.mode("overwrite").parquet(s"$path/rejected")
    report.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$path/summary")
    reconciliation.coalesce(1).write.mode("overwrite").option("header", "true")
      .json(s"$path/reconciliation")
  }
}
