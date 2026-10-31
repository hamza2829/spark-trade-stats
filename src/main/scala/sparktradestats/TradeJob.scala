package sparktradestats

import org.apache.spark.sql.functions._

object TradeJob {
  def main(args: Array[String]): Unit = {
    require(args.length <= 1, "Usage: TradeJob [config/application.conf]")
    val settings = Settings.load(args.headOption.getOrElse("config/application.conf"))
    val spark = LocalSpark.start("spark-trade-stats")
    import spark.implicits._
    try {
      val raw = Ingest.read(spark, settings.inputPath).cache()
      val inputCount = raw.count()
      require(inputCount > 0, "Input contains no trade rows")
      val cleaned = Ingest.clean(raw)
      val accepted = cleaned.accepted.cache()
      val rejected = cleaned.rejected.cache()
      val acceptedCount = accepted.count()
      val rejectedCount = rejected.count()
      require(inputCount == acceptedCount + rejectedCount, "Input reconciliation failed")
      require(acceptedCount > 0, "No valid trades remain; inspect the input before running again")
      val instruments = Transform.dailyInstrumentStats(accepted).cache()
      val members = Transform.dailyClearingStats(accepted, settings.threshold).cache()
      val report = Output.summary(instruments, members)
      val reconciliation = Seq((inputCount, acceptedCount, rejectedCount, true))
        .toDF("input_rows", "accepted_rows", "rejected_rows", "balanced")
      Output.write(instruments, members, rejected, report, reconciliation, settings.outputPath)

      // Read actual Parquet output back and reconcile both independent aggregations.
      val writtenInstruments = spark.read.parquet(s"${settings.outputPath}/instrument_daily")
      val writtenMembers = spark.read.parquet(s"${settings.outputPath}/clearing_daily")
      val writtenRejected = spark.read.parquet(s"${settings.outputPath}/rejected")
      val expectedTotals = accepted.toDF().agg(
        count(lit(1)), sum("quantity"),
        sum(expr("cast(price as decimal(12,2)) * cast(quantity as decimal(10,0))"))
      ).head()
      val actualTotals = writtenInstruments.agg(
        sum("trade_count"), sum("total_quantity"), sum("notional")
      ).head()
      require(expectedTotals.toSeq == actualTotals.toSeq, "Instrument output totals differ from accepted trades")
      val clearingTotals = writtenMembers.agg(sum("trade_count"), sum("daily_notional")).head()
      require(clearingTotals.getLong(0) == acceptedCount &&
        clearingTotals.getDecimal(1).compareTo(expectedTotals.getDecimal(2)) == 0,
        "Clearing output totals differ from accepted trades")
      require(writtenRejected.count() == rejectedCount, "Rejected output count differs")

      println(s"Reconciled: input=$inputCount accepted=$acceptedCount rejected=$rejectedCount")
      println(s"Verified Parquet: quantity=${actualTotals.getLong(1)} notional=${actualTotals.getDecimal(2)}")
      rejected.groupBy("reason").count().orderBy("reason").show(false)
      writtenInstruments.orderBy("date", "instrument").show(5, truncate = false)
      report.show(20, truncate = false)
      println(s"Instrument rows=${writtenInstruments.count()}, clearing rows=${writtenMembers.count()}, " +
        s"flagged member-days=${writtenMembers.filter(col("above_threshold")).count()}")
    } finally {
      spark.catalog.clearCache()
      spark.stop()
    }
  }
}
