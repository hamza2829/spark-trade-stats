package sparktradestats

import java.nio.file.Files
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TradeStatsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = LocalSpark.start("trade-stats-tests", "local[2]")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally super.afterAll()
  }

  private def row(changes: (String, String)*): Row = {
    val defaults = Map(
      "trade_id" -> "T1", "timestamp" -> "2026-09-01T09:30:00", "instrument" -> "ALFA",
      "side" -> "BUY", "price" -> "100.00", "quantity" -> "2", "venue" -> "VENUE_A",
      "clearing_member" -> "CM01", "_corrupt_record" -> null
    ) ++ changes
    Row.fromSeq((Ingest.columns :+ "_corrupt_record").map(defaults))
  }

  private def raw(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq, 2), Ingest.rawSchema)

  private def reasons(rows: Row*): Seq[String] =
    Ingest.clean(raw(rows: _*)).rejected.select("reason").collect().map(_.getString(0)).toSeq

  private def stats(rows: Row*): Row =
    Transform.dailyInstrumentStats(Ingest.clean(raw(rows: _*)).accepted).head()

  test("valid raw fields become a typed trade with an exact price") {
    val trade = Ingest.clean(raw(row())).accepted.head()
    assert(trade.trade_id == "T1" && trade.price == BigDecimal("100.00") && trade.quantity == 2)
  }

  test("whitespace around fields is trimmed") {
    val trade = Ingest.clean(raw(row("trade_id" -> " T1 ", "side" -> " BUY "))).accepted.head()
    assert(trade.trade_id == "T1" && trade.side == "BUY")
  }

  test("missing price is rejected") {
    assert(reasons(row("price" -> null)) == Seq("invalid_price"))
  }

  test("zero and negative prices are rejected") {
    assert(reasons(row("price" -> "0"), row("price" -> "-1.00")) == Seq.fill(2)("invalid_price"))
  }

  test("nonnumeric price, nonfinite price and decimal overflow are rejected") {
    assert(reasons(row("price" -> "oops"), row("price" -> "NaN"),
      row("price" -> "10000000000.00")).forall(_ == "invalid_price"))
  }

  test("prices with extra decimal places are rejected instead of rounded") {
    assert(reasons(row("price" -> "1.234")) == Seq("invalid_price"))
  }

  test("negative, zero and missing quantities are rejected") {
    assert(reasons(row("quantity" -> "-1"), row("quantity" -> "0"),
      row("quantity" -> null)) == Seq.fill(3)("invalid_quantity"))
  }

  test("fractional, overflowing and out-of-range quantities are rejected") {
    assert(reasons(row("quantity" -> "1.5"), row("quantity" -> "9223372036854775808"),
      row("quantity" -> "1000000001")) == Seq.fill(3)("invalid_quantity"))
  }

  test("wrong timestamp format and impossible calendar date are rejected") {
    assert(reasons(row("timestamp" -> "01/09/2026 09:30:00"),
      row("timestamp" -> "2026-02-30T09:30:00")) == Seq.fill(2)("invalid_timestamp"))
  }

  test("missing timestamp and trailing timestamp text are rejected") {
    assert(reasons(row("timestamp" -> null), row("timestamp" -> "2026-09-01T09:30:00junk")) ==
      Seq.fill(2)("invalid_timestamp"))
  }

  test("unknown and missing sides are rejected") {
    assert(reasons(row("side" -> "HOLD"), row("side" -> null)) == Seq.fill(2)("invalid_side"))
  }

  test("all required identifier fields are checked and reasons accumulate") {
    val reason = reasons(row("trade_id" -> "", "instrument" -> null,
      "venue" -> " ", "clearing_member" -> null)).head
    assert(reason.split(";").toSet == Set("missing_trade_id", "missing_instrument",
      "missing_venue", "missing_clearing_member"))
  }

  test("malformed CSV marker is retained with its reason") {
    val rejected = Ingest.clean(raw(row("_corrupt_record" -> "broken,row"))).rejected.head()
    assert(rejected.getAs[String]("_corrupt_record") == "broken,row")
    assert(rejected.getAs[String]("reason") == "malformed_csv")
  }

  test("duplicate trade IDs keep the earliest valid timestamp") {
    val result = Ingest.clean(raw(row("timestamp" -> "2026-09-01T10:00:00", "price" -> "200"), row()))
    assert(result.accepted.count() == 1 && result.accepted.head().price == BigDecimal(100))
    assert(result.rejected.head().getAs[String]("reason") == "duplicate_trade_id")
  }

  test("duplicate ties use consistent field ordering across repartitioning") {
    val rows = raw(row("price" -> "200"), row("price" -> "100"))
    assert(Ingest.clean(rows.repartition(1)).accepted.head().price == BigDecimal(100))
    assert(Ingest.clean(rows.repartition(3)).accepted.head().price == BigDecimal(100))
  }

  test("an invalid row with the same ID does not displace a valid row") {
    val result = Ingest.clean(raw(row("price" -> null), row()))
    assert(result.accepted.count() == 1)
    assert(result.rejected.head().getAs[String]("reason") == "invalid_price")
  }

  test("all input rows including exact duplicate copies are accounted for") {
    val input = raw(row(), row(), row(), row("trade_id" -> "T2", "quantity" -> "-1"))
    val result = Ingest.clean(input)
    assert(result.accepted.count() == 1 && result.rejected.count() == 3)
    assert(result.accepted.count() + result.rejected.count() == input.count())
  }

  test("VWAP is quantity-weighted, with correct totals and price range") {
    val result = stats(row(), row("trade_id" -> "T2", "price" -> "200.00", "quantity" -> "3", "side" -> "SELL"))
    assert(result.getAs[Long]("trade_count") == 2)
    assert(result.getAs[Long]("total_quantity") == 5)
    assert(result.getAs[java.math.BigDecimal]("notional").toPlainString == "800.00")
    assert(result.getAs[java.math.BigDecimal]("vwap").toPlainString == "160.000000")
    assert(result.getAs[java.math.BigDecimal]("high").compareTo(new java.math.BigDecimal("200")) == 0)
    assert(result.getAs[java.math.BigDecimal]("low").compareTo(new java.math.BigDecimal("100")) == 0)
    assert(result.getAs[Double]("buy_sell_ratio") == 1.0)
  }

  test("decimal notionals remain exact and VWAP rounds to six places") {
    val result = stats(row("price" -> "0.10", "quantity" -> "1"),
      row("trade_id" -> "T2", "price" -> "0.20", "quantity" -> "2"))
    assert(result.getAs[java.math.BigDecimal]("notional").toPlainString == "0.50")
    assert(result.getAs[java.math.BigDecimal]("vwap").toPlainString == "0.166667")
  }

  test("buy/sell ratio uses trade counts, not quantities") {
    val result = stats(row(), row("trade_id" -> "T2"),
      row("trade_id" -> "T3", "side" -> "SELL", "quantity" -> "999"))
    assert(result.getAs[Double]("buy_sell_ratio") == 2.0)
  }

  test("buy/sell ratio is null with no sells and zero with no buys") {
    val buysOnly = stats(row())
    assert(buysOnly.isNullAt(buysOnly.fieldIndex("buy_sell_ratio")))
    assert(stats(row("side" -> "SELL")).getAs[Double]("buy_sell_ratio") == 0.0)
  }

  test("statistics are isolated by UTC date and instrument") {
    val trades = Ingest.clean(raw(row(), row("trade_id" -> "T2", "timestamp" -> "2026-09-02T00:00:00"),
      row("trade_id" -> "T3", "instrument" -> "BRAV"))).accepted
    val result = Transform.dailyInstrumentStats(trades)
    assert(result.count() == 3 && result.filter(col("trade_count") =!= 1L).count() == 0)
  }

  test("threshold flag is strictly above, including equal and below boundary cases") {
    val trades = Ingest.clean(raw(row())).accepted // notional = 200.00
    for ((threshold, expected) <- Seq("199.99" -> true, "200.00" -> false, "200.01" -> false)) {
      val result = Transform.dailyClearingStats(trades, BigDecimal(threshold)).head()
      assert(result.getAs[Boolean]("above_threshold") == expected)
    }
  }

  test("clearing notional groups by member and date after deduplication") {
    val trades = Ingest.clean(raw(row(), row(), row("trade_id" -> "T2", "clearing_member" -> "CM02"),
      row("trade_id" -> "T3", "timestamp" -> "2026-09-02T09:00:00"))).accepted
    val result = Transform.dailyClearingStats(trades, BigDecimal(300))
    assert(result.count() == 3)
    assert(result.filter(col("above_threshold")).count() == 0)
  }

  test("CSV ingestion uses the declared schema and preserves bad price text") {
    val directory = Files.createTempDirectory("trade-input-")
    val file = directory.resolve("raw.csv")
    try {
      Files.writeString(file, Ingest.columns.mkString(",") + "\n" +
        "T1,2026-09-01T09:30:00,ALFA,BUY,not-a-price,2,VENUE_A,CM01\n")
      val input = Ingest.read(spark, directory.toString)
      assert(input.schema == Ingest.rawSchema)
      val rejected = Ingest.clean(input).rejected.head()
      assert(rejected.getAs[String]("price") == "not-a-price")
      assert(rejected.getAs[String]("reason") == "invalid_price")
    } finally {
      Files.deleteIfExists(file)
      Files.deleteIfExists(directory)
    }
  }

  test("a short CSV row is rejected for its missing required member") {
    val file = Files.createTempFile("trade-short-", ".csv")
    try {
      Files.writeString(file, Ingest.columns.mkString(",") + "\n" +
        "T1,2026-09-01T09:30:00,ALFA,BUY,100.00,2,VENUE_A\n")
      assert(Ingest.clean(Ingest.read(spark, file.toString)).rejected.head()
        .getAs[String]("reason").contains("missing_clearing_member"))
    } finally Files.deleteIfExists(file)
  }

  test("daily summary reconciles both instruments and member flags") {
    val trades = Ingest.clean(raw(row(), row("trade_id" -> "T2", "instrument" -> "BRAV"))).accepted
    val report = Output.summary(Transform.dailyInstrumentStats(trades),
      Transform.dailyClearingStats(trades, BigDecimal(300))).head()
    assert(report.getAs[Long]("trade_count") == 2 && report.getAs[Long]("total_quantity") == 4)
    assert(report.getAs[Long]("instrument_count") == 2 && report.getAs[Long]("flagged_members") == 1)
    assert(report.getAs[java.math.BigDecimal]("notional").toPlainString == "400.00")
  }

  test("empty input returns empty accepted, rejected and aggregate datasets") {
    val result = Ingest.clean(raw())
    assert(result.accepted.count() == 0 && result.rejected.count() == 0)
    assert(Transform.dailyInstrumentStats(result.accepted).count() == 0)
    assert(Transform.dailyClearingStats(result.accepted, BigDecimal(100)).count() == 0)
  }

  test("configuration reads threshold and rejects overlapping output paths") {
    val file = Files.createTempFile("trade-config-", ".conf")
    try {
      Files.writeString(file,
        "trade-stats { input-path = raw, output-path = reports, clearing-notional-threshold = 123.45 }")
      assert(Settings.load(file.toString).threshold == BigDecimal("123.45"))
      Files.writeString(file,
        "trade-stats { input-path = raw, output-path = raw/reports, clearing-notional-threshold = 123.45 }")
      intercept[IllegalArgumentException](Settings.load(file.toString))
    } finally Files.deleteIfExists(file)
  }
}
