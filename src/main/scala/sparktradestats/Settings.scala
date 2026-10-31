package sparktradestats

import com.typesafe.config.ConfigFactory
import java.nio.file.Paths

final case class Settings(inputPath: String, outputPath: String, threshold: BigDecimal)

object Settings {
  def load(path: String): Settings = {
    val config = ConfigFactory.parseFile(Paths.get(path).toFile).resolve().getConfig("trade-stats")
    val settings = Settings(
      config.getString("input-path"),
      config.getString("output-path"),
      BigDecimal(config.getString("clearing-notional-threshold"))
    )
    require(settings.inputPath.trim.nonEmpty && settings.outputPath.trim.nonEmpty, "Paths must not be empty")
    val input = Paths.get(settings.inputPath).toAbsolutePath.normalize()
    val output = Paths.get(settings.outputPath).toAbsolutePath.normalize()
    require(!input.startsWith(output) && !output.startsWith(input), "Input and output paths must not overlap")
    require(settings.threshold >= 0, "Threshold must be non-negative")
    require(settings.threshold.scale <= 2 && settings.threshold < BigDecimal(10).pow(22),
      "Threshold must fit DECIMAL(24,2)")
    settings
  }
}
