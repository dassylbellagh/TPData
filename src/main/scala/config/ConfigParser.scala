package config

import scopt.{DefaultOParserSetup, OParserBuilder, OParserSetup}


case class ConfigParser(inputPath: String = "D:\\microserviceTP\\untitled2\\data\\data.csv", inputFormat: String = "csv", outputFormat: String = "csv" , outputPath: String = "D:\\microserviceTP\\untitled2\\result\\ResultData.csv", service: String = "service1", clientId : String ="292494523"  )

object ConfigParser {

  private val setup: OParserSetup = new DefaultOParserSetup {
    override def showUsageOnError: Option[Boolean] = Some(true)
    override def errorOnUnknownArgument = false
  }
  import scopt.OParser
  val builder: OParserBuilder[ConfigParser] = OParser.builder[ConfigParser]

  val parser: OParser[Unit, ConfigParser] = {
    import builder._

    OParser.sequence(
      programName("GDPR Compliance"),

      opt[String]("inputPath")
        .required()
        .action((x, c) => c.copy(inputPath = x))
        .text("D:\\microserviceTP\\untitled2\\data\\data.csv"),

      opt[String]("inputFormat")
        .required()
        .action((x, c) => c.copy(inputFormat = x))
        .text("csv"),

      opt[String]("outputPath")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("D:\\microserviceTP\\untitled2\\result\\ResultData.csv"),

      opt[String]("outputFormat")
        .required()
        .action((x, c) => c.copy(outputFormat = x))
        .text("csv"),

      opt[String]("service")
        .required()
        .action((x, c) => c.copy(service = x))
        .text("service1"),

      opt[String]("clientId")
        .required()
        .action((x, c) => c.copy(service = x))
        .text("292494523")




    )
  }

  def parser(arguments: Array[String]): Option[ConfigParser] = {
    OParser.parse(ConfigParser.parser, arguments, ConfigParser(), setup)
  }

  def getConfigArgs(args: Array[String]): ConfigParser = {
    ConfigParser.parser(args) match {
      case Some(config) => config
      case _ => {
        print("cannot parse conf")
        sys.exit(1)
      }
    }
  }

}
