import java.util.logging.{Level, Logger}

import config.ConfigParser

import org.apache.spark.sql.SparkSession
import reader.{ConfigReader, SparkReaderWriter}
import services.{Service1, Service2, Service3}

object SampleApplication {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val configCli: ConfigParser = ConfigParser.getConfigArgs(args)

    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    import org.apache.spark.sql.functions._


    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat)

    //Service 1
    var resService1 = Service1.implemService1(df)
    SparkReaderWriter.writeData(resService1,configCli.outputPath, configCli.outputFormat)

    //Service 2
    var resService2 = Service2.implemService2(df)
    SparkReaderWriter.writeData(resService2,configCli.outputPath, configCli.outputFormat)

    //Service 3
    var resService3 = Service3.implemService3(df)
    SparkReaderWriter.writeData(resService3,configCli.outputPath, configCli.outputFormat)

    //TO DO : Send email

    // val ListFiles = configCli.outputPath.mkString(";")
    //val msg = "Bonjour, Voici les informations vous concernant !"
    //val obj = new Email(subject = "Client information",
    //  from = EmailAddress("dassyl bellagh", "dassyl.bellagh@gmail.com"),
    //  text = "Bonjour, voici les informations vous concernant !")
    //  .to("Edassyl bellagh TO", "dassyl.bellagh@gmail.com")
    //obj.sendMail(msg, spark.sparkContext.applicationId, "", "","",ListFiles)

  }
}