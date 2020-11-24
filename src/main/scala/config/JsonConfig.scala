package config


import spray.json.{DefaultJsonProtocol, JsonFormat}

case class JsonConfig(inputPath:String ,inputFormat:String , outputFormat: String,
                      outputPath: String, service: String, clientId: String)


object JsonConfig extends DefaultJsonProtocol{

  //implicit val jsonConfig: JsonFormat[JsonConfig] = lazyFormat(jsonFormat6(JsonConfig))

}
