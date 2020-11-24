package services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object Service3 {

  def implemService3(df : DataFrame) : DataFrame = {

    var dfFiltrer = df.filter(col("ClientID") === 292494523)
    println("Service 3 Récupération des données du client 292494523")
    dfFiltrer.show()
    return df

  }

}
