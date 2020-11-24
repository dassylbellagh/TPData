package services


import org.apache.spark.sql.catalyst.expressions.Murmur3Hash
import org.apache.spark.sql.functions.{col, concat, hash}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.collection.mutable
import scala.io.Source

object Service2 {
  def implemService2(df : DataFrame) : DataFrame= {

    println("Start Service 2")
    df.show()

    //Récupérer les ligne du client 292494523
    var dfFiltrer = df.filter(col("ClientID") === 292494523)

    //hasher colonne par colonne
    var df1 = dfFiltrer.withColumn("Region", hash(dfFiltrer.columns.map(col):_*))
    var df2 = df1.withColumn("Country", hash(df1.columns.map(col):_*))
    var df3 = df2.withColumn("Item Type", hash(df2.columns.map(col):_*))
    var df4 = df3.withColumn("Sales Channel", hash(df3.columns.map(col):_*))
    var df5 = df4.withColumn("Order Priority", hash(df4.columns.map(col):_*))
    var df6 = df5.withColumn("Order Date", hash(df5.columns.map(col):_*))
    var df7 = df6.withColumn("ClientID", hash(df6.columns.map(col):_*))
    var df8 = df7.withColumn("Ship Date", hash(df7.columns.map(col):_*))
    var df9 = df8.withColumn("Units Sold", hash(df8.columns.map(col):_*))
    var df10 = df9.withColumn("Unit Price", hash(df9.columns.map(col):_*))
    var df11 = df10.withColumn("Unit Cost", hash(df10.columns.map(col):_*))
    var df12 = df11.withColumn("Total Revenue", hash(df11.columns.map(col):_*))
    var df13 = df12.withColumn("Total Cost", hash(df12.columns.map(col):_*))
    var df14 = df13.withColumn("Total Profit", hash(df13.columns.map(col):_*))

    println("hash colonne par colonne, dans cet exemple j'ai a pris 2 colonnes car ça bouffe trop de RAM d'afficher toutes les colonnes")
    df2.show()

    //Récupérer les lignes sauf celles du client 292494523
    var dfFilter2 = df.filter(col("ClientID") =!= 292494523)

    //Faire un union des deux : sans le client + avec le client dont les donnes sont hashées
    var res = dfFilter2.union(df2)
    return res
  }

}
