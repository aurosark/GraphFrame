//./bin/spark-shell --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11

//Code for Exploring graphs using GraphFrames section
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.Row
import org.graphframes._

//Code for Constructing a GraphFrame section
val edgesRDD = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Downloads/amzncopurchase/amazon0601.txt")
val schemaString = "src dst"
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false))
val edgesSchema = new StructType(fields)
val rowRDD = edgesRDD.map(_.split("\t")).map(attributes => Row(attributes(0).trim, attributes(1).trim))
val edgesDF = spark.createDataFrame(rowRDD, edgesSchema)
val srcVerticesDF = edgesDF.select($"src").distinct
val destVerticesDF = edgesDF.select($"dst").distinct
val verticesDF = srcVerticesDF.union(destVerticesDF).distinct.select($"src".alias("id"))
edgesDF.count()
verticesDF.count()
val g = GraphFrame(verticesDF, edgesDF)


//Code for Motif analysis using GraphFrames section
val motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show(5)
motifs.filter("b.id == 2").show()
val motifs3 = g.find("(a)-[e1]->(b); (a)-[e2]->(c)").filter("(b != c)")
motifs3.show(5)
val motifs3 = g.find("(a)-[]->(b); (a)-[]->(c)").filter("(b != c)")
motifs3.show()
motifs3.count()
val motifs3 = g.find("(a)-[]->(b); (a)-[]->(c); (b)-[]->(a)").filter("(b != c)")
motifs3.show()
motifs3.count()
val motifs3 = g.find("(a)-[]->(b); (c)-[]->(b)").filter("(a != c)")
motifs3.show(5)
motifs3.count()

val motifs3 = g.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(b)")
motifs3.show(5)
motifs3.count()

//Ensure you sufficient disk >100 GB and RAM >=14 GB or the following motifs4. 
//Alternatively you run the following on smaller subgraphs
val motifs4 = g.find("(a)-[e1]->(b); (c)-[e2]->(b); (c)-[e3]->(d)").filter("(a != c) AND (d != b) AND (d != a)")
motifs4.show(5)
motifs4.count()
