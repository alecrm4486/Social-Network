// import the libraries
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.VertexRDD
/// Import the spark context and the file
val sc = spark.sparkContext
val graph = GraphLoader.edgeListFile(sc, "/Users/cecilia/Desktop/spark/Data.txt")
// verify the number of edges and vertex
graph.numVertices
graph.numEdges
/// Connected components
val connectedComponentsGraph: Graph[VertexId, Int] = graph.connectedComponents

def sortedConnectedComponents(connectedComponents: Graph[VertexId, _]): Seq[(VertexId, Long)] ={
	val componentCounts = connectedComponents.vertices.map(_._2).countByValue
	componentCounts.toSeq.sortBy(_._2).reverse
}

val componentCounts = sortedConnectedComponents(connectedComponentsGraph)

componentCounts.take(10).foreach(println)
/// Degree distribution
val degrees: VertexRDD[Int] = {
	graph.degrees.cache()
}.values.toDF("ID", "degree")   
degrees.map(_._2).stats()

degrees.take(15).foreach(println)
degrees.orderBy(desc("degrees")).show()
//the author with the highest number of degrees
graph.inDegrees.reduce((a,b) => if (a._2 > b._2) a else b)
//chacking the isolated groups
graph.inDegrees.filter(_._2 <= 1).count
graph.inDegrees.filter(_._2 <= 3).count
///Finding subpopulations
val dd = graph.connectedComponents()
dd.vertices.map(x => (x._2, Array(x._1))).reduceByKey((a,b) => a ++ b).values.map(_.mkString(" ")).collect.mkString(";")
///Counting the triangles
val g2 = Grapgh(graph.vertices, graph.edges.map(e => if (e.srcId < e.dstId) e else new Edge (e.dstId, e.srcId, e.attr))).partitionBy(PartitionStrategy.RandomVertexCut)
(0 to 3).map(i => g2.subgraph(vpred = (vid,_) => vid >= i*4693 && vid < (i+1)*4693).trianglesCount.vertices.map(_._2).reduce(_+_))
/// Computing the inDegrees
graph.inDegrees.reduce((a,b) => if (a._2 > b._2)a else b)
/// PageRank
val v = graph.pageRank(0.001).vertices
v.reduce((a,b) => if (a._2 > b._2) a else b)
v.vertices.take(10)
// personalized pageRank
graph.personalizedPageRank(53213, 0.001).vertices.filter(_._1 != 53213).reduce((a,b) => if (a._2 > b._2) a else b)
///Cluster Coefficient
val triCountGraph = graph.trianglesCount()
triCountGraph.vertices.map(x => x._2).stats()
val maxTrisGraph = Degrees.mapValues(d => d*(d - 1)/2.0)
val clusterCoef = triCountGraph.vertices.innerJoin(maxTrisGraph) {
	(vertexId, triCount, maxTris) => {
		if (maxTris == 0) 0 else triCount / maxTris
	}
}
clusterCoef.map(_._2).sum() / graph.vertices.count()










