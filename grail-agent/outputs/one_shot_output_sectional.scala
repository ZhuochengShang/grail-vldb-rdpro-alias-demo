import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, DoubleType}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RaptorMixin._
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions
import edu.ucr.cs.bdlab.raptor.RasterVectorOperations
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, Feature, IFeature, RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal, RasterOperationsGlobal, RaptorJoin,RaptorJoinFeature }
import java.net.URI
import java.nio.file.{Paths, Files}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HPath}

object GeoJob {
  def run(sc: SparkContext): Unit = {
    val loadOpts = new BeastOptions()

    // TODO SECTION_INPUTS_START
    val rasterPaths: Seq[String] = Seq("file:///Users/clockorangezoe/Downloads/Annual_NLCD_LndCov_2024_CU_C1V1/Annual_NLCD_LndCov_2024_CU_C_1V1_clip.tif")
    val vectorPaths: Seq[String] = Seq("file:///Users/clockorangezoe/Downloads/boston_neighborhood_boundaries/Boston_Neighborhood_Boundaries_sample.shp")
    val primaryRasterPath: String = rasterPaths.headOption.getOrElse(throw new IllegalArgumentException("rasterPaths is empty"))
    val primaryVectorPathOpt: Option[String] = vectorPaths.headOption
    val outputDir: String = "file:///Users/clockorangezoe/Documents/phd_projects/code/geoAI/LangGraph/Notebook/ui_mock_runs/agent_runs/generated_output"
    val outputBasename: String = "output"
    val outputExtension: String = ".csv"
    val outputRunTag: String = java.util.UUID.randomUUID().toString.replace("-", "")
    val outputPath: String = s"${outputDir}/${outputBasename}_${outputRunTag}${outputExtension}"
// TODO SECTION_INPUTS_END

    // TODO SECTION_LOAD_DATA_START
// Shared read options for raster loaders (reuse this variable; do not redeclare inside match/case)

    // Load the primary raster using the geoTiff method
    val raster: RasterRDD[Int] = sc.geoTiff(primaryRasterPath, 0)

    // Load the primary vector using the shapefile method
    val vector: RDD[IFeature] = sc.shapefile(primaryVectorPathOpt.getOrElse(
      throw new IllegalArgumentException("Primary vector path is not defined")
    ))

    println("__STEP__ LOAD_DATA_done")
// TODO SECTION_LOAD_DATA_END

    // TODO SECTION_TYPE_CHECK_START
// check pixel/schema/data types
val firstRaster = raster.take(1).headOption.getOrElse(
  throw new IllegalArgumentException("Raster is empty"))
if (firstRaster.pixelType != IntegerType) {
  throw new RuntimeException(s"Expected IntegerType but got ${firstRaster.pixelType}")
}
println("__STEP__ TYPE_CHECK_done")
// Assigned vars: raster, vector
// TODO SECTION_TYPE_CHECK_END

    // TODO SECTION_SPATIAL_CHECK_START
    // check CRS + metadata compatibility
    println("__STEP__ SPATIAL_CHECK_done")
    // TODO SECTION_SPATIAL_CHECK_END

    // TODO SECTION_TRANSFORM_START
    println("__STEP__ TRANSFORM_done")
    // TODO SECTION_TRANSFORM_END

    // TODO SECTION_RASTER_VECTOR_JOIN_START
val joinedRecords = raster.raptorJoin(vector)
println("__STEP__ RASTER_VECTOR_JOIN_done")
// Assigned vars: raster, vector
// Assigned vars: firstRaster
// TODO SECTION_RASTER_VECTOR_JOIN_END

    // TODO SECTION_ANALYTICS_START
val aggregatedResults = joinedRecords
  .map { record =>
    val zoneId = record.feature.getAs[String]("name") // Assuming "name" is the zone identifier
    val rasterValue = record.m
    (zoneId, rasterValue)
  }
  .aggregateByKey((0, 0))(
    (acc, value) => (acc._1 + value, acc._2 + 1),
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )
  .mapValues { case (sum, count) =>
    val average = if (count > 0) sum.toDouble / count else 0.0
    (sum, count, average)
  }

aggregatedResults
  .map { case (zoneId, (sum, count, average)) =>
    s"$zoneId,$sum,$count,$average"
  }
  .saveAsTextFile(outputPath)

println("__STEP__ ANALYTICS_done")
// Assigned vars: firstRaster
// Assigned vars: joinedRecords
// TODO SECTION_ANALYTICS_END

    // TODO SECTION_OUTPUT_START
    // save GeoTIFF
    println("__STEP__ OUTPUT_done")
    // TODO SECTION_OUTPUT_END
  }
}

object GeoJobMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GeoJobMain")
      .master("local[*]")
      .getOrCreate()
    try {
      GeoJob.run(spark.sparkContext)
      println("__DONE__ object=GeoJob")
    } finally {
      spark.stop()
    }
  }
}
