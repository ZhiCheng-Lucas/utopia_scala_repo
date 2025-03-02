package com.utopia.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import scala.util.Try

/** Camera Detection Analytics for Town of Utopia
  *
  * This application processes camera detection events and geographical location
  * data to identify the most popular detected items in each location.
  */
object CameraDetectionAnalytics {

  // Case classes for type safety
  case class DetectionEvent(
      geographical_location_oid: Long,
      video_camera_oid: Long,
      detection_oid: Long,
      item_name: String,
      timestamp_detected: Long
  )

  case class GeographicalLocation(
      geographical_location_oid: Long,
      geographical_location: String
  )

  // Updated to use location name instead of location_oid
  case class ItemRanking(
      geographical_location: String,
      item_rank: Int,
      item_name: String
  )

  /** Main method to run the application
    *
    * @param args
    *   command line arguments: args(0): path to detection events parquet file
    *   (Dataset A) args(1): path to geographical locations parquet file
    *   (Dataset B) args(2): output path for rankings args(3): number of top
    *   items to return per location (default: 10)
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("""
          |Usage: CameraDetectionAnalytics <detection_events_path> <geographical_locations_path> <output_path> [top_n]
          |  detection_events_path: Path to detection events parquet file (Dataset A)
          |  geographical_locations_path: Path to geographical locations parquet file (Dataset B)
          |  output_path: Path to write output rankings parquet file
          |  top_n: Number of top items to return per location (default: 10)
        """.stripMargin)
      System.exit(1)
    }

    // Parse command line arguments
    val detectionEventsPath = args(0)
    val geoLocationsPath = args(1)
    val outputPath = args(2)
    val topN = if (args.length > 3) args(3).toInt else 10

    // Initialize Spark session
    val spark = SparkSession
      .builder()
      .appName("Camera Detection Analytics")
      .getOrCreate()

    try {
      // Set log level to reduce verbosity
      spark.sparkContext.setLogLevel("WARN")

      // Process the data
      processData(
        spark,
        detectionEventsPath,
        geoLocationsPath,
        outputPath,
        topN
      )

      println(s"Analysis complete! Results written to: $outputPath")
    } catch {
      case e: Exception =>
        System.err.println(s"Error processing data: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  /** Process detection events and geographical location data to produce
    * rankings
    *
    * @param spark
    *   SparkSession
    * @param detectionEventsPath
    *   path to detection events parquet file
    * @param geoLocationsPath
    *   path to geographical locations parquet file
    * @param outputPath
    *   output path for rankings
    * @param topN
    *   number of top items to return per location
    */
  def processData(
      spark: SparkSession,
      detectionEventsPath: String,
      geoLocationsPath: String,
      outputPath: String,
      topN: Int
  ): Unit = {
    import spark.implicits._

    // Read datasets using DataFrame API
    val detectionEventsDf = spark.read.parquet(detectionEventsPath)
    val geoLocationsDf = spark.read.parquet(geoLocationsPath)

    // Convert DataFrames to RDDs for transformation
    val detectionEventsRdd = convertToDetectionEventsRdd(detectionEventsDf)
    val geoLocationsRdd = convertToGeoLocationsRdd(geoLocationsDf)

    // Implement RDD transformations
    val rankingsRdd = processRdds(detectionEventsRdd, geoLocationsRdd, topN)

    // Convert RDD to DataFrame for writing to Parquet
    val rankingsSchema = StructType(
      Seq(
        StructField("geographical_location", StringType, nullable = false),
        StructField("item_rank", IntegerType, nullable = false),
        StructField("item_name", StringType, nullable = false)
      )
    )

    val rankingsRows = rankingsRdd.map {
      case ItemRanking(locName, rank, itemName) =>
        Row(locName, rank, itemName)
    }

    val rankingsDf = spark.createDataFrame(rankingsRows, rankingsSchema)

    // Write results to Parquet as a single file
    rankingsDf.coalesce(1).write.mode("overwrite").parquet(outputPath)
  }

  /** Convert detection events DataFrame to RDD
    *
    * @param df
    *   detection events DataFrame
    * @return
    *   RDD of DetectionEvent objects
    */
  def convertToDetectionEventsRdd(df: DataFrame): RDD[DetectionEvent] = {
    df.rdd.map { row =>
      DetectionEvent(
        geographical_location_oid =
          row.getAs[Long]("geographical_location_oid"),
        video_camera_oid = row.getAs[Long]("video_camera_oid"),
        detection_oid = row.getAs[Long]("detection_oid"),
        item_name = row.getAs[String]("item_name"),
        timestamp_detected = row.getAs[Long]("timestamp_detected")
      )
    }
  }

  /** Convert geographical locations DataFrame to RDD
    *
    * @param df
    *   geographical locations DataFrame
    * @return
    *   RDD of GeographicalLocation objects
    */
  def convertToGeoLocationsRdd(df: DataFrame): RDD[GeographicalLocation] = {
    df.rdd.map { row =>
      GeographicalLocation(
        geographical_location_oid =
          row.getAs[Long]("geographical_location_oid"),
        geographical_location = row.getAs[String]("geographical_location")
      )
    }
  }

  /** Process RDDs to generate rankings
    *
    * @param detectionEventsRdd
    *   RDD of detection events
    * @param geoLocationsRdd
    *   RDD of geographical locations
    * @param topN
    *   number of top items to return per location
    * @return
    *   RDD of item rankings
    */
  def processRdds(
      detectionEventsRdd: RDD[DetectionEvent],
      geoLocationsRdd: RDD[GeographicalLocation],
      topN: Int
  ): RDD[ItemRanking] = {
    // Cache geoLocationsRdd as it's small and will be used for a join
    val cachedGeoLocationsRdd =
      geoLocationsRdd.persist(StorageLevel.MEMORY_AND_DISK)

    // Step 1: Deduplicate detection events by detection_oid
    // For each detection_oid, keep only the first occurrence
    val dedupDetectionsRdd = deduplicate(detectionEventsRdd)

    // Step 2: Count items by geographical location
    val countsByLocationAndItem = countItemsByLocation(dedupDetectionsRdd)

    // Step 3: Prepare geographical locations for join
    // Map to (location_oid, location_name)
    val locationMap = cachedGeoLocationsRdd.map(loc =>
      (loc.geographical_location_oid, loc.geographical_location)
    )

    // Step 4: Generate rankings with location names
    val rankingsRdd = generateRankingsWithLocationNames(
      countsByLocationAndItem,
      locationMap,
      topN
    )

    // Unpersist cached RDD
    cachedGeoLocationsRdd.unpersist()

    rankingsRdd
  }

  /** Deduplicate detection events by detection_oid
    *
    * @param detectionEventsRdd
    *   RDD of detection events
    * @return
    *   RDD of deduplicated detection events
    */
  def deduplicate(
      detectionEventsRdd: RDD[DetectionEvent]
  ): RDD[DetectionEvent] = {
    // Group by detection_oid and keep only the first event for each detection_oid
    detectionEventsRdd
      .map(event => (event.detection_oid, event))
      .reduceByKey((event1, event2) => event1) // Keep the first one
      .values
  }

  /** Count items by geographical location
    *
    * @param detectionEventsRdd
    *   RDD of deduplicated and optimized detection events
    * @return
    *   RDD of ((location_oid, item_name), count)
    */
  def countItemsByLocation(
      detectionEventsRdd: RDD[DetectionEvent]
  ): RDD[((Long, String), Int)] = {
    detectionEventsRdd
      .map(event => ((event.geographical_location_oid, event.item_name), 1))
      .reduceByKey(_ + _)
  }

  /** Generate rankings for each location with location names
    *
    * @param countsByLocationAndItem
    *   RDD of ((location_oid, item_name), count)
    * @param locationMap
    *   RDD of (location_oid, location_name)
    * @param topN
    *   number of top items to return per location
    * @return
    *   RDD of item rankings with location names
    */
  def generateRankingsWithLocationNames(
      countsByLocationAndItem: RDD[((Long, String), Int)],
      locationMap: RDD[(Long, String)],
      topN: Int
  ): RDD[ItemRanking] = {
    // Step 1: Transform counts to (location_oid, (item_name, count))
    val countsByLocation = countsByLocationAndItem
      .map { case ((locId, itemName), count) => (locId, (itemName, count)) }

    // Step 2: Group items by location_oid and calculate rankings
    val rankingsByLocationId = countsByLocation
      .groupByKey()
      .flatMap { case (locId, itemsWithCounts) =>
        // Sort items by count in descending order and take top N
        val sortedItems = itemsWithCounts.toSeq
          .sortBy(_._2)(Ordering[Int].reverse)
          .take(topN)

        // Create (location_oid, (rank, item_name)) pairs
        sortedItems.zipWithIndex.map { case ((itemName, _), idx) =>
          (locId, (idx + 1, itemName))
        }
      }

    // Step 3: Join with location map to get location names
    val rankingsWithLocationNames = rankingsByLocationId
      .join(locationMap)
      .map { case (_, ((rank, itemName), locationName)) =>
        ItemRanking(locationName, rank, itemName)
      }

    rankingsWithLocationNames
  }
}
