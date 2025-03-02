package com.utopia.analytics

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

/** Unit tests for CameraDetectionAnalytics
  */
class CameraDetectionAnalyticsTest extends AnyFunSuite {

  // Test conversion from detection events DataFrame to RDD
  test(
    "convertToDetectionEventsRdd should convert DataFrame to RDD of DetectionEvent objects"
  ) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestConvertToDetectionEventsRdd")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create a simple DataFrame
      val df = Seq(
        (1L, 101L, 1001L, "person", 1600000000L)
      ).toDF(
        "geographical_location_oid",
        "video_camera_oid",
        "detection_oid",
        "item_name",
        "timestamp_detected"
      )

      // Convert to RDD
      val rdd = CameraDetectionAnalytics.convertToDetectionEventsRdd(df)

      // Verify result
      val result = rdd.collect()
      assert(result.length === 1)
      assert(result(0).geographical_location_oid === 1L)
      assert(result(0).video_camera_oid === 101L)
      assert(result(0).detection_oid === 1001L)
      assert(result(0).item_name === "person")
      assert(result(0).timestamp_detected === 1600000000L)
    } finally {
      spark.stop()
    }
  }

  // Test conversion from geographical locations DataFrame to RDD
  test(
    "convertToGeoLocationsRdd should convert DataFrame to RDD of GeographicalLocation objects"
  ) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestConvertToGeoLocationsRdd")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create a simple DataFrame with minimal data
      val df = Seq(
        (1L, "Downtown")
      ).toDF(
        "geographical_location_oid",
        "geographical_location"
      )

      // Convert to RDD
      val rdd = CameraDetectionAnalytics.convertToGeoLocationsRdd(df)

      // Verify result
      val result = rdd.collect()
      assert(result.length === 1)
      assert(result(0).geographical_location_oid === 1L)
      assert(result(0).geographical_location === "Downtown")
    } finally {
      spark.stop()
    }
  }

  // Test deduplication logic
  test("deduplicate should remove duplicate detection_oid entries") {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestDeduplicate")
      .getOrCreate()

    try {
      // Create test data with a duplicate detection_oid
      val events = Seq(
        CameraDetectionAnalytics
          .DetectionEvent(1L, 101L, 1001L, "person", 1600000000L),
        CameraDetectionAnalytics.DetectionEvent(
          1L,
          102L,
          1001L,
          "person",
          1600000010L
        ) // Duplicate detection_oid
      )

      val rdd = spark.sparkContext.parallelize(events)

      // Deduplicate
      val dedupRdd = CameraDetectionAnalytics.deduplicate(rdd)

      // Verify result
      val result = dedupRdd.collect()
      assert(result.length === 1)
      assert(result(0).detection_oid === 1001L)
      assert(
        result(0).timestamp_detected === 1600000000L
      ) // First occurrence should be kept
    } finally {
      spark.stop()
    }
  }

  // Test counting items by location
  test("countItemsByLocation should count items correctly by location") {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestCountItemsByLocation")
      .getOrCreate()

    try {
      // Create test data
      val events = Seq(
        CameraDetectionAnalytics
          .DetectionEvent(1L, 101L, 1001L, "person", 1600000000L),
        CameraDetectionAnalytics
          .DetectionEvent(1L, 102L, 1002L, "car", 1600000010L),
        CameraDetectionAnalytics.DetectionEvent(
          1L,
          103L,
          1003L,
          "person",
          1600000020L
        )
      )

      val rdd = spark.sparkContext.parallelize(events)

      // Count items by location
      val countsByLocation = CameraDetectionAnalytics.countItemsByLocation(rdd)

      // Verify result
      val counts = countsByLocation.collect().sortBy(_._1._2)
      assert(counts.length === 2)

      // Check person count
      val personCount = counts
        .find { case ((locId, item), _) => locId == 1L && item == "person" }
        .get
        ._2
      assert(personCount === 2)

      // Check car count
      val carCount = counts
        .find { case ((locId, item), _) => locId == 1L && item == "car" }
        .get
        ._2
      assert(carCount === 1)
    } finally {
      spark.stop()
    }
  }

  // Test ranking generation
  test("generateRankingsWithLocationNames should produce correct rankings") {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestGenerateRankings")
      .getOrCreate()

    try {
      // Create sample count data
      val countsByLocationAndItem = spark.sparkContext.parallelize(
        Seq(
          ((1L, "person"), 2),
          ((1L, "car"), 1)
        )
      )

      // Create location map
      val locationMap = spark.sparkContext.parallelize(
        Seq(
          (1L, "Downtown")
        )
      )

      // Generate rankings
      val rankingsRdd =
        CameraDetectionAnalytics.generateRankingsWithLocationNames(
          countsByLocationAndItem,
          locationMap,
          2 // topN
        )

      // Verify results
      val rankings = rankingsRdd.collect().sortBy(_.item_rank)
      assert(rankings.length === 2)

      // Verify rank 1
      assert(rankings(0).geographical_location === "Downtown")
      assert(rankings(0).item_rank === 1)
      assert(rankings(0).item_name === "person")

      // Verify rank 2
      assert(rankings(1).geographical_location === "Downtown")
      assert(rankings(1).item_rank === 2)
      assert(rankings(1).item_name === "car")
    } finally {
      spark.stop()
    }
  }
}
