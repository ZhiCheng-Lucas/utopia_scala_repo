# Handling Data Skew in CameraDetectionAnalytics

## Assumption

This solution assumes we know in advance which geographical location has data skew (specifically location ID 42, as mentioned in the documentation).

## Problem Identification

The most performance-intensive operation affected by data skew is the `countItemsByLocation` method, which uses `reduceByKey` to count items by geographical location. Since location ID 42 has 5x the average number of detections, it creates a significant imbalance in partition workloads.

## Solution: Key Salting for Skewed Keys

Implement a technique called "key salting" (or "key expansion") that distributes the work for skewed keys across multiple partitions:

```scala
def countItemsByLocation(
    detectionEventsRdd: RDD[DetectionEvent]
): RDD[((Long, String), Int)] = {
  val SKEWED_LOCATION_ID = 42L
  val NUM_SALT_BUCKETS = 10 // Number of partitions to spread the skewed location across

  // Apply salting to the skewed location
  val saltedRdd = detectionEventsRdd.map { event =>
    if (event.geographical_location_oid == SKEWED_LOCATION_ID) {
      // Add a salt value (0-9) to the key for the skewed location
      val salt = (event.detection_oid % NUM_SALT_BUCKETS).toInt
      ((event.geographical_location_oid, event.item_name, salt), 1)
    } else {
      // Regular locations don't need salting
      ((event.geographical_location_oid, event.item_name, 0), 1)
    }
  }

  // Reduce by the salted key
  val reducedWithSalt = saltedRdd.reduceByKey(_ + _)

  // Remove the salt from the keys and combine counts for the skewed location
  reducedWithSalt
    .map { case ((locId, itemName, _), count) => ((locId, itemName), count) }
    .reduceByKey(_ + _)
}
```

## Explanation

1. **Identify the skewed key**: We specifically target location ID 42 which has 5x the normal load
2. **Add salt to the key**: For the skewed location, we add a random value (0-9) to distribute it across 10 partitions
3. **ReduceByKey**: We perform the first reduction with the salted keys
4. **Remove salt and combine**: We then remove the salt and combine the results with a final reduceByKey
