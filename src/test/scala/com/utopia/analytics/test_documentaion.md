# Camera Detection Analytics - Testing Documentation

## Current Testing Status

**Note:** This project currently implements unit testing only. Integration and end-to-end testing have not been implemented due to project timeline constraints.

The current approach to validation relies on manually comparing the spark output (.parquet files) with reference data using a Python comparison script.

For information about the sample data generation and comparison tools, see:
[<https://github.com/ZhiCheng-Lucas/utopia_generate_sample_data>](https://github.com/ZhiCheng-Lucas/utopia_generate_sample_data)

Future development will include automated end-to-end integration testing to replace the current manual comparison process.

## Overview

This document describes the testing approach for the Camera Detection Analytics application. The tests validate the core Spark RDD processing functionality to ensure that camera detection events are analyzed correctly.

## Test Configuration

The test suite is configured with special JVM parameters in the build.sbt file:

- Tests run in a forked JVM for proper Spark initialization
- JVM flags open necessary internal modules for Spark operations
- Each test creates an isolated SparkSession to ensure test independence

## Running the Tests

To run the test suite, in utopia_scala_repo:

```bash
sbt test
```

## Test Coverage

### Core RDD Transformation Tests

The test suite covers the following functional components:

#### 1. Data Conversion

- **`convertToDetectionEventsRdd`**
  - **Purpose**: Validates that DataFrame records are properly converted to typed DetectionEvent objects
  - **Input**: DataFrame with camera detection event fields
  - **Expected Output**: RDD of properly populated DetectionEvent objects
  - **Verification**: Object field values match input data

- **`convertToGeoLocationsRdd`**
  - **Purpose**: Validates that DataFrame records are properly converted to typed GeographicalLocation objects
  - **Input**: DataFrame with geographical location fields
  - **Expected Output**: RDD of properly populated GeographicalLocation objects
  - **Verification**: Object field values match input data

#### 2. Data Deduplication

- **`deduplicate`**
  - **Purpose**: Verifies that duplicate detection events (same detection_oid) are properly removed
  - **Input**: RDD containing duplicate detection_oid values
  - **Expected Output**: RDD with exactly one record per detection_oid (keeping the first occurrence)
  - **Verification**: Count is reduced by duplicate count, retained events are the first occurrences

#### 3. Data Aggregation

- **`countItemsByLocation`**
  - **Purpose**: Verifies proper counting of items detected at each geographical location
  - **Input**: RDD of detection events
  - **Expected Output**: RDD of ((location_oid, item_name), count) with accurate counts
  - **Verification**: Counts match expected aggregation of input data

#### 4. Results Generation

- **`generateRankingsWithLocationNames`**
  - **Purpose**: Validates that items are properly ranked by popularity for each location
  - **Input**:
    - RDD of ((location_oid, item_name), count)
    - RDD of (location_oid, location_name) mappings
    - topN configuration parameter
  - **Expected Output**: RDD of ItemRanking objects containing location name, rank, and item name
  - **Verification**: Items are correctly ordered by count (descending), limited to topN, and include proper location names
