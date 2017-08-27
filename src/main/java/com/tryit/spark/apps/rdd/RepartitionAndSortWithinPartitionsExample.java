package com.tryit.spark.apps.rdd;

/**
 * Created by agebriel on 12/13/16.
 */

/*
*   repartitionAndSortWithinPartitions(partitioner)
*   Repartition the RDD according to the given partitioner and, within
*   each resulting partition, sort records by their keys. This is more
*   efficient than calling repartition and then sorting within each
*   partition because it can push the sorting down into the shuffle
*   machinery.
*/
public class RepartitionAndSortWithinPartitionsExample
{
}
