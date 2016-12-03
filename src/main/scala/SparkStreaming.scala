package com.spark.sample
import com.spark.sample.GetContext.streamingContext
/**
  * Created by senomoy on 02-09-2016.
  */
object SparkStreaming extends App {

  // local machine
  val lines = streamingContext.socketTextStream("localhost", 9999)
  // Filter our DStream for lines with "error"
  val checkString= lines.filter(word=> word.contains("scala") || word.contains("spark"))
  // Print out the lines with errors
  checkString.print()
  // Start our streaming context and wait for it to "finish"
  streamingContext.start()
  // Wait for the job to finish
  streamingContext.awaitTermination()
}
// nc -lk 9999