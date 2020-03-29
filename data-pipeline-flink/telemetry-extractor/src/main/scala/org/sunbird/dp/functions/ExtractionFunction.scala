package org.sunbird.dp.functions

import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.joda.time.format.DateTimeFormat
import org.sunbird.dp.domain.LogEventGeneration
import org.sunbird.dp.task.ExtractionConfig

class ExtractionFunction(config: ExtractionConfig)(implicit val eventTypeInfo: TypeInformation[util.Map[String, AnyRef]]) extends ProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]] {
  /**
   * Raw event tag - Holding all extracted events to "raw-events" tag
   */
  lazy val rawEventOutPut: OutputTag[util.Map[String, AnyRef]] = new OutputTag[util.Map[String, AnyRef]](id = "raw-events")

  /**
   * Holding all failed events in the "failed-events" tag.
   */
  lazy val failedEventsOutPut: OutputTag[util.Map[String, AnyRef]] = new OutputTag[util.Map[String, AnyRef]](id = "failed-events")

  /**
   * Holding all Log events which is generated by telemetry extractor job in the "log-events" tag.
   */
  lazy val logEventOutPut: OutputTag[util.Map[String, AnyRef]] = new OutputTag[util.Map[String, AnyRef]](id = "log-events")

  /**
   * Method to process the events extraction from the batch
   * @param batchEvent - Batch of telemetry events
   * @param context
   * @param collector
   */
  override def processElement(batchEvent: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]]#Context, collector: Collector[util.Map[String, AnyRef]]): Unit = {
    val gson = new Gson();
    val eventsList = getEventsList(batchEvent)
    eventsList.foreach(event => {
      val eventJson = gson.toJson(event)
      val eventSize = eventJson.getBytes("UTF-8").length;
      val eventData = updateEvent(batchEvent, gson.fromJson(eventJson, (new util.HashMap[String, AnyRef]()).getClass))
      if (eventSize > config.rawEventSize) {
        context.output(failedEventsOutPut, eventData)
      } else {
        context.output(rawEventOutPut, eventData)
      }
    })
    context.output(logEventOutPut, gson.fromJson(gson.toJson(LogEventGeneration.generate(eventsList.length, batchEvent)), (new util.HashMap[String, AnyRef]()).getClass))
  }

  /**
   * Method to get the events from the batch.
   * @param batchEvent - Batch of telemetry event.
   * @return Array[AnyRef] - List of telemetry events.
   */
  def getEventsList(batchEvent: util.Map[String, AnyRef]): Array[AnyRef] = {
    val gson = new Gson();
    gson.fromJson(gson.toJson(batchEvent.get("events")), (new util.ArrayList[String]()).getClass).toArray
  }

  /**
   * Method to update the "SyncTS", "@TimeStamp" fileds of batch events into Events Object
   *
   * @param batchEvent - Batch of Telemetry Events.
   * @param event - Telemetry Event
   * @return - util.Map[String, AnyRef] Updated Telemetry Event
   */
  def updateEvent(batchEvent: util.Map[String, AnyRef], event: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val syncTs: Long = batchEvent.get("syncts").asInstanceOf[Number].longValue()
    val timeStamp: String = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC.print(syncTs)
    event.put("syncts", syncTs.asInstanceOf[AnyRef])
    event.put("@timestamp", timeStamp.asInstanceOf[AnyRef])
    event
  }
}