package org.ekstep.dp.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.ekstep.dp.cache.{DedupEngine, RedisConnect}
import org.ekstep.dp.domain.Event
import org.ekstep.dp.task.PipelinePreprocessorConfig
import org.slf4j.LoggerFactory

import java.util

class DeduplicationFunction(config: PipelinePreprocessorConfig)(implicit val eventTypeInfo: TypeInformation[Event])
  // extends ProcessFunction[Event, Event] {
  extends ProcessFunction[util.Map[String, AnyRef], Event] {

  private[this] val logger = LoggerFactory.getLogger(classOf[DeduplicationFunction])

  lazy val duplicateEventOutput: OutputTag[Event] = new OutputTag[Event](id = "duplicate-events")
  lazy val uniqueEventOuput: OutputTag[Event] = new OutputTag[Event](id = "unique-events")

  lazy val redisConnect = new RedisConnect(config)
  lazy val dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)

  override def processElement(
                               // event: Event,
                               inevent: util.Map[String, AnyRef],
                               // ctx: ProcessFunction[Event, Event]#Context,
                               ctx: ProcessFunction[util.Map[String, AnyRef], Event]#Context,
                               out: Collector[Event]): Unit = {

    val event = new Event(inevent)
    val duplicationCheckRequired = isDuplicateCheckRequired(event)
    if(duplicationCheckRequired) {
      if (!dedupEngine.isUniqueEvent(event.mid)) {
        logger.info(s"Duplicate Event mid: ${event.mid}")
        event.markDuplicate()
        ctx.output(duplicateEventOutput, event)
      } else {
        logger.info(s"Adding mid: ${event.mid} to Redis")
        dedupEngine.storeChecksum(event.mid)
        ctx.output(uniqueEventOuput, event)
      }
    } else {
      ctx.output(uniqueEventOuput, event)
    }
  }

  def isDuplicateCheckRequired(event: Event): Boolean = {
    config.includedProducersForDedup.contains(event.producerId())
  }
}