package org.sunbird.dp.spec

import java.util
import com.google.common.reflect.TypeToken
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.dp.assessment.domain.Event
import org.sunbird.dp.assessment.task.{AssessmentAggregatorConfig, AssessmentAggregatorStreamTask}
import org.sunbird.dp.core.cache.RedisConnect
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.dp.fixture.EventFixture
import org.sunbird.dp.{BaseMetricsReporter, BaseTestSpec}
import redis.embedded.RedisServer


class AssessmentAggregatorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  var redisServer: RedisServer = _

  val config: Config = ConfigFactory.load("test.conf")
  val assessmentConfig: AssessmentAggregatorConfig = new AssessmentAggregatorConfig(config)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()


  var cassandraUtil: CassandraUtil = _


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    redisServer.start()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(assessmentConfig.dbHost, assessmentConfig.dbPort)
    val session = cassandraUtil.session
    setupRedisTestData()

    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()

    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {

      }
    }
    flinkCluster.after()
  }


  "AssessmentAggregator " should "Update event to db" in {
    when(mockKafkaUtil.kafkaEventSource[Event](assessmentConfig.kafkaInputTopic)).thenReturn(new AssessmentAggreagatorEventSource)
    when(mockKafkaUtil.kafkaEventSink[Event](assessmentConfig.kafkaFailedTopic)).thenReturn(new FailedEventsSink)
    when(mockKafkaUtil.kafkaStringSink(assessmentConfig.kafkaCertIssueTopic)).thenReturn(new certificateIssuedEventsSink)
    val task = new AssessmentAggregatorStreamTask(assessmentConfig, mockKafkaUtil)
    task.process()
    assert(FailedEventsSink.values.get(0).getTelemetry.getMap.containsKey("metadata"))
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.skippedEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.dbReadCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.dbUpdateCount}").getValue() should be(5)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.failedEventCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.batchSuccessCount}").getValue() should be(5)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.cacheHitCount}").getValue() should be(7)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.cacheHitMissCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.certIssueEventsCount}").getValue() should be(5)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.dbScoreAggUpdateCount}").getValue() should be(5)
    BaseMetricsReporter.gaugeMetrics(s"${assessmentConfig.jobName}.${assessmentConfig.dbScoreAggReadCount}").getValue() should be(5)
    
    val test_row1 = cassandraUtil.findOne("select total_score,total_max_score from sunbird_courses.assessment_aggregator where user_id='d0d8a341-9637-484c-b871-0c27015af238' and course_id='do_2128410273679114241112'")
    assert(test_row1.getDouble("total_score") == 2.0)
    assert(test_row1.getDouble("total_max_score") == 2.0)

    val test_row2 = cassandraUtil.findOne("select total_score,total_max_score from sunbird_courses.assessment_aggregator where user_id='ff1c4bdf-27e2-49bc-a53f-6e304bb3a87f' and course_id='do_2128415652377067521125'")
    assert(test_row2.getDouble("total_score") == 3.0)
    assert(test_row2.getDouble("total_max_score") == 4.0)

    val test_row3 = cassandraUtil.findOne("select agg from sunbird_courses.user_activity_agg where activity_type='Course' and activity_id='do_2128410273679114241112' and  user_id='d0d8a341-9637-484c-b871-0c27015af238'")
    val resultMap3 = test_row3.getMap("agg", new TypeToken[String]() {}, new TypeToken[Integer]() {})
    assert(null != resultMap3)
    assert(2 == resultMap3.getOrDefault("score:do_2128373396098744321673", 0))
    assert(2 == resultMap3.getOrDefault("max_score:do_2128373396098744321673", 0))

    val test_row4 = cassandraUtil.findOne("select agg from sunbird_courses.user_activity_agg where activity_type='Course' and activity_id='do_2128415652377067521125' and  user_id='ff1c4bdf-27e2-49bc-a53f-6e304bb3a87f'")
    val resultMap4 = test_row4.getMap("agg", new TypeToken[String]() {}, new TypeToken[Integer]() {})
    assert(null != resultMap4)
    assert(3 == resultMap4.getOrDefault("score:do_212686723743318016173", 0))
    assert(4 == resultMap4.getOrDefault("max_score:do_212686723743318016173", 0))

    val test_row5 = cassandraUtil.findOne("select agg from sunbird_courses.user_activity_agg where activity_type='Course' and activity_id='do_3129323995959541761169' and  user_id='50a9e3fc-d047-4fa5-a37b-67501b8933db'")
    val resultMap5 = test_row5.getMap("agg", new TypeToken[String]() {}, new TypeToken[Integer]() {})
    assert(null != resultMap5)
    assert(1 == resultMap5.getOrDefault("score:do_3129323935897108481169", 0))
    assert(1 == resultMap5.getOrDefault("max_score:do_3129323935897108481169", 0))
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
    val response = cassandraUtil.find("SELECT * FROM sunbird_courses.assessment_aggregator;")
    response should not be (null)
  }

  def setupRedisTestData() {
    val redisConnect = new RedisConnect(assessmentConfig.metaRedisHost, assessmentConfig.metaRedisPort, assessmentConfig)
    val jedis = redisConnect.getConnection(assessmentConfig.relationCacheNode)
    EventFixture.leafNodesList.map(nodes => {
      nodes.map(node => {
        jedis.sadd(node._1, node._2)
      })
    })
  }
}

class AssessmentAggreagatorEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    val gson = new Gson()

    //val eventMap1 = gson.fromJson(EventFixture.BATCH_ASSESS_EVENT, new util.LinkedHashMap[String, Any]().getClass)

    val eventMap1 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.BATCH_ASSESS_EVENT)
    val eventMap2 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.BATCH_ASSESS__OLDER_EVENT)
    val eventMap3 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.BATCH_ASSESS_FAIL_EVENT)
    val eventMap4 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.QUESTION_EVENT_RES_VALUES)
    val eventMap5 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.LATEST_BATCH_ASSESS_EVENT)
    val eventMap6 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.BATCH_DUPLICATE_QUESTION_EVENT)
    val eventMap7 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.INVALID_CONTENT_ID_EVENT)
    val eventMap8 = JSONUtil.deserialize[util.HashMap[String, Any]](EventFixture.BATCH_ASSESS_EVENT_WITHOUT_CACHE)
    ctx.collect(new Event(eventMap1))
    ctx.collect(new Event(eventMap2))
    ctx.collect(new Event(eventMap3))
    ctx.collect(new Event(eventMap4))
    ctx.collect(new Event(eventMap5))
    ctx.collect(new Event(eventMap6))
    ctx.collect(new Event(eventMap7))
    ctx.collect(new Event(eventMap8))
  }

  override def cancel() = {}

}


class FailedEventsSink extends SinkFunction[Event] {

  override def invoke(value: Event): Unit = {
    synchronized {
      FailedEventsSink.values.add(value)
    }
  }
}

object FailedEventsSink {
  val values: util.List[Event] = new util.ArrayList()
}

class certificateIssuedEventsSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      certificateIssuedEvents.values.add(value)
    }
  }
}

object certificateIssuedEvents {
  val values: util.List[String] = new util.ArrayList()
}