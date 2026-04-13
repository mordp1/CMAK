/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

package kafka.manager.actor.cluster

import java.io.Closeable
import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedDeque, TimeUnit}

import akka.actor.{ActorContext, ActorPath, ActorRef, Cancellable, Props}
import akka.pattern._
import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import grizzled.slf4j.Logging
import kafka.common.OffsetAndMetadata
import kafka.manager._
import kafka.manager.base.cluster.{BaseClusterQueryActor, BaseClusterQueryCommandActor}
import kafka.manager.base.{LongRunningPoolActor, LongRunningPoolConfig}
import kafka.manager.features.{ClusterFeatures, KMDeleteTopicFeature, KMPollConsumersFeature}
import kafka.manager.model.ActorModel._
import kafka.manager.model._
import kafka.manager.utils.two40.{GroupMetadata, GroupMetadataKey, MemberMetadata, OffsetKey}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, Config => AdminConfig, ConfigEntry, ConsumerGroupDescription, DescribeConsumerGroupsOptions}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.{ConsumerGroupState, TopicPartition}
import org.apache.kafka.common.config.{ConfigResource, SaslConfigs}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.common.KafkaFuture.BiConsumer
import org.apache.kafka.common.metrics.{KafkaMetric, MetricsReporter}
import org.apache.kafka.common.utils.Time

/**
  * @author hiral
  */
import kafka.manager.utils._

import scala.collection.JavaConverters._

class NoopJMXReporter extends MetricsReporter {
  override def init(metrics: util.List[KafkaMetric]): Unit = {}

  override def metricChange(metric: KafkaMetric): Unit = {}

  override def metricRemoval(metric: KafkaMetric): Unit = {}

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
case class PartitionOffsetRequestInfo(time: Long, maxNumOffsets: Int)
case class KafkaAdminClientActorConfig(clusterContext: ClusterContext, longRunningPoolConfig: LongRunningPoolConfig, consumerProperties: Option[Properties])
case class KafkaAdminClientActor(config: KafkaAdminClientActorConfig) extends BaseClusterQueryActor with LongRunningPoolActor {

  private[this] var adminClientOption : Option[AdminClient] = None

  protected implicit val clusterContext: ClusterContext = config.clusterContext
  override protected def longRunningPoolConfig: LongRunningPoolConfig = config.longRunningPoolConfig

  override protected def longRunningQueueFull(): Unit = {
    log.error("Long running pool queue full, skipping!")
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    super.preStart()
    log.info(config.toString)
    Try {
      adminClientOption = Some(createAdminClient())
    }.recover { case e => log.error(e, "Failed to create AdminClient in KafkaAdminClientActor") }
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Closing admin client...")
    Try(adminClientOption.foreach(_.close()))
    log.info("Stopped actor %s".format(self.path))
  }

  private def createAdminClient(): AdminClient = {
    val props = new Properties()
    config.consumerProperties.foreach { cp => props.putAll(cp.asMap) }
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.clusterContext.config.securityProtocol.stringId)
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.clusterContext.config.bootstrapServers)
    if(config.clusterContext.config.saslMechanism.nonEmpty){
      props.put(SaslConfigs.SASL_MECHANISM, config.clusterContext.config.saslMechanism.get.stringId)
    }
    if(config.clusterContext.config.jaasConfig.nonEmpty){
      props.put(SaslConfigs.SASL_JAAS_CONFIG, config.clusterContext.config.jaasConfig.get)
    }
    log.info(s"Creating admin client with bootstrap servers: ${config.clusterContext.config.bootstrapServers}")
    AdminClient.create(props)
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    if(adminClientOption.isEmpty) {
      Try { adminClientOption = Some(createAdminClient()) }
    }
    adminClientOption match {
      case None =>
        log.error(s"AdminClient not initialized yet, cannot process request : $request")
      case Some(client) =>
        implicit val ec = longRunningExecutionContext
        request match {
          case KAGetGroupSummary(groupList: Seq[String], enqueue: java.util.Queue[(String, List[MemberMetadata])]) =>
            Future {
              try {
                val options = new DescribeConsumerGroupsOptions
                options.timeoutMs(1000)
                client.describeConsumerGroups(groupList.asJava, options).all().whenComplete {
                  (mapGroupDescription, error) =>
                    if (mapGroupDescription != null) {
                      mapGroupDescription.asScala.foreach {
                        case (group, desc) =>
                          enqueue.offer(group -> desc.members().asScala.map(m => MemberMetadata.from(group, desc, m)).toList)
                      }
                    }
                }
              } catch {
                case e: Exception =>
                  log.error(e, s"Failed to get group summary with admin client : $groupList")
                  Try { adminClientOption.foreach(_.close()) }
                  adminClientOption = None
              }
            }
          case any: Any => log.warning("kac : processQueryRequest : Received unknown message: {}", any.toString)
        }
    }
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("kac : processActorResponse : Received unknown message: {}", any.toString)
    }
  }
}

class KafkaAdminClient(context: => ActorContext, adminClientActorPath: ActorPath) {
  def enqueueGroupMetadata(groupList: Seq[String], queue: java.util.Queue[(String, List[MemberMetadata])]) : Unit = {
    Try {
      context.actorSelection(adminClientActorPath).tell(KAGetGroupSummary(groupList, queue), ActorRef.noSender)
    }
  }
}


object KafkaManagedOffsetCache {
  val supportedVersions: Set[KafkaVersion] = Set(Kafka_0_8_2_0, Kafka_0_8_2_1, Kafka_0_8_2_2, Kafka_0_9_0_0, Kafka_0_9_0_1, Kafka_0_10_0_0, Kafka_0_10_0_1, Kafka_0_10_1_0, Kafka_0_10_1_1, Kafka_0_10_2_0, Kafka_0_10_2_1, Kafka_0_11_0_0, Kafka_0_11_0_2, Kafka_1_0_0, Kafka_1_0_1, Kafka_1_1_0, Kafka_1_1_1, Kafka_2_0_0, Kafka_2_1_0, Kafka_2_1_1, Kafka_2_2_0, Kafka_2_2_1, Kafka_2_2_2, Kafka_2_3_0, Kafka_2_2_1, Kafka_2_4_0, Kafka_2_4_1, Kafka_2_5_0, Kafka_2_5_1, Kafka_2_6_0, Kafka_2_7_0, Kafka_2_8_0, Kafka_2_8_1, Kafka_3_0_0, Kafka_3_1_0, Kafka_3_1_1, Kafka_3_2_0, Kafka_3_3_0, Kafka_3_4_0, Kafka_3_5_0, Kafka_3_6_0, Kafka_3_7_0, Kafka_3_8_0, Kafka_4_0_0, Kafka_4_1_0)
  val ConsumerOffsetTopic = "__consumer_offsets"

  def isSupported(version: KafkaVersion) : Boolean = {
    supportedVersions(version)
  }

  def createSet[T](): mutable.Set[T] = {
    import scala.collection.JavaConverters._
    java.util.Collections.newSetFromMap(
      new java.util.concurrent.ConcurrentHashMap[T, java.lang.Boolean]).asScala
  }
}

object KafkaManagedOffsetCacheConfig {
  val defaultGroupMemberMetadataCheckMillis: Int = 30000
  val defaultGroupTopicPartitionOffsetMaxSize: Int = 1000000
  val defaultGroupTopicPartitionOffsetExpireDays: Int = 7
}

case class KafkaManagedOffsetCacheConfig(groupMemberMetadataCheckMillis: Int = KafkaManagedOffsetCacheConfig.defaultGroupMemberMetadataCheckMillis
                                         , groupTopicPartitionOffsetMaxSize: Int = KafkaManagedOffsetCacheConfig.defaultGroupTopicPartitionOffsetMaxSize
                                         , groupTopicPartitionOffsetExpireDays: Int = KafkaManagedOffsetCacheConfig.defaultGroupTopicPartitionOffsetExpireDays)

case class KafkaManagedOffsetCache(clusterContext: ClusterContext
                                   , adminClient: KafkaAdminClient
                                   , consumerProperties: Option[Properties]
                                   , bootstrapServers: String
                                   , config: KafkaManagedOffsetCacheConfig
                                  ) extends Runnable with Closeable with Logging {
  val groupTopicPartitionOffsetSet: mutable.Set[(String, String, Int)] = KafkaManagedOffsetCache.createSet()
  val groupTopicPartitionOffsetMap:Cache[(String, String, Int), OffsetAndMetadata] = Caffeine
    .newBuilder()
    .maximumSize(config.groupTopicPartitionOffsetMaxSize)
    .expireAfterAccess(config.groupTopicPartitionOffsetExpireDays, TimeUnit.DAYS)
    .removalListener(new RemovalListener[(String, String, Int), OffsetAndMetadata] {
      override def onRemoval(key: (String, String, Int), value: OffsetAndMetadata, cause: RemovalCause): Unit = {
        groupTopicPartitionOffsetSet.remove(key)
      }
    })
    .build[(String, String, Int), OffsetAndMetadata]()
  val topicConsumerSetMap = new TrieMap[String, mutable.Set[String]]()
  val consumerTopicSetMap = new TrieMap[String, mutable.Set[String]]()
  val groupTopicPartitionMemberSet: mutable.Set[(String, String, Int)] = KafkaManagedOffsetCache.createSet()
  val groupTopicPartitionMemberMap: Cache[(String, String, Int), MemberMetadata] = Caffeine
    .newBuilder()
    .maximumSize(config.groupTopicPartitionOffsetMaxSize)
    .expireAfterAccess(config.groupTopicPartitionOffsetExpireDays, TimeUnit.DAYS)
    .removalListener(new RemovalListener[(String, String, Int), MemberMetadata] {
      override def onRemoval(key: (String, String, Int), value: MemberMetadata, cause: RemovalCause): Unit = {
        groupTopicPartitionMemberSet.remove(key)
      }
    })
    .build[(String, String, Int), MemberMetadata]()

  private[this] val queue = new ConcurrentLinkedDeque[(String, List[MemberMetadata])]()

  @volatile
  private[this] var lastUpdateTimeMillis : Long = 0

  private[this] var lastGroupMemberMetadataCheckMillis : Long = System.currentTimeMillis()

  import KafkaManagedOffsetCache._
  import kafka.manager.utils.two40.GroupMetadataManager._

  require(isSupported(clusterContext.config.version), s"Kafka version not support : ${clusterContext.config}")

  @volatile
  private[this] var shutdown: Boolean = false

  private[this] def createKafkaConsumer(): Consumer[Array[Byte], Array[Byte]] = {
    val hostname = InetAddress.getLocalHost.getHostName
    val props: Properties = new Properties()
    props.put(GROUP_ID_CONFIG, s"KMOffsetCache-$hostname")
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(METRIC_REPORTER_CLASSES_CONFIG, classOf[NoopJMXReporter].getCanonicalName)
    consumerProperties.foreach { cp => props.putAll(cp.asMap) }
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, clusterContext.config.securityProtocol.stringId)
    if(clusterContext.config.saslMechanism.nonEmpty){
      props.put(SaslConfigs.SASL_MECHANISM, clusterContext.config.saslMechanism.get.stringId)
      if(clusterContext.config.jaasConfig.nonEmpty){
        props.put(SaslConfigs.SASL_JAAS_CONFIG, clusterContext.config.jaasConfig.get)
      }
    }
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  private[this] def performGroupMetadataCheck() : Unit = {
    val currentMillis = System.currentTimeMillis()
    if((lastGroupMemberMetadataCheckMillis + config.groupMemberMetadataCheckMillis) < currentMillis) {
      val diff = groupTopicPartitionOffsetSet.diff(groupTopicPartitionMemberSet)
      if(diff.nonEmpty) {
        val groupsToBackfill = diff.map(_._1).toSeq
        info(s"Backfilling group metadata for $groupsToBackfill")
        adminClient.enqueueGroupMetadata(groupsToBackfill, queue)
      }
      lastGroupMemberMetadataCheckMillis = System.currentTimeMillis()
      lastUpdateTimeMillis = System.currentTimeMillis()
    }
  }

  private[this] def dequeueAndProcessBackFill(): Unit = {
    while(!queue.isEmpty) {
      val (groupId, members) = queue.pop()
      members.foreach {
        member =>
          try {
            member.assignment.foreach {
              case (topic, part) =>
                val k = (groupId, topic, part)
                if(groupTopicPartitionMemberMap.getIfPresent(k) == null) {
                  groupTopicPartitionMemberMap.put(k, member)
                  groupTopicPartitionMemberSet.add(k)
                }
            }
          } catch {
            case e: Exception =>
              error(s"Failed to get member metadata from group summary and member summary : $groupId : $member", e)
          }
      }
    }
  }

  override def run(): Unit = {
    if(!shutdown) {
      for {
        consumer <- Try {
          val consumer = createKafkaConsumer()
          consumer.subscribe(java.util.Arrays.asList(KafkaManagedOffsetCache.ConsumerOffsetTopic))
          consumer
        }.logError(s"Failed to create consumer for offset topic for cluster ${clusterContext.config.name}")
      } {
        try {
          info(s"Consumer created for kafka offset topic consumption for cluster ${clusterContext.config.name}")
          while (!shutdown) {
            try {
              try {
                dequeueAndProcessBackFill()
                performGroupMetadataCheck()
              } catch {
                case e: Exception =>
                  error("Failed to backfill group metadata", e)
              }

              val records: ConsumerRecords[Array[Byte], Array[Byte]] = consumer.poll(Duration.ofMillis(100))
              val iterator = records.iterator()
              while (iterator.hasNext) {
                val record = iterator.next()
                val key = record.key()
                val value = record.value()
                if (key != null && value != null) {
                  readMessageKey(ByteBuffer.wrap(record.key())) match {
                    case OffsetKey(version, key) =>
                      val value: OffsetAndMetadata = readOffsetMessageValue(ByteBuffer.wrap(record.value()))
                      val newKey = (key.group, key.topicPartition.topic, key.topicPartition.partition)
                      groupTopicPartitionOffsetMap.put(newKey, value)
                      groupTopicPartitionOffsetSet.add(newKey)
                      val topic = key.topicPartition.topic
                      val group = key.group
                      val consumerSet = {
                        if (topicConsumerSetMap.contains(topic)) {
                          topicConsumerSetMap(topic)
                        } else {
                          val s = new mutable.TreeSet[String]()
                          topicConsumerSetMap += topic -> s
                          s
                        }
                      }
                      consumerSet += group

                      val topicSet = {
                        if (consumerTopicSetMap.contains(group)) {
                          consumerTopicSetMap(group)
                        } else {
                          val s = new mutable.TreeSet[String]()
                          consumerTopicSetMap += group -> s
                          s
                        }
                      }
                      topicSet += topic
                    case GroupMetadataKey(version, key) =>
                      val value: GroupMetadata = readGroupMessageValue(key, ByteBuffer.wrap(record.value()), Time.SYSTEM)
                      value.allMemberMetadata.foreach {
                        mm =>
                          mm.assignment.foreach {
                            case (topic, part) =>
                              val newKey = (key, topic, part)
                              groupTopicPartitionMemberMap.put(newKey, mm)
                              groupTopicPartitionMemberSet.add(newKey)
                          }
                      }
                    case other: Any =>
                      error(s"Unhandled key type : ${other.getClass.getCanonicalName}")
                  }
                }
                lastUpdateTimeMillis = System.currentTimeMillis()
              }
            } catch {
              case e: Exception =>
                warn(s"Failed to process a message from offset topic on cluster ${clusterContext.config.name}!", e)
            }
          }
        } finally {
          info(s"Shutting down consumer for $ConsumerOffsetTopic on cluster ${clusterContext.config.name}")
          Try(consumer.close())
        }
      }
    }
    groupTopicPartitionMemberSet.clear()
    groupTopicPartitionMemberMap.invalidateAll()
    groupTopicPartitionMemberMap.cleanUp()
    groupTopicPartitionOffsetSet.clear()
    groupTopicPartitionOffsetMap.invalidateAll()
    groupTopicPartitionOffsetMap.cleanUp()
    info(s"KafkaManagedOffsetCache shut down for cluster ${clusterContext.config.name}")
  }

  def close(): Unit = {
    this.shutdown = true
  }

  def getOffset(group: String, topic: String, part:Int) : Option[Long] = {
    Option(groupTopicPartitionOffsetMap.getIfPresent((group, topic, part))).map(_.offset)
  }

  def getOwner(group: String, topic: String, part:Int) : Option[String] = {
    Option(groupTopicPartitionMemberMap.getIfPresent((group, topic, part))).map(mm => s"${mm.memberId}:${mm.clientHost}")
  }

  def getConsumerTopics(group: String) : Set[String] = consumerTopicSetMap.get(group).map(_.toSet).getOrElse(Set.empty)
  def getTopicConsumers(topic: String) : Set[String] = topicConsumerSetMap.get(topic).map(_.toSet).getOrElse(Set.empty)
  def getConsumers : IndexedSeq[String] = consumerTopicSetMap.keys.toIndexedSeq
  def getLastUpdateTimeMillis: Long = lastUpdateTimeMillis
}

case class ConsumerInstanceSubscriptions private(id: String, subs: Map[String, Int])

object ConsumerInstanceSubscriptions extends Logging {

  //{"version":1,"subscription":{"DXSPreAgg":1},"pattern":"static","timestamp":"1443578242654"}
  def apply(consumer: String, id: String, jsonString: String) : ConsumerInstanceSubscriptions = {
    import org.json4s.jackson.JsonMethods.parse
    import org.json4s.scalaz.JsonScalaz.field
    val json = parse(jsonString)
    val subs: Map[String, Int] = field[Map[String,Int]]("subscription")(json).fold({ e =>
      error(s"[consumer=$consumer] Failed to parse consumer instance subscriptions : $id : $jsonString"); Map.empty}, identity)
    new ConsumerInstanceSubscriptions(id, subs)
  }
}

trait OffsetCache extends Logging {

  def consumerProperties: Option[Properties]

  def kafkaAdminClient: KafkaAdminClient

  def clusterContext: ClusterContext

  def getKafkaVersion: KafkaVersion

  def getCacheTimeoutSecs: Int

  def getSimpleConsumerSocketTimeoutMillis: Int

  def kafkaManagedOffsetCacheConfig: KafkaManagedOffsetCacheConfig

  protected[this] implicit def ec: ExecutionContext

  protected[this] implicit def cf: ClusterFeatures

  protected[this] val loadOffsets: Boolean

  // Caches a map of partitions to offsets at a key that is the topic's name.
  private[this] lazy val partitionOffsetsCache: LoadingCache[String, Future[PartitionOffsetsCapture]] = CacheBuilder.newBuilder()
    .expireAfterWrite(getCacheTimeoutSecs,TimeUnit.SECONDS)
    .build(
    new CacheLoader[String,Future[PartitionOffsetsCapture]] {
      def load(topic: String): Future[PartitionOffsetsCapture] = {
        loadPartitionOffsets(topic)
      }
    }
  )

  private[this] def loadPartitionOffsets(topic: String): Future[PartitionOffsetsCapture] = {
    val optPartitionsWithLeaders : Option[List[(Int, Option[BrokerIdentity])]] = getTopicPartitionLeaders(topic)

    val clientId = "partitionOffsetGetter"

    val partitionsByBroker = optPartitionsWithLeaders.map {
      listOfPartAndBroker => listOfPartAndBroker.collect {
        case (part, broker) if broker.isDefined => (broker.get, part)
      }.groupBy(_._1)
    }

    def getKafkaConsumer() = {
      new KafkaConsumer(consumerProperties.get)
    }

    val futureMap: Future[PartitionOffsetsCapture] = {
      partitionsByBroker.fold[Future[PartitionOffsetsCapture]]{
        Future.failed(new IllegalArgumentException(s"Do not have partitions and their leaders for topic $topic"))
      } { partitionsWithLeaders =>
        try {
          val listOfFutures = partitionsWithLeaders.toList.map(tpl => (tpl._2)).map {
            case (parts) =>
              val kafkaConsumer = getKafkaConsumer()
              val f: Future[Map[TopicPartition, java.lang.Long]] = Future {
                try {
                  val topicAndPartitions = parts.map(tpl => (new TopicPartition(topic, tpl._2), PartitionOffsetRequestInfo(-1, 1)))
                  val request: List[TopicPartition] = topicAndPartitions.map(f => new TopicPartition(f._1.topic(), f._1.partition()))
                  kafkaConsumer.endOffsets(request.asJava).asScala.toMap
                } finally {
                  kafkaConsumer.close()
                }
              }
              f.recover { case t =>
                error(s"[topic=$topic] An error has occurred while getting topic offsets from broker $parts", t)
                Map.empty[TopicPartition, java.lang.Long]
              }
          }
          val result: Future[Map[TopicPartition, java.lang.Long]] = Future.sequence(listOfFutures).map(_.foldRight(Map.empty[TopicPartition, java.lang.Long])((b, a) => b ++ a))
          result.map(m => PartitionOffsetsCapture(System.currentTimeMillis(), m.map(f => (f._1.partition(), f._2.toLong))))
        }
        catch {
          case e: Exception =>
            error(s"Failed to get offsets for topic $topic", e)
            Future.failed(e)
        }
      }
    }

    futureMap.failed.foreach {
      t => error(s"[topic=$topic] An error has occurred while getting topic offsets", t)
    }
    futureMap
  }

  private[this] def emptyPartitionOffsetsCapture: Future[PartitionOffsetsCapture] = Future.successful(PartitionOffsetsCapture(System.currentTimeMillis(), Map()))

  protected def getTopicPartitionLeaders(topic: String) : Option[List[(Int, Option[BrokerIdentity])]]

  protected def getTopicDescription(topic: String, interactive: Boolean) : Option[TopicDescription]

  protected def readConsumerOffsetByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long]

  protected def readConsumerOwnerByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String]

  protected def getConsumerTopicsFromIds(consumer: String) : Set[String]

  protected def getConsumerTopicsFromOffsets(consumer: String) : Set[String]

  protected def getConsumerTopicsFromOwners(consumer: String) : Set[String]

  protected def getZKManagedConsumerList: IndexedSeq[ConsumerNameAndType]

  protected def lastUpdateMillisZK : Long

  protected def hasNonSecureEndpointOverride: Boolean = true

  protected def getConsumerTopics(consumer: String) : Set[String] = {
    getConsumerTopicsFromOffsets(consumer) ++ getConsumerTopicsFromOwners(consumer) ++ getConsumerTopicsFromIds(consumer)
  }

  private[this] var kafkaManagedOffsetCache : Option[KafkaManagedOffsetCache] = None

  private[this] lazy val hasNonSecureEndpoint = hasNonSecureEndpointOverride

  def start() : Unit = {
    if(KafkaManagedOffsetCache.isSupported(clusterContext.config.version)) {
      if(kafkaManagedOffsetCache.isEmpty) {
        info("Starting kafka managed offset cache ...")
        Try {
          val bootstrapServers = clusterContext.config.bootstrapServers
          require(bootstrapServers.nonEmpty, "Cannot consume from offset topic: bootstrapServers is empty!")
          val of = new KafkaManagedOffsetCache(clusterContext, kafkaAdminClient, consumerProperties, bootstrapServers, kafkaManagedOffsetCacheConfig)
          kafkaManagedOffsetCache = Option(of)
          val t = new Thread(of, "KafkaManagedOffsetCache")
          t.start()
        }.logError("Failed to start KafkaManagedOffsetCache")
      }
    } else {
      throw new IllegalArgumentException(s"Unsupported Kafka Version: ${clusterContext.config.version}")
    }
  }

  def stop() : Unit = {
    kafkaManagedOffsetCache.foreach { of =>
      info("Stopping kafka managed offset cache ...")
      Try {
        of.close()
      }
    }
  }

  def getTopicPartitionOffsets(topic: String, interactive: Boolean) : Future[PartitionOffsetsCapture] = {
    if((interactive || loadOffsets) && hasNonSecureEndpoint) {
      partitionOffsetsCache.get(topic)
    } else {
      emptyPartitionOffsetsCapture
    }
  }

  protected def readKafkaManagedConsumerOffsetByTopicPartition(consumer: String
                                                               , topic: String
                                                               , tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, Long] = {
    kafkaManagedOffsetCache.fold(Map.empty[Int,Long]) {
      oc =>
        tpi.map {
          case (part, _) =>
            part -> oc.getOffset(consumer, topic, part).getOrElse(-1L)
        }
    }
  }

  protected def readKafkaManagedConsumerOwnerByTopicPartition(consumer: String
                                                              , topic: String
                                                              , tpi: Map[Int, TopicPartitionIdentity]) : Map[Int, String] = {
    kafkaManagedOffsetCache.fold(Map.empty[Int,String]) {
      oc =>
        tpi.map {
          case (part, _) =>
            part -> oc.getOwner(consumer, topic, part).getOrElse("")
        }
    }
  }

  protected def getKafkaManagedConsumerTopics(consumer: String) : Set[String] = {
    kafkaManagedOffsetCache.fold(Set.empty[String]) {
      oc => oc.getConsumerTopics(consumer)
    }
  }

  protected def getKafkaManagedConsumerList : IndexedSeq[ConsumerNameAndType] = {
    kafkaManagedOffsetCache.fold(IndexedSeq.empty[ConsumerNameAndType]) {
      oc => oc.getConsumers.map(name => ConsumerNameAndType(name, KafkaManagedConsumer))
    }
  }

  final def lastUpdateMillis : Long = {
    Math.max(lastUpdateMillisZK, kafkaManagedOffsetCache.map(_.getLastUpdateTimeMillis).getOrElse(Long.MinValue))
  }

  final def getConsumerDescription(consumer: String, consumerType: ConsumerType) : ConsumerDescription = {
    val consumerTopics: Set[String] = getKafkaVersion match {
      case Kafka_0_8_1_1 => getConsumerTopicsFromOffsets(consumer)
      case _ =>
        consumerType match {
          case ZKManagedConsumer =>
            getConsumerTopicsFromOffsets(consumer) ++ getConsumerTopicsFromOwners(consumer)
          case KafkaManagedConsumer =>
            getKafkaManagedConsumerTopics(consumer)
        }
    }

    val topicDescriptions: Map[String, ConsumedTopicDescription] = consumerTopics.map { topic =>
      val topicDesc = getConsumedTopicDescription(consumer, topic, false, consumerType)
      (topic, topicDesc)
    }.toMap
    ConsumerDescription(consumer, topicDescriptions, consumerType)
  }

  final def getConsumedTopicDescription(consumer:String
                                        , topic:String
                                        , interactive: Boolean
                                        , consumerType: ConsumerType) : ConsumedTopicDescription = {
    val optTopic = getTopicDescription(topic, interactive)
    val optTpi = optTopic.map(TopicIdentity.getTopicPartitionIdentity(_, None))
    val (partitionOffsets, partitionOwners) = consumerType match {
      case ZKManagedConsumer =>
        val partitionOffsets = for {
          td <- optTopic
          tpi <- optTpi
        } yield {
          readConsumerOffsetByTopicPartition(consumer, topic, tpi)
        }
        val partitionOwners = for {
          td <- optTopic
          tpi <- optTpi
        } yield {
          readConsumerOwnerByTopicPartition(consumer, topic, tpi)
        }
        (partitionOffsets, partitionOwners)
      case KafkaManagedConsumer =>
        val partitionOffsets = for {
          td <- optTopic
          tpi <- optTpi
        } yield {
          readKafkaManagedConsumerOffsetByTopicPartition(consumer, topic, tpi)
        }
        val partitionOwners = for {
          td <- optTopic
          tpi <- optTpi
        } yield {
          readKafkaManagedConsumerOwnerByTopicPartition(consumer, topic, tpi)
        }
        (partitionOffsets, partitionOwners)
    }

    val numPartitions: Int = math.max(optTopic.flatMap(_.partitionState.map(_.size)).getOrElse(0),
      partitionOffsets.map(_.size).getOrElse(0))
    ConsumedTopicDescription(consumer, topic, numPartitions, optTopic, partitionOwners, partitionOffsets)
  }

  final def getConsumerList: ConsumerList = {
    ConsumerList(getKafkaManagedConsumerList ++ getZKManagedConsumerList, clusterContext)
  }
}

/**
 * KRaft-compatible offset cache — no ZK consumers, only Kafka-managed consumers.
 */
case class OffsetCacheKraft(kafkaAdminClient: KafkaAdminClient
                            , clusterContext: ClusterContext
                            , partitionLeaders: String => Option[List[(Int, Option[BrokerIdentity])]]
                            , topicDescriptions: (String, Boolean) => Option[TopicDescription]
                            , cacheTimeoutSecs: Int
                            , socketTimeoutMillis: Int
                            , kafkaVersion: KafkaVersion
                            , consumerProperties: Option[Properties]
                            , kafkaManagedOffsetCacheConfig: KafkaManagedOffsetCacheConfig
                           )
                           (implicit protected[this] val ec: ExecutionContext, val cf: ClusterFeatures) extends OffsetCache {

  def getKafkaVersion: KafkaVersion = kafkaVersion
  def getCacheTimeoutSecs: Int = cacheTimeoutSecs
  def getSimpleConsumerSocketTimeoutMillis: Int = socketTimeoutMillis

  val loadOffsets = featureGateFold(KMPollConsumersFeature)(false, true)

  override protected def hasNonSecureEndpointOverride: Boolean = true

  protected def getTopicPartitionLeaders(topic: String): Option[List[(Int, Option[BrokerIdentity])]] = partitionLeaders(topic)
  protected def getTopicDescription(topic: String, interactive: Boolean): Option[TopicDescription] = topicDescriptions(topic, interactive)

  protected def lastUpdateMillisZK: Long = System.currentTimeMillis()

  // No ZK consumers in KRaft mode
  protected def readConsumerOffsetByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]): Map[Int, Long] = Map.empty
  protected def readConsumerOwnerByTopicPartition(consumer: String, topic: String, tpi: Map[Int, TopicPartitionIdentity]): Map[Int, String] = Map.empty
  protected def getConsumerTopicsFromIds(consumer: String): Set[String] = Set.empty
  protected def getConsumerTopicsFromOffsets(consumer: String): Set[String] = Set.empty
  protected def getConsumerTopicsFromOwners(consumer: String): Set[String] = Set.empty
  protected def getZKManagedConsumerList: IndexedSeq[ConsumerNameAndType] = IndexedSeq.empty
}

case class KafkaStateActorConfig(pinnedDispatcherName: String
                                 , clusterContext: ClusterContext
                                 , offsetCachePoolConfig: LongRunningPoolConfig
                                 , kafkaAdminClientPoolConfig: LongRunningPoolConfig
                                 , partitionOffsetCacheTimeoutSecs: Int
                                 , simpleConsumerSocketTimeoutMillis: Int
                                 , consumerProperties: Option[Properties]
                                 , kafkaManagedOffsetCacheConfig: KafkaManagedOffsetCacheConfig
                                )

class KafkaStateActor(config: KafkaStateActorConfig) extends BaseClusterQueryCommandActor with LongRunningPoolActor {

  protected implicit val clusterContext: ClusterContext = config.clusterContext

  protected implicit val cf: ClusterFeatures = clusterContext.clusterFeatures

  override protected def longRunningPoolConfig: LongRunningPoolConfig = config.offsetCachePoolConfig

  override protected def longRunningQueueFull(): Unit = {
    log.error("Long running pool queue full, skipping!")
  }

  private[this] val kaConfig = KafkaAdminClientActorConfig(
    clusterContext,
    config.kafkaAdminClientPoolConfig,
    config.consumerProperties
  )
  private[this] val kaProps = Props(classOf[KafkaAdminClientActor],kaConfig)
  private[this] val kafkaAdminClientActor : ActorPath = context.actorOf(kaProps.withDispatcher(config.pinnedDispatcherName),"kafka-admin-client").path
  private[this] val kafkaAdminClient = new KafkaAdminClient(context, kafkaAdminClientActor)

  // Admin client for state polling (brokers, topics, configs)
  @volatile private[this] var stateAdminClient: Option[AdminClient] = None
  private[this] var pollSchedule: Option[Cancellable] = None

  // In-memory state populated by Admin API polling (replaces ZK caches)
  @volatile private[this] var cachedBrokers: IndexedSeq[BrokerIdentity] = IndexedSeq.empty
  @volatile private[this] var cachedTopicDescriptions: Map[String, TopicDescription] = Map.empty
  @volatile private[this] var cachedBrokerConfigs: Map[Int, (Int, String)] = Map.empty
  @volatile private[this] var topicsLastUpdateMillis: Long = System.currentTimeMillis()

  @volatile
  private[this] var preferredLeaderElection : Option[PreferredReplicaElection] = None

  @volatile
  private[this] var reassignPartitions : Option[ReassignPartitions] = None

  private[this] lazy val offsetCache: OffsetCache = {
    OffsetCacheKraft(
      kafkaAdminClient
      , config.clusterContext
      , getPartitionLeaders
      , getTopicDescription
      , config.partitionOffsetCacheTimeoutSecs
      , config.simpleConsumerSocketTimeoutMillis
      , config.clusterContext.config.version
      , config.consumerProperties
      , config.kafkaManagedOffsetCacheConfig
    )(longRunningExecutionContext, cf)
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    log.info(config.toString)
    log.info("Started actor %s".format(self.path))

    // Create Admin Client for state polling
    Try {
      stateAdminClient = Some(createStateAdminClient())
    }.recover { case e => log.error(e, "Failed to create state AdminClient for KafkaStateActor") }

    // Schedule periodic polling (immediately then every 30s)
    pollSchedule = Some(
      context.system.scheduler.schedule(0.seconds, 30.seconds, self, KSPollKafkaState)(context.system.dispatcher, self)
    )

    // Start offset cache (uses bootstrapServers directly)
    Try(offsetCache.start()).recover { case e => log.error(e, "Failed to start offset cache") }

    startTopicOffsetGetter()
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Stopped actor %s".format(self.path))

    Try(stopTopicOffsetGetter())
    log.info("Stopping offset cache...")
    Try(offsetCache.stop())

    log.info("Cancelling poll schedule...")
    Try(pollSchedule.foreach(_.cancel()))

    log.info("Closing state AdminClient...")
    Try(stateAdminClient.foreach(_.close()))

    super.postStop()
  }

  private[this] def createStateAdminClient(): AdminClient = {
    val props = new Properties()
    config.consumerProperties.foreach { cp => props.putAll(cp.asMap) }
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.clusterContext.config.securityProtocol.stringId)
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.clusterContext.config.bootstrapServers)
    if(config.clusterContext.config.saslMechanism.nonEmpty){
      props.put(SaslConfigs.SASL_MECHANISM, config.clusterContext.config.saslMechanism.get.stringId)
    }
    if(config.clusterContext.config.jaasConfig.nonEmpty){
      props.put(SaslConfigs.SASL_JAAS_CONFIG, config.clusterContext.config.jaasConfig.get)
    }
    AdminClient.create(props)
  }

  private[this] def pollKafkaState(): Unit = {
    val client = stateAdminClient.getOrElse {
      Try { stateAdminClient = Some(createStateAdminClient()) }
      stateAdminClient.getOrElse { return }
    }
    Try {
      // Poll brokers
      val nodes = client.describeCluster().nodes().get().asScala.toIndexedSeq
      val newBrokers = nodes.flatMap { node =>
        val json = synthesizeBrokerJson(node.id(), node.host(), node.port())
        BrokerIdentity.from(node.id(), json).toOption
      }.sortBy(_.id)
      cachedBrokers = newBrokers

      // Poll broker configs
      if (newBrokers.nonEmpty) {
        Try {
          val brokerResources = newBrokers.map(b => new ConfigResource(ConfigResource.Type.BROKER, b.id.toString))
          val brokerConfigMap = client.describeConfigs(brokerResources.asJava).all().get().asScala
          cachedBrokerConfigs = brokerConfigMap.map { case (resource, cfg) =>
            resource.name().toInt -> (0, synthesizeConfigJson(cfg))
          }.toMap
        }.recover { case e => log.error(e, "Failed to poll broker configs") }
      }

      // Poll topics
      val topicNames = client.listTopics().names().get().asScala.toSet
      if (topicNames.nonEmpty) {
        val adminTopicDescMap = client.describeTopics(topicNames.asJava).all().get().asScala

        val topicConfigResources = topicNames.map(t => new ConfigResource(ConfigResource.Type.TOPIC, t))
        val topicConfigMap: mutable.Map[ConfigResource, AdminConfig] =
          Try(client.describeConfigs(topicConfigResources.asJava).all().get().asScala)
            .getOrElse(mutable.Map.empty)

        val newDescriptions: Map[String, TopicDescription] = adminTopicDescMap.map { case (topicName, adminDesc) =>
          val partitions = adminDesc.partitions().asScala

          // Synthesize topic assignment JSON: {"version":1,"partitions":{"0":[0,1],"1":[1,0]}}
          val partitionAssignments = partitions.map { tpi =>
            tpi.partition().toString -> tpi.replicas().asScala.map(_.id())
          }.toMap
          val topicJson = synthesizeTopicAssignmentJson(partitionAssignments)

          // Synthesize partition states: Map[partitionNum -> stateJson]
          val partitionStates: Map[String, String] = partitions.map { tpi =>
            val leader = Option(tpi.leader()).map(_.id()).getOrElse(-1)
            val isr = tpi.isr().asScala.map(_.id())
            val stateJson = s"""{"controller_epoch":0,"leader":$leader,"version":1,"leader_epoch":0,"isr":[${isr.mkString(",")}]}"""
            tpi.partition().toString -> stateJson
          }.toMap

          val topicCfgRes = new ConfigResource(ConfigResource.Type.TOPIC, topicName)
          val topicConfigJson: Option[(Int, String)] = topicConfigMap.get(topicCfgRes).map { cfg =>
            (0, synthesizeConfigJson(cfg))
          }

          val partitionOffsets = getTopicPartitionOffsetsNotFuture(topicName, false)
          topicName -> TopicDescription(topicName, (0, topicJson), Some(partitionStates), partitionOffsets, topicConfigJson)
        }.toMap

        cachedTopicDescriptions = newDescriptions
      } else {
        cachedTopicDescriptions = Map.empty
      }

      topicsLastUpdateMillis = System.currentTimeMillis()

      // Poll in-progress partition reassignments (replaces ZK watcher in KRaft mode)
      Try {
        val inProgress = client.listPartitionReassignments().reassignments().get().asScala
        if (inProgress.isEmpty) {
          reassignPartitions = reassignPartitions.flatMap { existing =>
            existing.endTime.fold(Some(existing.copy(endTime = Some(getDateTime(System.currentTimeMillis())))))(t => Some(existing))
          }
        } else {
          val m: Map[TopicPartition, Seq[Int]] = inProgress.map { case (tp, r) =>
            tp -> r.replicas().asScala.map(_.toInt).toSeq
          }.toMap
          reassignPartitions = reassignPartitions.fold {
            Some(ReassignPartitions(getDateTime(System.currentTimeMillis()), m, None, config.clusterContext))
          } { existing =>
            Some(existing.copy(partitionsToBeReassigned = m, endTime = None))
          }
        }
      }.recover { case e => log.error(e, "Failed to poll partition reassignments") }

    }.recover { case e => log.error(e, "Failed to poll Kafka state") }
  }

  private[this] def synthesizeBrokerJson(id: Int, host: String, port: Int): String = {
    val jmxPort = if (config.clusterContext.config.jmxEnabled) config.clusterContext.config.jmxPort else -1
    s"""{"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":["PLAINTEXT://$host:$port"],"rack":null,"jmx_port":$jmxPort,"host":"$host","timestamp":"${System.currentTimeMillis()}","port":$port,"version":4}"""
  }

  private[this] def synthesizeTopicAssignmentJson(partitionAssignments: scala.collection.Map[String, Seq[Int]]): String = {
    val parts = partitionAssignments.map { case (p, replicas) =>
      s""""$p":[${replicas.mkString(",")}]"""
    }.mkString(",")
    s"""{"version":1,"partitions":{$parts}}"""
  }

  private[this] def synthesizeConfigJson(cfg: AdminConfig): String = {
    val entries = cfg.entries().asScala
      .filter(e => e.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG &&
                   e.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
      .map(e => s""""${e.name()}":"${e.value()}"""")
      .mkString(",")
    s"""{"version":1,"config":{$entries}}"""
  }

  def getTopicZookeeperData(topic: String): Option[(Int,String)] = {
    cachedTopicDescriptions.get(topic).map(_.description)
  }

  def getTopicPartitionOffsetsNotFuture(topic: String, interactive: Boolean): PartitionOffsetsCapture = {
    var partitionOffsets = PartitionOffsetsCapture(System.currentTimeMillis(), Map())
    val loadOffsets = featureGateFold(KMPollConsumersFeature)(false, true)
    if ((interactive || loadOffsets) && kafkaTopicOffsetCaptureMap.contains(topic)) {
      partitionOffsets = kafkaTopicOffsetCaptureMap(topic)
    }
    partitionOffsets
  }

  def getBrokerDescription(broker:Int): Option[BrokerDescription] = {
    cachedBrokerConfigs.get(broker).map(c => BrokerDescription(broker, Some(c)))
  }

  def getTopicDescription(topic: String, interactive: Boolean): Option[TopicDescription] = {
    cachedTopicDescriptions.get(topic)
  }

  def getPartitionLeaders(topic: String): Option[List[(Int, Option[BrokerIdentity])]] = {
    cachedTopicDescriptions.get(topic).flatMap { td =>
      td.partitionState.map { states =>
        import org.json4s.jackson.JsonMethods.parse
        import org.json4s.scalaz.JsonScalaz.field
        states.map { case (partStr, stateJson) =>
          val partition = partStr.toInt
          val descJson = parse(stateJson)
          val leaderID = field[Int]("leader")(descJson).fold({ _ => -1 }, identity)
          val leader = cachedBrokers.find(_.id == leaderID)
          (partition, leader)
        }.toList
      }
    }
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("ksa : processActorResponse : Received unknown message: {}", any.toString)
    }
  }

  private[this] def getBrokers : IndexedSeq[BrokerIdentity] = cachedBrokers

  private[this] def asyncPipeToSender[T](fn: => T):  Unit = {
    implicit val ec = longRunningExecutionContext
    val result: Future[T] = Future { fn }
    result pipeTo sender
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case KSGetTopics =>
        val topics = cachedTopicDescriptions.keySet.toIndexedSeq
        sender ! TopicList(topics, Set.empty, config.clusterContext)

      case KSGetConsumers =>
        asyncPipeToSender {
          offsetCache.getConsumerList
        }

      case KSGetTopicConfig(topic) =>
        sender ! TopicConfig(topic, cachedTopicDescriptions.get(topic).flatMap(_.config))

      case KSGetTopicDescription(topic) =>
        sender ! getTopicDescription(topic, false)

      case KSGetBrokerDescription(broker)=>
        sender ! getBrokerDescription(broker)

      case KSGetTopicDescriptions(topics) =>
        sender ! TopicDescriptions(topics.toIndexedSeq.flatMap(getTopicDescription(_, false)), topicsLastUpdateMillis)

      case KSGetConsumerDescription(consumer, consumerType) =>
        asyncPipeToSender {
          offsetCache.getConsumerDescription(consumer, consumerType)
        }

      case KSGetConsumedTopicDescription(consumer, topic, consumerType) =>
        asyncPipeToSender {
          offsetCache.getConsumedTopicDescription(consumer, topic, true, consumerType)
        }

      case KSGetAllTopicDescriptions(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (topicsLastUpdateMillis > lastUpdateMillis || ((topicsLastUpdateMillis + (config.partitionOffsetCacheTimeoutSecs * 1000)) < System.currentTimeMillis())) {
          sender ! TopicDescriptions(cachedTopicDescriptions.values.toIndexedSeq, topicsLastUpdateMillis)
        }

      case KSGetAllConsumerDescriptions(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (offsetCache.lastUpdateMillis > lastUpdateMillis) {
          asyncPipeToSender {
            ConsumerDescriptions(offsetCache
              .getConsumerList
              .list
              .map(c => offsetCache.getConsumerDescription(c.name, c.consumerType)), offsetCache.lastUpdateMillis)
          }
        }

      case KSGetTopicsLastUpdateMillis =>
        sender ! topicsLastUpdateMillis

      case KSGetBrokers =>
        sender ! getBrokerList

      case KSGetPreferredLeaderElection =>
        sender ! preferredLeaderElection

      case KSGetReassignPartition =>
        sender ! reassignPartitions

      case any: Any => log.warning("ksa : processQueryRequest : Received unknown message: {}", any.toString)
    }
  }

  private def getBrokerList : BrokerList = {
    BrokerList(getBrokers, config.clusterContext)
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case KSPollKafkaState =>
        pollKafkaState()

      case KSUpdatePreferredLeaderElection(millis,json) =>
        safeExecute {
          import kafka.manager.utils.zero81.PreferredReplicaLeaderElectionCommand
          val s: Set[TopicPartition] = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(json)
          preferredLeaderElection.fold {
            preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None, config.clusterContext))
          } {
            existing =>
              existing.endTime.fold {
                preferredLeaderElection = Some(existing.copy(topicAndPartition = existing.topicAndPartition ++ s))
              } { _ =>
                preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None, config.clusterContext))
              }
          }
        }
      case KSSetReassignPartitions(m) =>
        safeExecute {
          reassignPartitions = Some(ReassignPartitions(getDateTime(System.currentTimeMillis()), m, None, config.clusterContext))
        }

      case KSUpdateReassignPartition(millis,json) =>
        safeExecute {
          import kafka.manager.utils.zero81.ReassignPartitionCommand
          val m : Map[TopicPartition, Seq[Int]] = ReassignPartitionCommand.parsePartitionReassignmentZkData(json)
          reassignPartitions.fold {
            reassignPartitions = Some(ReassignPartitions(getDateTime(millis),m, None, config.clusterContext))
          } {
            existing =>
              existing.endTime.fold {
                reassignPartitions = Some(existing.copy(partitionsToBeReassigned = existing.partitionsToBeReassigned ++ m))
              } { _ =>
                reassignPartitions = Some(ReassignPartitions(getDateTime(millis),m, None, config.clusterContext))
              }
          }
        }
      case KSEndPreferredLeaderElection(millis) =>
        safeExecute {
          preferredLeaderElection.foreach { existing =>
            preferredLeaderElection = Some(existing.copy(endTime = Some(getDateTime(millis))))
          }
        }
      case KSEndReassignPartition(millis) =>
        safeExecute {
          reassignPartitions.foreach { existing =>
            reassignPartitions = Some(existing.copy(endTime = Some(getDateTime(millis))))
          }
        }
      case any: Any => log.warning("ksa : processCommandRequest : Received unknown message: {}", any.toString)
    }
  }

  private[this] def getDateTime(millis: Long) : DateTime = new DateTime(millis,DateTimeZone.UTC)

  private[this] def safeExecute(fn: => Any) : Unit = {
    Try(fn) match {
      case Failure(t) =>
        log.error("Failed!",t)
      case Success(_) =>
      //do nothing
    }
  }

  //---------------------------------------------------
  private[this] var kafkaTopicOffsetGetter : Option[KafkaTopicOffsetGetter] = None
  private[this] var kafkaTopicOffsetMap = new TrieMap[String, Map[Int, Long]]
  private[this] var kafkaTopicOffsetCaptureMap = new TrieMap[String, PartitionOffsetsCapture]

  def startTopicOffsetGetter() : Unit = {
    log.info("Starting kafka managed Topic Offset Getter ...")
    kafkaTopicOffsetGetter = Option(new KafkaTopicOffsetGetter())
    val topicOffsetGetterThread = new Thread(kafkaTopicOffsetGetter.get, "KafkaTopicOffsetGetter")
    topicOffsetGetterThread.start()
  }

  def stopTopicOffsetGetter() : Unit = {
    kafkaTopicOffsetGetter.foreach {
      kto =>
        Try {
          log.info("Stopping kafka managed Topic Offset Getter ...")
          kto.close()
        }
    }
  }

  class KafkaTopicOffsetGetter() extends Runnable {
    @volatile
    private[this] var shutdown: Boolean = false

    override def run(): Unit = {
      import scala.util.control.Breaks._

      while (!shutdown) {
        try {
          val topics = cachedTopicDescriptions.keySet.toIndexedSeq
          if (topics.nonEmpty) {
            var broker2TopicPartitionMap: Map[BrokerIdentity, List[(TopicPartition, PartitionOffsetRequestInfo)]] = Map()

            breakable {
              topics.foreach(topic => {
                if (shutdown) {
                  return
                }
                getPartitionLeaders(topic) match {
                  case Some(leaders) =>
                    leaders.foreach(leader => {
                      leader._2 match {
                        case Some(brokerIden) =>
                          var tlList : List[(TopicPartition, PartitionOffsetRequestInfo)] = broker2TopicPartitionMap.getOrElse(brokerIden, List())
                          tlList = (new TopicPartition(topic, leader._1), PartitionOffsetRequestInfo(-1, 1)) +: tlList
                          broker2TopicPartitionMap += (brokerIden -> tlList)
                        case None =>
                      }
                    })
                  case None =>
                }
              })
            }

            breakable {
              broker2TopicPartitionMap.keys.foreach(broker => {
                if (shutdown) {
                  return
                }

                val tpList = broker2TopicPartitionMap(broker)
                val consumerProperties = kaConfig.consumerProperties.getOrElse(getDefaultConsumerProperties())
                val securityProtocol = Option(kaConfig.clusterContext.config.securityProtocol).getOrElse(PLAINTEXT)
                val port: Int = broker.endpoints.getOrElse(securityProtocol, broker.endpoints.values.headOption.getOrElse(9092))
                consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, s"${broker.host}:$port")
                consumerProperties.put(SECURITY_PROTOCOL_CONFIG, securityProtocol.stringId)
                consumerProperties.put(METRIC_REPORTER_CLASSES_CONFIG, classOf[NoopJMXReporter].getCanonicalName)
                if(kaConfig.clusterContext.config.saslMechanism.nonEmpty){
                  consumerProperties.put(SaslConfigs.SASL_MECHANISM, kaConfig.clusterContext.config.saslMechanism.get.stringId)
                }
                if(kaConfig.clusterContext.config.jaasConfig.nonEmpty){
                  consumerProperties.put(SaslConfigs.SASL_JAAS_CONFIG, kaConfig.clusterContext.config.jaasConfig.get)
                }
                var kafkaConsumer: Option[KafkaConsumer[Any, Any]] = None
                try {
                  kafkaConsumer = Option(new KafkaConsumer(consumerProperties))
                  val request = tpList.map(f => new TopicPartition(f._1.topic(), f._1.partition()))
                  val tpOffsetMapOption = kafkaConsumer.map(_.endOffsets(request.asJavaCollection).asScala)

                  tpOffsetMapOption.foreach(tpOffsetMap => tpOffsetMap.keys.foreach(tp => {
                    val topicOffsetMap = kafkaTopicOffsetMap.getOrElse(tp.topic, Map[Int, Long]())
                    kafkaTopicOffsetMap += (tp.topic -> (topicOffsetMap + (tp.partition -> tpOffsetMap(tp).toLong)))
                  }))
                } catch {
                  case e: Exception =>
                    log.error(e, s"Error getting topic offsets from broker $broker")
                } finally {
                  kafkaConsumer.foreach(_.close())
                }
              })
            }

            kafkaTopicOffsetCaptureMap = kafkaTopicOffsetMap.map(kv =>
              (kv._1, PartitionOffsetsCapture(System.currentTimeMillis(), kv._2)))
          }
        } catch {
          case e: Exception =>
            log.error(e, s"KafkaTopicOffsetGetter exception ")
        }

        if (!shutdown) {
          Thread.sleep(config.partitionOffsetCacheTimeoutSecs * 1000)
        }
      }

      log.info(s"KafkaTopicOffsetGetter exit")
    }

    def close(): Unit = {
      this.shutdown = true
    }

    def getDefaultConsumerProperties(): Properties = {
      val properties = new Properties()
      properties.put(GROUP_ID_CONFIG, getClass.getCanonicalName)
      properties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      properties
    }
  }
}
