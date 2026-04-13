/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor.{ActorPath, Props}
import akka.pattern._
import kafka.manager.actor.cluster.{ClusterManagerActor, ClusterManagerActorConfig}
import kafka.manager.base.{LongRunningPoolConfig, BaseQueryCommandActor}
import kafka.manager.model.{ClusterTuning, ClusterConfig}
import kafka.manager.model.ActorModel.CMShutdown
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Kafka Manager Actor — KRaft edition.
 * Cluster configurations are persisted to a local JSON file instead of ZooKeeper.
 */

object KafkaManagerActor {
  // kept for backward compat references; no longer a ZK path
  val ZkRoot : String = "/kafka-manager"
}

import kafka.manager.model.ActorModel._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

case class KafkaManagerActorConfig(clusterConfigFile: String
                                   , pinnedDispatcherName : String = "pinned-dispatcher"
                                   , startDelayMillis: Long = 1000
                                   , threadPoolSize: Int = 2
                                   , maxQueueSize: Int = 100
                                   , kafkaManagerUpdatePeriod: FiniteDuration = 10 seconds
                                   , deleteClusterUpdatePeriod: FiniteDuration = 10 seconds
                                   , deletionBatchSize : Int = 2
                                   , clusterActorsAskTimeoutMillis: Int = 2000
                                   , simpleConsumerSocketTimeoutMillis : Int = 10000
                                   , defaultTuning: ClusterTuning
                                   , consumerProperties: Option[Properties]
                                  )

class KafkaManagerActor(kafkaManagerConfig: KafkaManagerActorConfig)
  extends BaseQueryCommandActor {

  private[this] val configFile = new File(kafkaManagerConfig.clusterConfigFile)

  private[this] val longRunningExecutor = new ThreadPoolExecutor(
    kafkaManagerConfig.threadPoolSize,
    kafkaManagerConfig.threadPoolSize,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable](kafkaManagerConfig.maxQueueSize))

  private[this] val longRunningExecutionContext = ExecutionContext.fromExecutor(longRunningExecutor)

  private[this] var lastUpdateMillis: Long = 0L
  private[this] var clusterManagerMap : Map[String,ActorPath] = Map.empty
  private[this] var clusterConfigMap : Map[String,ClusterConfig] = Map.empty
  private[this] var pendingClusterConfigMap : Map[String,ClusterConfig] = Map.empty
  // cluster names pending deletion
  private[this] var deletingClusters : Set[String] = Set.empty

  // --- File persistence ---

  private[this] def loadClustersFromFile(): Map[String, ClusterConfig] = {
    if (!configFile.exists()) {
      log.info(s"Cluster config file not found, starting with empty registry: ${configFile.getAbsolutePath}")
      return Map.empty
    }
    Try {
      val json = new String(Files.readAllBytes(configFile.toPath), StandardCharsets.UTF_8)
      val parsed = parse(json)
      parsed match {
        case JArray(list) =>
          list.flatMap { item =>
            ClusterConfig.deserialize(compact(render(item)).getBytes(StandardCharsets.UTF_8)) match {
              case Success(cc) => Some(cc.name -> cc)
              case Failure(t) =>
                log.error(s"Failed to deserialize cluster config from file: $t")
                None
            }
          }.toMap[String, ClusterConfig]
        case _ =>
          log.error("Cluster config file does not contain a JSON array")
          Map.empty[String, ClusterConfig]
      }
    } match {
      case Success(m) => m
      case Failure(t) =>
        log.error(s"Failed to read cluster config file: $t")
        Map.empty
    }
  }

  private[this] def persistClusters(): Unit = {
    Try {
      val configs = clusterConfigMap.values.toList
      val jsonStr = "[" + configs.map { cc =>
        new String(ClusterConfig.serialize(cc), StandardCharsets.UTF_8)
      }.mkString(",") + "]"
      val tmpFile = new File(configFile.getAbsolutePath + ".tmp")
      configFile.getParentFile.mkdirs()
      val pw = new PrintWriter(tmpFile, "UTF-8")
      try { pw.print(jsonStr) } finally { pw.close() }
      Files.move(tmpFile.toPath, configFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    } match {
      case Failure(t) => log.error(s"Failed to persist cluster configs: $t")
      case _ =>
    }
  }

  // --- Actor lifecycle ---

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    log.info(s"Started actor ${self.path}, cluster config file: ${configFile.getAbsolutePath}")

    // Load initial cluster state from file
    val loaded = loadClustersFromFile()
    loaded.values.foreach { cc =>
      val configWithDefaults = getConfigWithDefaults(cc, kafkaManagerConfig)
      addCluster(configWithDefaults)
    }
    lastUpdateMillis = System.currentTimeMillis()

    implicit val ec = longRunningExecutionContext
    context.system.scheduler.schedule(
      Duration(kafkaManagerConfig.startDelayMillis, TimeUnit.MILLISECONDS),
      kafkaManagerConfig.kafkaManagerUpdatePeriod) {
      self ! KMUpdateState
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info(s"Stopped actor ${self.path}")
    Try(longRunningExecutor.shutdown())
    super.postStop()
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("kma : processActorResponse : Received unknown message: {}", any)
    }
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case KMGetActiveClusters =>
        sender ! KMQueryResult(clusterConfigMap.values.filter(_.enabled).toIndexedSeq)

      case KMGetAllClusters =>
        sender ! KMClusterList(clusterConfigMap.values.toIndexedSeq, pendingClusterConfigMap.values.toIndexedSeq)

      case KSGetScheduleLeaderElection =>
        // No ZK-based scheduled leader election in KRaft mode
        sender ! "{}"

      case KMGetClusterConfig(name) =>
        sender ! KMClusterConfigResult(Try {
          val cc = clusterConfigMap.get(name)
          require(cc.isDefined, s"Unknown cluster : $name")
          cc.get
        })

      case KMClusterQueryRequest(clusterName, request) =>
        clusterManagerMap.get(clusterName).fold[Unit] {
          sender ! ActorErrorResponse(s"Unknown cluster : $clusterName")
        } {
          clusterManagerPath:ActorPath =>
            context.actorSelection(clusterManagerPath).forward(request)
        }

      case any: Any => log.warning("kma : processQueryRequest : Received unknown message: {}", any)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case KMAddCluster(clusterConfig) =>
        implicit val ec = longRunningExecutionContext
        Future {
          KMCommandResult(Try {
            require(!(clusterConfig.displaySizeEnabled && !clusterConfig.jmxEnabled),
              "Display topic and broker size can only be enabled when JMX is enabled")
            require(!(clusterConfig.filterConsumers && !clusterConfig.pollConsumers),
              "Filter consumers can only be enabled when consumer polling is enabled")
            require(!clusterConfigMap.contains(clusterConfig.name),
              s"Cluster already exists : ${clusterConfig.name}")
            require(!deletingClusters.contains(clusterConfig.name),
              s"Cluster is marked for deletion : ${clusterConfig.name}")
            val withDefaults = getConfigWithDefaults(clusterConfig, kafkaManagerConfig)
            clusterConfigMap += (withDefaults.name -> withDefaults)
            persistClusters()
            addCluster(withDefaults)
          })
        } pipeTo sender()

      case KMUpdateCluster(clusterConfig) =>
        implicit val ec = longRunningExecutionContext
        Future {
          KMCommandResult(Try {
            require(!(clusterConfig.displaySizeEnabled && !clusterConfig.jmxEnabled),
              "Display topic and broker size can only be enabled when JMX is enabled")
            require(!(clusterConfig.filterConsumers && !clusterConfig.pollConsumers),
              "Filter consumers can only be enabled when consumer polling is enabled")
            require(!deletingClusters.contains(clusterConfig.name),
              s"Cluster is marked for deletion : ${clusterConfig.name}")
            require(clusterConfigMap.contains(clusterConfig.name),
              s"Cannot update non-existing cluster : ${clusterConfig.name}")
            val withDefaults = getConfigWithDefaults(clusterConfig, kafkaManagerConfig)
            val current = clusterConfigMap(withDefaults.name)
            clusterConfigMap += (withDefaults.name -> withDefaults)
            persistClusters()
            updateCluster(current, withDefaults)
          })
        } pipeTo sender()

      case KMDisableCluster(clusterName) =>
        implicit val ec = longRunningExecutionContext
        Future {
          KMCommandResult(Try {
            val existingConfigOption = clusterConfigMap.get(clusterName)
            require(existingConfigOption.isDefined, s"Cannot disable non-existing cluster : $clusterName")
            require(!deletingClusters.contains(clusterName), s"Cluster is marked for deletion : $clusterName")
            existingConfigOption.foreach { existingConfig =>
              val disabledConfig = existingConfig.copy(enabled = false)
              clusterConfigMap += (clusterName -> disabledConfig)
              persistClusters()
              updateCluster(existingConfig, disabledConfig)
            }
          })
        } pipeTo sender()

      case KMEnableCluster(clusterName) =>
        implicit val ec = longRunningExecutionContext
        Future {
          KMCommandResult(Try {
            val existingManagerOption = clusterManagerMap.get(clusterName)
            require(existingManagerOption.isEmpty, s"Cannot enable already enabled cluster : $clusterName")
            val existingConfigOption = clusterConfigMap.get(clusterName)
            require(existingConfigOption.isDefined, s"Cannot enable non-existing cluster : $clusterName")
            require(!deletingClusters.contains(clusterName), s"Cluster is marked for deletion : $clusterName")
            existingConfigOption.foreach { existingConfig =>
              val enabledConfig = existingConfig.copy(enabled = true)
              clusterConfigMap += (clusterName -> enabledConfig)
              persistClusters()
              updateCluster(existingConfig, enabledConfig)
            }
          })
        } pipeTo sender()

      case KMDeleteCluster(clusterName) =>
        implicit val ec = longRunningExecutionContext
        Future {
          KMCommandResult(Try {
            val existingManagerOption = clusterManagerMap.get(clusterName)
            require(existingManagerOption.isEmpty, s"Cannot delete enabled cluster : $clusterName")
            val existingConfigOption = clusterConfigMap.get(clusterName)
            require(existingConfigOption.isDefined, s"Cannot delete non-existing cluster : $clusterName")
            require(existingConfigOption.exists(!_.enabled), s"Cannot delete enabled cluster : $clusterName")
            deletingClusters += clusterName
            clusterConfigMap -= clusterName
            pendingClusterConfigMap -= clusterName
            persistClusters()
          })
        } pipeTo sender()

      case KMClusterCommandRequest(clusterName, request) =>
        clusterManagerMap.get(clusterName).fold[Unit] {
          sender ! ActorErrorResponse(s"Unknown cluster : $clusterName")
        } {
          clusterManagerPath:ActorPath =>
            context.actorSelection(clusterManagerPath).forward(request)
        }

      case KMUpdateState =>
        updateState()

      case KMPruneClusters =>
        pruneClusters()

      case KMShutdown =>
        log.info(s"Shutting down kafka manager")
        context.children.foreach(context.stop)
        shutdown = true

      case any: Any => log.warning("kma : processCommandRequest : Received unknown message: {}", any)
    }
  }

  private[this] def markPendingClusterManager(clusterConfig: ClusterConfig): Unit = {
    log.info(s"Mark pending cluster manager $clusterConfig")
    pendingClusterConfigMap += (clusterConfig.name -> clusterConfig)
  }

  private[this] def removeClusterManager(clusterConfig: ClusterConfig): Unit = {
    implicit val ec = context.system.dispatcher
    clusterManagerMap.get(clusterConfig.name).foreach { actorPath =>
      log.info(s"Removing cluster manager $clusterConfig")
      val selection = context.actorSelection(actorPath)
      selection.tell(CMShutdown, self)
      selection.resolveOne(1 seconds).foreach(ref => context.stop(ref))
    }
    clusterManagerMap -= clusterConfig.name
    clusterConfigMap -= clusterConfig.name
  }

  private[this] def getConfigWithDefaults(config: ClusterConfig, kmConfig: KafkaManagerActorConfig): ClusterConfig = {
    val brokerViewUpdatePeriodSeconds = config.tuning.flatMap(_.brokerViewUpdatePeriodSeconds) orElse kmConfig.defaultTuning.brokerViewUpdatePeriodSeconds
    val clusterManagerThreadPoolSize = config.tuning.flatMap(_.clusterManagerThreadPoolSize) orElse kmConfig.defaultTuning.clusterManagerThreadPoolSize
    val clusterManagerThreadPoolQueueSize = config.tuning.flatMap(_.clusterManagerThreadPoolQueueSize) orElse kmConfig.defaultTuning.clusterManagerThreadPoolQueueSize
    val kafkaCommandThreadPoolSize = config.tuning.flatMap(_.kafkaCommandThreadPoolSize) orElse kmConfig.defaultTuning.kafkaCommandThreadPoolSize
    val kafkaCommandThreadPoolQueueSize = config.tuning.flatMap(_.kafkaCommandThreadPoolQueueSize) orElse kmConfig.defaultTuning.kafkaCommandThreadPoolQueueSize
    val logkafkaCommandThreadPoolSize = config.tuning.flatMap(_.logkafkaCommandThreadPoolSize) orElse kmConfig.defaultTuning.logkafkaCommandThreadPoolSize
    val logkafkaCommandThreadPoolQueueSize = config.tuning.flatMap(_.logkafkaCommandThreadPoolQueueSize) orElse kmConfig.defaultTuning.logkafkaCommandThreadPoolQueueSize
    val logkafkaUpdatePeriodSeconds = config.tuning.flatMap(_.logkafkaUpdatePeriodSeconds) orElse kmConfig.defaultTuning.brokerViewUpdatePeriodSeconds
    val partitionOffsetCacheTimeoutSecs = config.tuning.flatMap(_.partitionOffsetCacheTimeoutSecs) orElse kmConfig.defaultTuning.partitionOffsetCacheTimeoutSecs
    val brokerViewThreadPoolSize = config.tuning.flatMap(_.brokerViewThreadPoolSize) orElse kmConfig.defaultTuning.brokerViewThreadPoolSize
    val brokerViewThreadPoolQueueSize = config.tuning.flatMap(_.brokerViewThreadPoolQueueSize) orElse kmConfig.defaultTuning.brokerViewThreadPoolQueueSize
    val offsetCacheThreadPoolSize = config.tuning.flatMap(_.offsetCacheThreadPoolSize) orElse kmConfig.defaultTuning.offsetCacheThreadPoolSize
    val offsetCacheThreadPoolQueueSize = config.tuning.flatMap(_.offsetCacheThreadPoolQueueSize) orElse kmConfig.defaultTuning.offsetCacheThreadPoolQueueSize
    val kafkaAdminClientThreadPoolSize = config.tuning.flatMap(_.kafkaAdminClientThreadPoolSize) orElse kmConfig.defaultTuning.kafkaAdminClientThreadPoolSize
    val kafkaAdminClientThreadPoolQueueSize = config.tuning.flatMap(_.kafkaAdminClientThreadPoolQueueSize) orElse kmConfig.defaultTuning.kafkaAdminClientThreadPoolQueueSize
    val kafkaManagedOffsetMetadataCheckMillis = config.tuning.flatMap(_.kafkaManagedOffsetMetadataCheckMillis) orElse kmConfig.defaultTuning.kafkaManagedOffsetMetadataCheckMillis
    val kafkaManagedOffsetGroupCacheSize = config.tuning.flatMap(_.kafkaManagedOffsetGroupCacheSize) orElse kmConfig.defaultTuning.kafkaManagedOffsetGroupCacheSize
    val kafkaManagedOffsetGroupExpireDays = config.tuning.flatMap(_.kafkaManagedOffsetGroupExpireDays) orElse kmConfig.defaultTuning.kafkaManagedOffsetGroupExpireDays

    config.copy(tuning = Option(ClusterTuning(
      brokerViewUpdatePeriodSeconds = brokerViewUpdatePeriodSeconds
      , clusterManagerThreadPoolSize = clusterManagerThreadPoolSize
      , clusterManagerThreadPoolQueueSize = clusterManagerThreadPoolQueueSize
      , kafkaCommandThreadPoolSize = kafkaCommandThreadPoolSize
      , kafkaCommandThreadPoolQueueSize = kafkaCommandThreadPoolQueueSize
      , logkafkaCommandThreadPoolSize = logkafkaCommandThreadPoolSize
      , logkafkaCommandThreadPoolQueueSize = logkafkaCommandThreadPoolQueueSize
      , logkafkaUpdatePeriodSeconds = logkafkaUpdatePeriodSeconds
      , partitionOffsetCacheTimeoutSecs = partitionOffsetCacheTimeoutSecs
      , brokerViewThreadPoolSize = brokerViewThreadPoolSize
      , brokerViewThreadPoolQueueSize = brokerViewThreadPoolQueueSize
      , offsetCacheThreadPoolSize = offsetCacheThreadPoolSize
      , offsetCacheThreadPoolQueueSize = offsetCacheThreadPoolQueueSize
      , kafkaAdminClientThreadPoolSize = kafkaAdminClientThreadPoolSize
      , kafkaAdminClientThreadPoolQueueSize = kafkaAdminClientThreadPoolQueueSize
      , kafkaManagedOffsetMetadataCheckMillis = kafkaManagedOffsetMetadataCheckMillis
      , kafkaManagedOffsetGroupCacheSize = kafkaManagedOffsetGroupCacheSize
      , kafkaManagedOffsetGroupExpireDays = kafkaManagedOffsetGroupExpireDays
    )))
  }

  private[this] def addCluster(config: ClusterConfig): Try[Boolean] = {
    Try {
      if (!config.enabled) {
        log.info("Not adding cluster manager for disabled cluster : {}", config.name)
        clusterConfigMap += (config.name -> config)
        pendingClusterConfigMap -= config.name
        false
      } else {
        log.info("Adding new cluster manager for cluster : {}", config.name)
        val clusterManagerConfig = ClusterManagerActorConfig(
          kafkaManagerConfig.pinnedDispatcherName
          , config
          , askTimeoutMillis = kafkaManagerConfig.clusterActorsAskTimeoutMillis
          , simpleConsumerSocketTimeoutMillis = kafkaManagerConfig.simpleConsumerSocketTimeoutMillis
          , consumerProperties = kafkaManagerConfig.consumerProperties
        )
        val props = Props(classOf[ClusterManagerActor], clusterManagerConfig)
        val newClusterManager = context.actorOf(props, config.name).path
        clusterConfigMap += (config.name -> config)
        clusterManagerMap += (config.name -> newClusterManager)
        pendingClusterConfigMap -= config.name
        true
      }
    }
  }

  private[this] def updateCluster(currentConfig: ClusterConfig, newConfig: ClusterConfig): Try[Boolean] = {
    Try {
      if (newConfig.bootstrapServers == currentConfig.bootstrapServers
        && newConfig.enabled == currentConfig.enabled
        && newConfig.version == currentConfig.version
        && newConfig.jmxEnabled == currentConfig.jmxEnabled
        && newConfig.jmxUser == currentConfig.jmxUser
        && newConfig.jmxPass == currentConfig.jmxPass
        && newConfig.jmxSsl == currentConfig.jmxSsl
        && newConfig.logkafkaEnabled == currentConfig.logkafkaEnabled
        && newConfig.pollConsumers == currentConfig.pollConsumers
        && newConfig.filterConsumers == currentConfig.filterConsumers
        && newConfig.activeOffsetCacheEnabled == currentConfig.activeOffsetCacheEnabled
        && newConfig.displaySizeEnabled == currentConfig.displaySizeEnabled
        && newConfig.tuning == currentConfig.tuning
        && newConfig.securityProtocol == currentConfig.securityProtocol
        && newConfig.saslMechanism == currentConfig.saslMechanism
        && newConfig.jaasConfig == currentConfig.jaasConfig
      ) {
        false
      } else {
        log.info("Updating cluster manager for cluster={} , old={}, new={}",
          currentConfig.name, currentConfig, newConfig)
        markPendingClusterManager(newConfig)
        removeClusterManager(currentConfig)
        true
      }
    }
  }

  private[this] def updateState(): Unit = {
    log.debug("Updating internal state from file...")
    val loaded = loadClustersFromFile()
    loaded.values.foreach { newConfig =>
      val configWithDefaults = getConfigWithDefaults(newConfig, kafkaManagerConfig)
      clusterConfigMap.get(newConfig.name)
        .fold(addCluster(configWithDefaults))(updateCluster(_, configWithDefaults))
    }
    lastUpdateMillis = System.currentTimeMillis()
  }

  private[this] def pruneClusters(): Unit = {
    log.debug("Pruning clusters...")
    val loaded = loadClustersFromFile()
    clusterConfigMap.foreach { case (name, clusterConfig) =>
      if (!loaded.contains(name)) {
        pendingClusterConfigMap -= clusterConfig.name
        removeClusterManager(clusterConfig)
        clusterConfigMap -= name
      }
    }
    lastUpdateMillis = System.currentTimeMillis()
  }
}
