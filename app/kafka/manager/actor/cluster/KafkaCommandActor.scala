/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor.cluster

import java.util.Properties

import kafka.manager.base.cluster.BaseClusterCommandActor
import kafka.manager.base.{LongRunningPoolActor, LongRunningPoolConfig}
import kafka.manager.features.KMDeleteTopicFeature
import kafka.manager.model.ActorModel._
import kafka.manager.model.ClusterContext
import kafka.manager.utils.zero81.{ForceReassignmentCommand, ReassignPartitionCommand}
import kafka.manager.utils.AdminUtils
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.apache.kafka.common.config.{ConfigResource, SaslConfigs}
import org.apache.kafka.common.errors.TopicExistsException

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Try}

/**
 * @author hiral
 */

case class KafkaCommandActorConfig(longRunningPoolConfig: LongRunningPoolConfig,
                                   askTimeoutMillis: Long = 400,
                                   clusterContext: ClusterContext,
                                   adminUtils: AdminUtils)

class KafkaCommandActor(kafkaCommandActorConfig: KafkaCommandActorConfig) extends BaseClusterCommandActor with LongRunningPoolActor {

  protected implicit val clusterContext: ClusterContext = kafkaCommandActorConfig.clusterContext

  private[this] var adminClientOption: Option[AdminClient] = None

  private[this] val reassignPartitionCommand = new ReassignPartitionCommand(kafkaCommandActorConfig.adminUtils)

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    log.info("Started actor %s".format(self.path))
    Try {
      adminClientOption = Some(createAdminClient())
    }.recover { case e => log.error(e, "Failed to create AdminClient in KafkaCommandActor") }
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    Try(adminClientOption.foreach(_.close()))
    super.postStop()
  }

  override protected def longRunningPoolConfig: LongRunningPoolConfig = kafkaCommandActorConfig.longRunningPoolConfig

  override protected def longRunningQueueFull(): Unit = {
    sender ! KCCommandResult(Try(throw new UnsupportedOperationException("Long running executor blocking queue is full!")))
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("kca : processActorResponse : Received unknown message: {}", any)
    }
  }

  private[this] def createAdminClient(): AdminClient = {
    val cfg = kafkaCommandActorConfig.clusterContext.config
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, cfg.securityProtocol.stringId)
    cfg.saslMechanism.foreach(m => props.put(SaslConfigs.SASL_MECHANISM, m.stringId))
    cfg.jaasConfig.foreach(j => props.put(SaslConfigs.SASL_JAAS_CONFIG, j))
    AdminClient.create(props)
  }

  private[this] def withAdminClient[T](fn: AdminClient => T): Try[T] = {
    adminClientOption match {
      case Some(client) => Try(fn(client))
      case None =>
        Try {
          adminClientOption = Some(createAdminClient())
          fn(adminClientOption.get)
        }
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    implicit val ec = longRunningExecutionContext
    request match {
      case KCDeleteTopic(topic) =>
        featureGateFold(KMDeleteTopicFeature)(
        {
          val result : KCCommandResult = KCCommandResult(Failure(new UnsupportedOperationException(
            s"Delete topic not supported for kafka version ${kafkaCommandActorConfig.clusterContext.config.version}")))
          sender ! result
        },
        {
          longRunning {
            Future {
              KCCommandResult(withAdminClient { client =>
                log.info(s"Deleting topic : $topic")
                client.deleteTopics(java.util.Collections.singletonList(topic)).all().get()
              })
            }
          }
        })

      case KCCreateTopic(topic, brokers, partitions, replicationFactor, config) =>
        longRunning {
          Future {
            KCCommandResult(withAdminClient { client =>
              val newTopic = new NewTopic(topic, partitions, replicationFactor.toShort)
              val topicConfig: java.util.Map[String, String] = config.asScala
                .map { case (k, v) => k.toString -> v.toString }.toMap.asJava
              newTopic.configs(topicConfig)
              try {
                client.createTopics(java.util.Collections.singletonList(newTopic)).all().get()
              } catch {
                case _: TopicExistsException => // idempotent
              }
            })
          }
        }

      case KCAddTopicPartitions(topic, brokers, partitions, partitionReplicaList, readVersion) =>
        longRunning {
          Future {
            KCCommandResult(withAdminClient { client =>
              val assignments: java.util.Map[String, NewPartitions] =
                Map(topic -> NewPartitions.increaseTo(partitions)).asJava
              client.createPartitions(assignments).all().get()
            })
          }
        }

      case KCAddMultipleTopicsPartitions(topicsAndReplicas, brokers, partitions, readVersion) =>
        longRunning {
          Future {
            KCCommandResult(withAdminClient { client =>
              val assignments: java.util.Map[String, NewPartitions] =
                topicsAndReplicas.map { case (topic, _) => topic -> NewPartitions.increaseTo(partitions) }.toMap.asJava
              client.createPartitions(assignments).all().get()
            })
          }
        }

      case KCUpdateBrokerConfig(broker, config, readVersion) =>
        longRunning {
          Future {
            KCCommandResult(withAdminClient { client =>
              val resource = new ConfigResource(ConfigResource.Type.BROKER, broker.toString)
              val alterations: java.util.Collection[AlterConfigOp] = config.asScala.map { case (k, v) =>
                new AlterConfigOp(new ConfigEntry(k.toString, v.toString), AlterConfigOp.OpType.SET)
              }.toList.asJava
              val alterMap = new java.util.HashMap[ConfigResource, java.util.Collection[AlterConfigOp]]()
              alterMap.put(resource, alterations)
              client.incrementalAlterConfigs(alterMap).all().get()
            })
          }
        }

      case KCUpdateTopicConfig(topic, config, readVersion) =>
        longRunning {
          Future {
            KCCommandResult(withAdminClient { client =>
              val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
              val alterations: java.util.Collection[AlterConfigOp] = config.asScala.map { case (k, v) =>
                new AlterConfigOp(new ConfigEntry(k.toString, v.toString), AlterConfigOp.OpType.SET)
              }.toList.asJava
              val alterMap = new java.util.HashMap[ConfigResource, java.util.Collection[AlterConfigOp]]()
              alterMap.put(resource, alterations)
              client.incrementalAlterConfigs(alterMap).all().get()
            })
          }
        }

      case KCPreferredReplicaLeaderElection(topicAndPartition) =>
        longRunning {
          log.info("Running replica leader election via Admin API: {}", topicAndPartition)
          Future {
            KCCommandResult(withAdminClient { client =>
              val tps: java.util.Set[TopicPartition] = topicAndPartition.asJava
              client.electLeaders(ElectionType.PREFERRED, tps).all().get()
            })
          }
        }

      case KCReassignPartition(current, generated, forceSet) =>
        longRunning {
          log.info("Running reassign partition from {} to {}", current, generated)
          Future {
            KCCommandResult(withAdminClient { client =>
              reassignPartitionCommand.getValidAssignments(current, generated, forceSet).map { validAssignments =>
                val reassignments: java.util.Map[TopicPartition, java.util.Optional[NewPartitionReassignment]] =
                  validAssignments.map { case (tp, replicas) =>
                    tp -> java.util.Optional.of(new NewPartitionReassignment(replicas.map(Integer.valueOf).asJava))
                  }.asJava
                client.alterPartitionReassignments(reassignments).all().get()
              }.get
            })
          }
        }

      case any: Any => log.warning("kca : processCommandRequest : Received unknown message: {}", any)
    }
  }
}
