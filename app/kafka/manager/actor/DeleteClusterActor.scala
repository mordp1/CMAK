/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager.actor

import akka.actor.Cancellable
import kafka.manager.model.ActorModel
import ActorModel.{ActorResponse, CommandRequest, DCUpdateState}
import kafka.manager.base.BaseCommandActor

import scala.concurrent.duration._
import scala.util.Try

/**
 * No-op delete cluster actor — cluster deletion is now handled inline in KafkaManagerActor
 * using file-based storage. This actor is kept for API compatibility only.
 */
case class DeleteClusterActorConfig(updatePeriod: FiniteDuration = 10 seconds,
                                    deletionBatchSize: Int = 2)

class DeleteClusterActor(config: DeleteClusterActorConfig) extends BaseCommandActor {
  private[this] var cancellable : Option[Cancellable] = None

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    log.info("Started actor %s".format(self.path))
    cancellable = Some(
      context.system.scheduler.schedule(0 seconds, config.updatePeriod, self, DCUpdateState)(context.system.dispatcher, self)
    )
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Stopped actor %s".format(self.path))
    Try(cancellable.foreach(_.cancel()))
    super.postStop()
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case DCUpdateState => // no-op: deletion is handled in KafkaManagerActor
      case any: Any => log.warning("dca : processCommandRequest : Received unknown message: {}", any)
    }
  }

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("dca : processActorResponse : Received unknown message: {}", any)
    }
  }
}
