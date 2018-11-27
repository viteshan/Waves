package com.wavesplatform.matcher

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorRef
import com.wavesplatform.matcher.Matcher.RequestId
import com.wavesplatform.matcher.api.MatcherResponse
import com.wavesplatform.matcher.market.MatcherActor.Request

import scala.concurrent.{Future, Promise}

trait CommandBus {
  def sendRequest(matcher: ActorRef)(payload: Any): Future[MatcherResponse]
  def resolveRequest(id: RequestId, response: MatcherResponse): Unit
}

object CommandBus {
  class Local extends CommandBus {
    private val requests = new ConcurrentHashMap[RequestId, Promise[MatcherResponse]]()
    private val seqNr    = new AtomicLong(0)

    override def sendRequest(matcher: ActorRef)(payload: Any): Future[MatcherResponse] = {
      val request    = Request(seqNr.getAndIncrement(), payload)
      val newPromise = Promise[MatcherResponse]()
      val p          = Option(requests.putIfAbsent(request.seqNr, newPromise)).getOrElse(newPromise)

      // ??? + cancellable in map
      //    import actorSystem.dispatcher
      //    actorSystem.scheduler.scheduleOnce(60.seconds) {
      //      Option(requests.remove(id)).foreach(_.trySuccess(OperationTimedOut))
      //    }

      matcher ! request
      p.future
    }

    override def resolveRequest(id: RequestId, response: MatcherResponse): Unit = Option(requests.remove(id)).foreach(_.trySuccess(response))
  }
}
