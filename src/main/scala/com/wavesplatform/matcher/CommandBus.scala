package com.wavesplatform.matcher
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorRef
import com.wavesplatform.matcher.Matcher.RequestId
import com.wavesplatform.matcher.api.MatcherResponse

import scala.concurrent.{Future, Promise}

class CommandBus {
  private val requests = new ConcurrentHashMap[RequestId, Promise[MatcherResponse]]()

  def sendRequest(matcher: ActorRef)(payload: Any): Future[MatcherResponse] = {
    val request    = Matcher.wrap(payload)
    val newPromise = Promise[MatcherResponse]()
    val p          = Option(requests.putIfAbsent(request.id, newPromise)).getOrElse(newPromise)

    // ??? + cancellable in map
    //    import actorSystem.dispatcher
    //    actorSystem.scheduler.scheduleOnce(60.seconds) {
    //      Option(requests.remove(id)).foreach(_.trySuccess(OperationTimedOut))
    //    }

    matcher ! request
    p.future
  }

  def resolveRequest(id: RequestId, response: MatcherResponse): Unit = Option(requests.remove(id)).foreach(_.trySuccess(response))
}
