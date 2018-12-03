package com.wavesplatform.matcher.market

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Terminated}
import akka.persistence.{PersistentActor, RecoveryCompleted, _}
import com.google.common.base.Charsets
import com.google.common.primitives.Longs
import com.wavesplatform.crypto.DigestSize
import com.wavesplatform.matcher.Matcher.{RequestNr, RequestResolver}
import com.wavesplatform.matcher.MatcherSettings
import com.wavesplatform.matcher.api.{DuringShutdown, OrderBookUnavailable}
import com.wavesplatform.matcher.market.OrderBookActor._
import com.wavesplatform.matcher.model.Events.OrderExecuted
import com.wavesplatform.matcher.model.OrderBook
import com.wavesplatform.state.{AssetDescription, ByteStr}
import com.wavesplatform.transaction.assets.exchange.{AssetPair, ExchangeTransaction, Order}
import com.wavesplatform.transaction.{AssetId, ValidationError}
import com.wavesplatform.utils.ScorexLogging
import com.wavesplatform.utx.UtxPool
import io.netty.channel.group.ChannelGroup
import play.api.libs.json._
import scorex.utils._

import scala.collection.mutable

// TODO: should send snapshot requests to OBA
class MatcherActor(recoveryCompletedWithCommandNr: Long => Unit,
                   orderBooks: AtomicReference[Map[AssetPair, Either[Unit, ActorRef]]],
                   orderBookActorProps: (AssetPair, ActorRef) => Props,
                   assetDescription: ByteStr => Option[AssetDescription])
    extends PersistentActor
    with ScorexLogging {

  import MatcherActor._

  private var tradedPairs            = Map.empty[AssetPair, MarketData]
  private var childrenNames          = Map.empty[ActorRef, AssetPair]
  private var lastSnapshotSequenceNr = 0L

  private val oldestCommandNrOffset = 1000L
  private val oldestSnapshot        = mutable.PriorityQueue.empty[(AssetPair, Long)](Ordering.by[(AssetPair, Long), Long](_._2).reverse)

  private var shutdownStatus: ShutdownStatus = ShutdownStatus(
    initiated = false,
    orderBooksStopped = false,
    oldMessagesDeleted = false,
    oldSnapshotsDeleted = false,
    onComplete = () => ()
  )

  private def orderBook(pair: AssetPair) = Option(orderBooks.get()).flatMap(_.get(pair))

  private def getAssetName(asset: Option[AssetId], desc: Option[AssetDescription]): String =
    asset.fold(AssetPair.WavesName) { _ =>
      desc.fold("Unknown")(d => new String(d.name, Charsets.UTF_8))
    }

  private def getAssetInfo(asset: Option[AssetId], desc: Option[AssetDescription]): Option[AssetInfo] =
    asset.fold(Option(8))(_ => desc.map(_.decimals)).map(AssetInfo)

  private def createMarketData(pair: AssetPair): MarketData = {
    val amountDesc = pair.amountAsset.flatMap(assetDescription)
    val priceDesc  = pair.priceAsset.flatMap(assetDescription)

    MarketData(
      pair,
      getAssetName(pair.amountAsset, amountDesc),
      getAssetName(pair.priceAsset, priceDesc),
      System.currentTimeMillis(),
      getAssetInfo(pair.amountAsset, amountDesc),
      getAssetInfo(pair.priceAsset, priceDesc)
    )
  }

  private def createOrderBookActor(pair: AssetPair): ActorRef = {
    val r = context.actorOf(orderBookActorProps(pair, self), OrderBookActor.name(pair))
    childrenNames += r -> pair
    context.watch(r)
    r
  }

  private def createOrderBook(pair: AssetPair): ActorRef = {
    log.info(s"Creating order book for $pair")
    val orderBook = createOrderBookActor(pair)
    orderBooks.updateAndGet(_ + (pair -> Right(orderBook)))
    tradedPairs += pair -> createMarketData(pair)
    orderBook
  }

  /**
    * @param f (sender, orderBook)
    */
  private def runFor(request: Request)(f: (ActorRef, ActorRef) => Unit): Unit = {
    val s = sender()
    if (shutdownStatus.initiated) s ! DuringShutdown
    else
      orderBook(request.assetPair) match {
        case Some(Right(ob)) =>
          oldestSnapshot.headOption.foreach {
            case (oldestPair, oldestSnapshotNr) =>
              if (request.assetPair == oldestPair && request.seqNr >= (oldestSnapshotNr + oldestCommandNrOffset)) ob ! SaveSnapshot
          }
          f(s, ob)
        case Some(Left(_)) => s ! OrderBookUnavailable
        case None =>
          val ob = createOrderBook(request.assetPair)
          persistAsync(OrderBookCreated(request.assetPair))(_ => f(s, ob))
      }
  }

  private def forwardToOrderBook: Receive = {
    case GetMarkets => sender() ! tradedPairs.values.toSeq

    case request: Request.DeleteOrderBook =>
      runFor(request) { (sender, ref) =>
        ref.tell(request, sender)
        orderBooks.getAndUpdate(_.filterNot { x =>
          x._2.right.exists(_ == ref)
        })

        tradedPairs -= request.assetPair
        deleteMessages(lastSequenceNr)
        saveSnapshot(Snapshot(tradedPairs.keySet))
      }

    case x: Request => runFor(x)((_, orderBook) => orderBook ! x)

    case Shutdown =>
      shutdownStatus = shutdownStatus.copy(
        initiated = true,
        onComplete = { () =>
          context.stop(self)
        }
      )

      context.children.foreach(context.unwatch)
      context.become(snapshotsCommands orElse shutdownFallback)

      if (lastSnapshotSequenceNr < lastSequenceNr) saveSnapshot(Snapshot(tradedPairs.keySet))
      else {
        log.debug(s"No changes, lastSnapshotSequenceNr = $lastSnapshotSequenceNr, lastSequenceNr = $lastSequenceNr")
        shutdownStatus = shutdownStatus.copy(
          oldMessagesDeleted = true,
          oldSnapshotsDeleted = true
        )
      }

      if (context.children.isEmpty) {
        shutdownStatus = shutdownStatus.copy(orderBooksStopped = true)
        shutdownStatus.tryComplete()
      } else {
        context.actorOf(Props(classOf[GracefulShutdownActor], context.children.toVector, self))
      }

    case Terminated(ref) =>
      orderBooks.getAndUpdate { m =>
        childrenNames.get(ref).fold(m)(m.updated(_, Left(())))
      }
  }

  override def receiveRecover: Receive = {
    case OrderBookCreated(pair) => if (orderBook(pair).isEmpty) createOrderBook(pair)

    case SnapshotOffer(metadata, snapshot: Snapshot) =>
      lastSnapshotSequenceNr = metadata.sequenceNr
      log.info(s"Loaded the snapshot with nr = ${metadata.sequenceNr}")
      snapshot.tradedPairsSet.foreach(createOrderBook)

    case RecoveryCompleted =>
      if (orderBooks.get().isEmpty) {
        log.info("Recovery completed!")
        recoveryCompletedWithCommandNr(-1)
      } else {
        log.info(s"Recovery completed, waiting order books to restore: ${orderBooks.get().keys.mkString(", ")}")
        context.become(collectOrderBooks(orderBooks.get().size, Long.MaxValue))
      }
  }

  private def collectOrderBooks(restOrderBooksNumber: Long, oldestCommandNr: Long): Receive = {
    case OrderBookRecovered(assetPair, lastProcessedCommandNr) =>
      val updatedRestOrderBooksNumber = restOrderBooksNumber - 1
      val updatedOldestCommandNr      = math.min(oldestCommandNr, lastProcessedCommandNr)
      oldestSnapshot.enqueue(assetPair -> lastProcessedCommandNr)

      if (updatedRestOrderBooksNumber > 0) context.become(collectOrderBooks(updatedRestOrderBooksNumber, updatedOldestCommandNr))
      else {
        context.become(receiveCommand)
        recoveryCompletedWithCommandNr(updatedOldestCommandNr)
        unstashAll()
      }

    case Terminated(ref) =>
      orderBooks.getAndUpdate { m =>
        childrenNames.get(ref).fold(m)(m.updated(_, Left(())))
      }

      val updatedRestOrderBooksNumber = restOrderBooksNumber - 1
      if (updatedRestOrderBooksNumber > 0) context.become(collectOrderBooks(updatedRestOrderBooksNumber, oldestCommandNr))
      else {
        context.become(receiveCommand)
        recoveryCompletedWithCommandNr(oldestCommandNr)
        unstashAll()
      }

    case _ => stash()
  }

  private def snapshotsCommands: Receive = {
    case SaveSnapshotSuccess(metadata) =>
      lastSnapshotSequenceNr = metadata.sequenceNr
      log.info(s"Snapshot saved with metadata $metadata")
      deleteMessages(metadata.sequenceNr - 1)
      deleteSnapshots(SnapshotSelectionCriteria.Latest.copy(maxSequenceNr = metadata.sequenceNr - 1))

    case SaveSnapshotFailure(metadata, reason) =>
      log.error(s"Failed to save snapshot: $metadata, $reason.")
      if (shutdownStatus.initiated) {
        shutdownStatus = shutdownStatus.copy(
          oldMessagesDeleted = true,
          oldSnapshotsDeleted = true
        )
        shutdownStatus.tryComplete()
      }

    case DeleteMessagesSuccess(nr) =>
      log.info(s"Old messages are deleted up to $nr")
      if (shutdownStatus.initiated) {
        shutdownStatus = shutdownStatus.copy(oldMessagesDeleted = true)
        shutdownStatus.tryComplete()
      }

    case DeleteMessagesFailure(cause, nr) =>
      log.info(s"Failed to delete messages up to $nr: $cause")
      if (shutdownStatus.initiated) {
        shutdownStatus = shutdownStatus.copy(oldMessagesDeleted = true)
        shutdownStatus.tryComplete()
      }

    case DeleteSnapshotsSuccess(nr) =>
      log.info(s"Old snapshots are deleted up to $nr")
      if (shutdownStatus.initiated) {
        shutdownStatus = shutdownStatus.copy(oldSnapshotsDeleted = true)
        shutdownStatus.tryComplete()
      }

    case DeleteSnapshotsFailure(cause, nr) =>
      log.info(s"Failed to delete old snapshots to $nr: $cause")
      if (shutdownStatus.initiated) {
        shutdownStatus = shutdownStatus.copy(oldSnapshotsDeleted = true)
        shutdownStatus.tryComplete()
      }
  }

  private def shutdownFallback: Receive = {
    case ShutdownComplete =>
      shutdownStatus = shutdownStatus.copy(orderBooksStopped = true)
      shutdownStatus.tryComplete()

    case _ if shutdownStatus.initiated => sender() ! DuringShutdown
  }

  override def receiveCommand: Receive = forwardToOrderBook orElse snapshotsCommands

  override def persistenceId: String = "matcher"
}

object MatcherActor {
  def name = "matcher"

  def props(recoveryCompletedWithCommandNr: Long => Unit,
            validateAssetPair: AssetPair => Either[String, AssetPair],
            orderBooks: AtomicReference[Map[AssetPair, Either[Unit, ActorRef]]],
            updateSnapshot: AssetPair => OrderBook => Unit,
            updateMarketStatus: AssetPair => MarketStatus => Unit,
            utx: UtxPool,
            allChannels: ChannelGroup,
            settings: MatcherSettings,
            assetDescription: ByteStr => Option[AssetDescription],
            createTransaction: OrderExecuted => Either[ValidationError, ExchangeTransaction],
            requestResolver: RequestResolver): Props =
    Props(
      new MatcherActor(
        recoveryCompletedWithCommandNr,
        orderBooks,
        (assetPair, matcher) =>
          OrderBookActor
            .props(
              matcher,
              assetPair,
              updateSnapshot(assetPair),
              updateMarketStatus(assetPair),
              allChannels,
              settings,
              requestResolver,
              createTransaction
          ),
        assetDescription
      ))

  private case class ShutdownStatus(initiated: Boolean,
                                    oldMessagesDeleted: Boolean,
                                    oldSnapshotsDeleted: Boolean,
                                    orderBooksStopped: Boolean,
                                    onComplete: () => Unit) {
    def completed: ShutdownStatus = copy(
      initiated = true,
      oldMessagesDeleted = true,
      oldSnapshotsDeleted = true,
      orderBooksStopped = true
    )
    def isCompleted: Boolean = initiated && oldMessagesDeleted && oldSnapshotsDeleted && orderBooksStopped
    def tryComplete(): Unit  = if (isCompleted) onComplete()
  }

  trait Request {
    def seqNr: RequestNr
    def assetPair: AssetPair
  }

  object Request {
    case class DeleteOrderBook(seqNr: RequestNr, assetPair: AssetPair) extends Request
    case class Place(seqNr: RequestNr, payload: Order) extends Request {
      override def assetPair: AssetPair = payload.assetPair
    }
    case class Cancel(seqNr: RequestNr, inner: CancelOrder) extends Request {
      override def assetPair: AssetPair = inner.assetPair
    }

    def toBytes(x: Request): Array[Byte] = Longs.toByteArray(x.seqNr) ++ {
      x match {
        case x: DeleteOrderBook => (1: Byte) +: x.assetPair.bytes
        case x: Place           => (2: Byte) +: x.payload.version +: x.payload.bytes()
        case x: Cancel          => (3: Byte) +: (x.inner.assetPair.bytes ++ x.inner.orderId.arr)
      }
    }
    def fromBytes(xs: Array[Byte]): Request = {
      val seqNr = Longs.fromByteArray(xs.take(8))
      xs.drop(8).head match {
        case 1 => DeleteOrderBook(seqNr, AssetPair.fromBytes(xs.drop(9)))
        case 2 => Place(seqNr, Order.fromBytes(xs.drop(9)))
        case 3 =>
          val assetPair = AssetPair.fromBytes(xs.drop(9))
          Cancel(seqNr, CancelOrder(assetPair, ByteStr(xs.takeRight(DigestSize))))
      }
    }
  }

  case object SaveSnapshot

  case class Snapshot(tradedPairsSet: Set[AssetPair])

  case class OrderBookCreated(pair: AssetPair)

  case object GetMarkets

  case class MatcherRecovered(oldestCommandNr: Long)

  case object Shutdown

  case object ShutdownComplete

  case class AssetInfo(decimals: Int)
  implicit val assetInfoFormat: Format[AssetInfo] = Json.format[AssetInfo]

  case class MarketData(pair: AssetPair,
                        amountAssetName: String,
                        priceAssetName: String,
                        created: Long,
                        amountAssetInfo: Option[AssetInfo],
                        priceAssetinfo: Option[AssetInfo])

  def compare(buffer1: Option[Array[Byte]], buffer2: Option[Array[Byte]]): Int = {
    if (buffer1.isEmpty && buffer2.isEmpty) 0
    else if (buffer1.isEmpty) -1
    else if (buffer2.isEmpty) 1
    else ByteArray.compare(buffer1.get, buffer2.get)
  }

  class GracefulShutdownActor(children: Vector[ActorRef], receiver: ActorRef) extends Actor {
    children.map(context.watch).foreach(_ ! PoisonPill)

    override def receive: Receive = state(children.size)

    private def state(expectedResponses: Int): Receive = {
      case _: Terminated =>
        if (expectedResponses > 1) context.become(state(expectedResponses - 1))
        else {
          receiver ! ShutdownComplete
          context.stop(self)
        }
    }
  }
}
