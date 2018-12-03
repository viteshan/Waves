package com.wavesplatform.matcher

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.pattern.gracefulStop
import akka.stream.ActorMaterializer
import com.wavesplatform.account.{Address, PrivateKeyAccount}
import com.wavesplatform.api.http.CompositeHttpService
import com.wavesplatform.db._
import com.wavesplatform.matcher.api.{MatcherApiRoute, MatcherResponse, OrderBookSnapshotHttpCache}
import com.wavesplatform.matcher.market.OrderBookActor.MarketStatus
import com.wavesplatform.matcher.market.{MatcherActor, MatcherTransactionWriter, OrderHistoryActor}
import com.wavesplatform.matcher.model.{ExchangeTransactionCreator, OrderBook, OrderValidator}
import com.wavesplatform.settings.WavesSettings
import com.wavesplatform.state.{Blockchain, EitherExt2}
import com.wavesplatform.transaction.assets.exchange.AssetPair
import com.wavesplatform.utils.{NTP, ScorexLogging}
import com.wavesplatform.utx.UtxPool
import com.wavesplatform.wallet.Wallet
import io.netty.channel.group.ChannelGroup

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

class Matcher(actorSystem: ActorSystem,
              utx: UtxPool,
              allChannels: ChannelGroup,
              blockchain: Blockchain,
              settings: WavesSettings,
              matcherPrivateKey: PrivateKeyAccount,
              isDuringShutdown: () => Boolean)
    extends ScorexLogging {

  import settings._

  private val pairBuilder        = new AssetPairBuilder(settings.matcherSettings, blockchain)
  private val orderBookCache     = new ConcurrentHashMap[AssetPair, OrderBook](1000, 0.9f, 10)
  private val transactionCreator = new ExchangeTransactionCreator(blockchain, matcherPrivateKey, matcherSettings, NTP)
  private val orderValidator = new OrderValidator(
    db,
    blockchain,
    transactionCreator,
    utx.portfolio,
    pairBuilder.validateAssetPair,
    settings.matcherSettings,
    matcherPrivateKey,
    NTP
  )

  private val orderBooks = new AtomicReference(Map.empty[AssetPair, Either[Unit, ActorRef]])
  private val orderBooksSnapshotCache = new OrderBookSnapshotHttpCache(
    matcherSettings.orderBookSnapshotHttpCache,
    p => Option(orderBookCache.get(p))
  )

  private val marketStatuses = new ConcurrentHashMap[AssetPair, MarketStatus](1000, 0.9f, 10)

  private def updateOrderBookCache(assetPair: AssetPair)(newSnapshot: OrderBook): Unit = {
    orderBookCache.put(assetPair, newSnapshot)
    orderBooksSnapshotCache.invalidate(assetPair)
  }

  private val commandBus = new CommandBus.Local

  lazy val matcherApiRoutes = Seq(
    MatcherApiRoute(
      pairBuilder,
      orderValidator,
      matcher,
      orderHistory,
      p => Option(orderBooks.get()).flatMap(_.get(p)),
      p => Option(marketStatuses.get(p)),
      commandBus.sendRequest(matcher),
      orderBooksSnapshotCache,
      settings,
      isDuringShutdown,
      db,
      NTP
    )
  )

  lazy val matcherApiTypes: Set[Class[_]] = Set(
    classOf[MatcherApiRoute]
  )

  lazy val matcher: ActorRef = actorSystem.actorOf(
    MatcherActor.props(
      pairBuilder.validateAssetPair,
      orderBooks,
      updateOrderBookCache,
      p => ms => marketStatuses.put(p, ms),
      utx,
      allChannels,
      matcherSettings,
      blockchain.assetDescription,
      transactionCreator.createTransaction,
      commandBus.resolveRequest
    ),
    MatcherActor.name
  )

  private lazy val db = openDB(matcherSettings.dataDir)

  private lazy val orderHistory: ActorRef = actorSystem.actorOf(OrderHistoryActor.props(db, matcherSettings), OrderHistoryActor.name)

  @volatile var matcherServerBinding: ServerBinding = _

  def shutdown(): Unit = {
    log.info("Shutting down matcher")
    Await.result(matcherServerBinding.unbind(), 10.seconds)
    val stopMatcherTimeout = 5.minutes
    orderBooksSnapshotCache.close()
    Await.result(gracefulStop(matcher, stopMatcherTimeout, MatcherActor.Shutdown), stopMatcherTimeout)
    Await.result(gracefulStop(orderHistory, stopMatcherTimeout), stopMatcherTimeout)
    log.debug("Matcher's actor system has been shut down")
    db.close()
    log.debug("Matcher's database closed")
    log.info("Matcher shutdown successful")
  }

  private def checkDirectory(directory: File): Unit = if (!directory.exists()) {
    log.error(s"Failed to create directory '${directory.getPath}'")
    sys.exit(1)
  }

  def runMatcher(): Unit = {
    val journalDir  = new File(matcherSettings.journalDataDir)
    val snapshotDir = new File(matcherSettings.snapshotsDataDir)
    journalDir.mkdirs()
    snapshotDir.mkdirs()

    checkDirectory(journalDir)
    checkDirectory(snapshotDir)

    log.info(s"Starting matcher on: ${matcherSettings.bindAddress}:${matcherSettings.port} ...")

    implicit val as: ActorSystem                 = actorSystem
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val combinedRoute = CompositeHttpService(actorSystem, matcherApiTypes, matcherApiRoutes, restAPISettings).compositeRoute
    matcherServerBinding = Await.result(Http().bindAndHandle(combinedRoute, matcherSettings.bindAddress, matcherSettings.port), 5.seconds)

    log.info(s"Matcher bound to ${matcherServerBinding.localAddress}")

    actorSystem.actorOf(MatcherTransactionWriter.props(db, matcherSettings), MatcherTransactionWriter.name)
  }
}

object Matcher extends ScorexLogging {
  type RequestId       = Long
  type RequestSender   = Any => Future[MatcherResponse]
  type RequestResolver = (RequestId, MatcherResponse) => Unit

  def apply(actorSystem: ActorSystem,
            wallet: Wallet,
            utx: UtxPool,
            allChannels: ChannelGroup,
            blockchain: Blockchain,
            settings: WavesSettings,
            isDuringShutdown: () => Boolean): Option[Matcher] =
    try {
      val privateKey = (for {
        address <- Address.fromString(settings.matcherSettings.account)
        pk      <- wallet.privateKeyAccount(address)
      } yield pk).explicitGet()

      val matcher = new Matcher(actorSystem, utx, allChannels, blockchain, settings, privateKey, isDuringShutdown)
      matcher.runMatcher()
      Some(matcher)
    } catch {
      case NonFatal(e) =>
        log.warn("Error starting matcher", e)
        None
    }
}
