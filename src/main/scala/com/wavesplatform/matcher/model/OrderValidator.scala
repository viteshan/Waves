package com.wavesplatform.matcher.model

import cats.implicits._
import com.wavesplatform.account.Address
import com.wavesplatform.features.BlockchainFeatures
import com.wavesplatform.features.FeatureProvider._
import com.wavesplatform.lang.v1.compiler.Terms.{EVALUATED, FALSE, TRUE}
import com.wavesplatform.matcher.model.Events.OrderExecuted
import com.wavesplatform.matcher.smart.MatcherScriptRunner
import com.wavesplatform.metrics.TimerExt
import com.wavesplatform.state._
import com.wavesplatform.transaction._
import com.wavesplatform.transaction.assets.exchange._
import com.wavesplatform.transaction.smart.Verifier
import com.wavesplatform.transaction.smart.script.ScriptRunner
import com.wavesplatform.matcher.api.DBUtils.indexes.active.MaxElements
import kamon.Kamon
import shapeless.Coproduct

import scala.util.control.NonFatal

object OrderValidator {

  type ValidationResult = Either[String, Order]

  private val timer = Kamon.timer("matcher.validation").refine("type" -> "blockchain")

  private def verifySignature(order: Order): ValidationResult =
    Verifier
      .verifyAsEllipticCurveSignature(order)
      .leftMap(_.toString)

  private def verifyOrderByAccountScript(blockchain: Blockchain, address: Address, order: Order) =
    blockchain.accountScript(address).fold(verifySignature(order)) { script =>
      if (!blockchain.isFeatureActivated(BlockchainFeatures.SmartAccountTrading, blockchain.height))
        Left("Trading on scripted account isn't allowed yet")
      else if (order.version <= 1) Left("Can't process order with signature from scripted account")
      else
        try MatcherScriptRunner[EVALUATED](script, order, isTokenScript = false) match {
          case (_, Left(execError)) => Left(s"Error executing script for $address: $execError")
          case (_, Right(FALSE))    => Left(s"Order rejected by script for $address")
          case (_, Right(TRUE))     => Right(order)
          case (_, Right(x))        => Left(s"Script returned not a boolean result, but $x")
        } catch {
          case NonFatal(e) => Left(s"Caught ${e.getClass.getCanonicalName} while executing script for $address: ${e.getMessage}")
        }
    }

  private def verifySmartToken(blockchain: Blockchain, assetId: AssetId, tx: ExchangeTransaction) =
    blockchain.assetScript(assetId).fold[Either[String, Unit]](Right(())) { script =>
      if (!blockchain.isFeatureActivated(BlockchainFeatures.SmartAssets, blockchain.height))
        Left("Trading of scripted asset isn't allowed yet")
      else {
        try ScriptRunner[EVALUATED](blockchain.height, Coproduct(tx), blockchain, script, proofsEnabled = false, orderEnabled = false) match {
          case (_, Left(execError)) => Left(s"Error executing script of asset $assetId: $execError")
          case (_, Right(FALSE))    => Left(s"Order rejected by script of asset $assetId")
          case (_, Right(TRUE))     => Right(())
          case (_, Right(x))        => Left(s"Script returned not a boolean result, but $x")
        } catch {
          case NonFatal(e) => Left(s"Caught ${e.getClass.getCanonicalName} while executing script of asset $assetId: ${e.getMessage}")
        }
      }.left.map(_.toString)
    }

  @inline private def decimals(blockchain: Blockchain, assetId: Option[AssetId]) = assetId.fold[Either[String, Int]](Right(8)) { aid =>
    blockchain.assetDescription(aid).map(_.decimals).toRight(s"Invalid asset id $aid")
  }

  private def validateDecimals(blockchain: Blockchain, o: Order): ValidationResult =
    for {
      pd <- decimals(blockchain, o.assetPair.priceAsset)
      ad <- decimals(blockchain, o.assetPair.amountAsset)
      insignificantDecimals = (pd - ad).max(0)
      _ <- Either.cond(o.price % BigDecimal(10).pow(insignificantDecimals).toLongExact == 0,
        o,
        s"Invalid price, last $insignificantDecimals digits must be 0")
    } yield o

  def blockchainAware(
      blockchain: Blockchain,
      orderMatchTxFee: Long,
      matcherAddress: Address,
      transactionCreator: ExchangeTransactionCreator,
  )(order: Order): ValidationResult =
    timer
      .measure {
        lazy val minOrderFee: Long =
          ExchangeTransactionCreator.minFee(blockchain, orderMatchTxFee, matcherAddress, Some(order.sender), None, order.assetPair)

        lazy val exchangeTx = {
          val fakeOrder: Order = order match {
            case x: OrderV1 => x.copy(orderType = x.orderType.opposite)
            case x: OrderV2 => x.copy(orderType = x.orderType.opposite)
          }
          transactionCreator.createTransaction(OrderExecuted(LimitOrder(fakeOrder), LimitOrder(order))).left.map(_.toString)
        }

        def verifyAssetScript(assetId: Option[AssetId]) = assetId.fold[ValidationResult](Right(order)) { assetId =>
          exchangeTx.flatMap(verifySmartToken(blockchain, assetId, _)).right.map(_ => order)
        }

        for {
          _ <- (Right(order): ValidationResult)
            .ensure(s"Order matcherFee should be >= $minOrderFee")(_.matcherFee >= minOrderFee)
          _ <- validateDecimals(blockchain, order)
          _ <- verifyOrderByAccountScript(blockchain, order.sender, order)
          _ <- verifyAssetScript(order.assetPair.amountAsset)
          _ <- verifyAssetScript(order.assetPair.priceAsset)
        } yield order
      }

  def accountStateAware(
      sender: Address,
      tradableBalance: Option[AssetId] => Long,
      activeOrderCount: => Int,
      latestOrderTimestamp: => Long
  )(order: Order): ValidationResult = {
    Either.cond()

    for {
      _ <- (Right(order): ValidationResult)
        .ensure(s"Limit of $MaxElements active orders has been reached")(_ => activeOrderCount < MaxElements)
      _ <- validateBalance(order, openVolume)
    } yield order
  }
}
