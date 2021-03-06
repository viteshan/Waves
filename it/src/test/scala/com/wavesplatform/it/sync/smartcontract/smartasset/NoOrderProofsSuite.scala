package com.wavesplatform.it.sync.smartcontract.smartasset

import java.util.concurrent.TimeoutException

import com.wavesplatform.account.AddressScheme
import com.wavesplatform.it.api.SyncHttpApi._
import com.wavesplatform.it.sync.{someAssetAmount, _}
import com.wavesplatform.it.transactions.BaseTransactionSuite
import com.wavesplatform.state.ByteStr
import com.wavesplatform.transaction.Proofs
import com.wavesplatform.transaction.assets.BurnTransactionV2
import com.wavesplatform.transaction.smart.script.ScriptCompiler
import com.wavesplatform.transaction.transfer.TransferTransactionV2
import play.api.libs.json.JsNumber
import scala.concurrent.duration._

class NoOrderProofsSuite extends BaseTransactionSuite {
  test("try to use Order in asset scripts") {
    try sender
      .issue(
        firstAddress,
        "assetWProofs",
        "Test coin for assetWProofs test",
        someAssetAmount,
        0,
        reissuable = true,
        issueFee,
        2,
        script = Some(
          ScriptCompiler(
            s"""
            match tx {
            case o: Order => true
            case _ => false
            }""".stripMargin,
            true
          ).explicitGet()._1.bytes.value.base64)
      )
    catch {
      case ex: java.lang.Exception => assert(ex.getMessage.contains("Compilation failed: Matching not exhaustive"))
      case _ => throw new Exception("ScriptCompiler works incorrect for orders with smart assets")
    }

  }

  test("try to use proofs in assets script") {
    val errProofMsg = "Reason: Script doesn't exist and proof doesn't validate as signature"
    val assetWProofs = sender
      .issue(
        firstAddress,
        "assetWProofs",
        "Test coin for assetWProofs test",
        someAssetAmount,
        0,
        reissuable = true,
        issueFee,
        2,
        script = Some(
          ScriptCompiler(
            s"""
                let proof = base58'assetWProofs'
                match tx {
                  case tx: SetAssetScriptTransaction | TransferTransaction | ReissueTransaction | BurnTransaction => tx.proofs[0] == proof
                  case _ => false
                }""".stripMargin,
            false
          ).explicitGet()._1.bytes.value.base64),
        waitForTx = true
      )
      .id

    val incorrectTrTx = TransferTransactionV2
      .create(
        2,
        Some(ByteStr.decodeBase58(assetWProofs).get),
        pkByAddress(firstAddress),
        pkByAddress(thirdAddress),
        1,
        System.currentTimeMillis + 10.minutes.toMillis,
        None,
        smartMinFee,
        Array.emptyByteArray,
        Proofs(Seq(ByteStr("assetWProofs".getBytes())))
      )
      .right
      .get

    assertBadRequestAndMessage(
      sender.signedBroadcast(incorrectTrTx.json() + ("type" -> JsNumber(TransferTransactionV2.typeId.toInt))),
      errProofMsg
    )

    val incorrectBrTx = BurnTransactionV2
      .create(
        2,
        AddressScheme.current.chainId,
        pkByAddress(firstAddress),
        ByteStr.decodeBase58(assetWProofs).get,
        1,
        smartMinFee,
        System.currentTimeMillis + 10.minutes.toMillis,
        Proofs(Seq(ByteStr("assetWProofs".getBytes())))
      )
      .right
      .get

    assertBadRequestAndMessage(
      sender.signedBroadcast(incorrectBrTx.json() + ("type" -> JsNumber(BurnTransactionV2.typeId.toInt))),
      errProofMsg
    )
  }

}
