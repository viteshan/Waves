package com.wavesplatform.matcher.market
import java.io.File
import java.nio.file.Files

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestKitBase
import com.typesafe.config.ConfigFactory
import com.wavesplatform.TestHelpers.deleteRecursively
import com.wavesplatform.matcher.Matcher.RequestResolver
import com.wavesplatform.settings.loadConfig
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

abstract class MatcherSpec(actorSystemName: String) extends TestKitBase with WordSpecLike with Matchers with BeforeAndAfterAll {
  import MatcherSpec._
  implicit lazy val system: ActorSystem = ActorSystem(
    actorSystemName,
    loadConfig(ConfigFactory.parseString(s"$SnapshotStorePath = ${Files.createTempDirectory(actorSystemName)}"))
  )
  override protected def afterAll(): Unit = {
    super.afterAll()
    shutdown(system)
    deleteRecursively(new File(system.settings.config.getString(SnapshotStorePath)).toPath)
  }

  protected def akkaRequestResolver(matcherActor: ActorRef): RequestResolver = (_, response) => testActor.tell(response, matcherActor)
}

object MatcherSpec {
  private[MatcherSpec] val SnapshotStorePath = "akka.persistence.snapshot-store.local.dir"
}
