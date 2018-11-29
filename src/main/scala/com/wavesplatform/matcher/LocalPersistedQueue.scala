package com.wavesplatform.matcher

import java.util.concurrent.atomic.AtomicLong

import com.wavesplatform.database.{DBExt, ReadOnlyDB}
import com.wavesplatform.matcher.MatcherKeys._
import com.wavesplatform.matcher.market.MatcherActor.Request
import com.wavesplatform.matcher.market.OrderBookActor.{CancelOrder, DeleteOrderBookRequest}
import com.wavesplatform.transaction.assets.exchange.Order
import org.iq80.leveldb.{DB, ReadOptions}

class LocalPersistedQueue(db: DB) {

  private val newestIdx = new AtomicLong(db.get(lpqNewestIdx))

  def enqueue(data: Any): Long = {
    val idx = newestIdx.incrementAndGet()
    // todo write inner data, not request, coz we have idx
    val request = data match {
      case x: Order                  => Request.Place(idx, x)
      case x: CancelOrder            => Request.Cancel(idx, x)
      case x: DeleteOrderBookRequest => Request.DeleteOrderBook(idx, x.assetPair)
    }

    val requestKey = lpqElement(idx)

    db.readWrite { rw =>
      rw.put(requestKey, Some(request))
      rw.put(lpqNewestIdx, idx)
    }

    idx
  }

  def getFrom(offset: Long): Vector[Request] =
    new ReadOnlyDB(db, new ReadOptions())
      .read(LpqElementKeyName, LpqElementPrefixBytes, lpqElement(math.max(offset, 0)).keyBytes, Int.MaxValue) { e =>
        Request.fromBytes(e.getValue)
      }

}
