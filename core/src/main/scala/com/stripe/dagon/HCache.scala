package com.stripe.dagon

/**
 * This is a useful cache for memoizing heterogenously types functions
 */
class HCache[K[_], V[_]]() {
  private var hmap: HMap[K, V] = HMap.empty[K, V]

  /**
   * Get snapshot of the current state
   */
  def snapshot: HMap[K, V] = hmap

  def getOrElseUpdate[T](k: K[T], v: => V[T]): V[T] =
    hmap.get(k) match {
      case Some(exists) => exists
      case None =>
        val res = v
        hmap = hmap + (k -> res)
        res
    }
}
