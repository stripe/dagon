/*
 Copyright 2014 Twitter, Inc.
 Copyright 2017 Stripe, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.stripe.dagon

/**
 * This is a weak heterogenous map. It uses equals on the keys,
 * so it is your responsibilty that if k: K[_] == k2: K[_] then
 * the types are actually equal (either be careful or store a
 * type identifier).
 */
final class HMap[K[_], V[_]](protected val map: Map[K[_], V[_]]) {

  type Pair[t] = (K[t], V[t])

  override def toString: String =
    "H%s".format(map)

  override def equals(that: Any): Boolean =
    that match {
      case null => false
      case h: HMap[_, _] => map.equals(h.map)
      case _ => false
    }

  override def hashCode: Int =
    map.hashCode

  def updated[T](k: K[T], v: V[T]): HMap[K, V] =
    HMap.from[K, V](map.updated(k, v))

  def +[T](kv: (K[T], V[T])): HMap[K, V] =
    HMap.from[K, V](map + kv)

  def -(k: K[_]): HMap[K, V] =
    HMap.from[K, V](map - k)

  def apply[T](id: K[T]): V[T] =
    get(id).get

  def get[T](id: K[T]): Option[V[T]] =
    map.get(id).asInstanceOf[Option[V[T]]]

  def contains[T](id: K[T]): Boolean =
    get(id).isDefined

  def size: Int = map.size

  def exists(p: ((K[_], V[_])) => Boolean): Boolean =
    map.exists(p)

  def forall(p: ((K[_], V[_])) => Boolean): Boolean =
    map.forall(p)
  
  def filter(p: ((K[_], V[_])) => Boolean): HMap[K, V] =
    HMap.from[K, V](map.filter(p))

  def keysOf[T](v: V[T]): Set[K[T]] =
    map.collect {
      case (k, w) if v == w => k.asInstanceOf[K[T]]
    }.toSet

  def optionMap[R[_]](f: FunctionK[Pair, Lambda[x => Option[R[x]]]]): Stream[R[_]] =
    map.toStream.asInstanceOf[Stream[(K[Any], V[Any])]].flatMap(f(_))
}

object HMap {

  def empty[K[_], V[_]]: HMap[K, V] =
    from[K, V](Map.empty[K[_], V[_]])

  private def from[K[_], V[_]](m: Map[K[_], V[_]]): HMap[K, V] =
    new HMap[K, V](m)
}
