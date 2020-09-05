/*
 * Copyright (c) 2020 Luis Rodero-Merino
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at.
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package catsEffectTutorial.producerconsumer.exerciseconcurrentqueue

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Sync}
import cats.implicits._

import scala.collection.mutable

/**
 * Concurrent queue exercise for cats-effect tutorial at
 * https://typelevel.org/cats-effect/tutorial/tutorial.html
 *
 * This `trait` defines the common methods for both a bounded an unbounded queue.
 * Implementations are given by `ConcurrentQueue.bounded` and `ConcurrentQueue.unbounded`
 * functions in companion object.
 *
 * This code is not intended to be used in production environments with high-performance
 * requirements. Also, the implementations provided have not been thoroughly tested.
 * For a professional-grade concurrent queue implementation which is also compatible
 * with cats-effect it is strongly suggested to take a look to Monix's `ConcurrentQueue`:
 * https://monix.io/api/current/monix/catnap/ConcurrentQueue.html
 *
 * */
trait ConcurrentQueue[F[_], A] {
  /** Get and remove first element from queue, blocks if queue empty. */
  def poll: F[A]
  /** Get and remove first `n` elements from queue, blocks if less than `n` items are available in queue. Error raised if n < 0. */
  def pollN(n: Int): F[List[A]]
  /** Get, but not remove, first element in queue, blocks if queue empty. */
  def peek: F[A]
  /** Get, but not remove, first `n` elements in queue, blocks if less than `n` items are available in queue. Error raised if n < 0. */
  def peekN(n: Int): F[List[A]]
  /** Put element at then end of queue, blocks if queue is bounded and full. */
  def put(a: A): F[Unit]
  /** Puts elements at the end of the queue, blocks if queue is bounded and does not have spare size for all items. */
  def putN(as: List[A]): F[Unit]
  /** Try to get and remove first element from queue, immediately returning `F[None]` if queue empty. Non-blocking. */
  def tryPoll: F[Option[A]]
  /** Try to get and remove first `n` elements from queue, immediately returning `F[None]` if less than `n` items are available in queue. Non-blocking. Error raised if n < 0. */
  def tryPollN(n: Int): F[Option[List[A]]]
  /** Try to get, but not remove, first element from queue, immediately returning `F[None]` if queue empty. Non-blocking. */
  def tryPeek: F[Option[A]]
  /** Try to get, but not remove, first  `n` elements from queue, immediately returning `F[None]` if less than `n` items are available in queue. Non-blocking. Error raised if n < 0. */
  def tryPeekN(n: Int): F[Option[List[A]]]
  /** Try to put element at the end of queue, immediately returning `F[false]` if queue is bounded and full. Non-blocking. */
  def tryPut(a: A): F[Boolean]
  /** Try to put elements in list at the end of queue, immediately returning `F[false]` if queue is bounded and does not have spare size for all items. Non-blocking. */
  def tryPutN(as: List[A]): F[Boolean]
  /** Returns # of items in queue. Non-blocking. */
  def size: F[Long]
  /** Returns `F[true]` if queue empty, `F[false]` otherwise. Non-blocking. */
  def isEmpty: F[Boolean]
}

/**
 * Specific methods for bounded concurrent queues.
 */
trait BoundedConcurrentQueue[F[_], A] extends ConcurrentQueue [F, A] {
  /** Remaining empty buckets. Non-blocking.*/
  def emptyBuckets: F[Long]
  /** Returns `F[true]` if queue full, `F[false]` otherwise. Non-blocking. */
  def isFull: F[Boolean]
}

object ConcurrentQueue {

  protected def assertNonNegative[F[_]](n: Int)(implicit F: Sync[F]): F[Unit] =
    if(n < 0) F.raiseError(new IllegalArgumentException(s"Argument must be >= 0 but it is $n")) else F.unit

  /**
   * Implementation of methods that are common for both bounded and unbounded
   * concurrent queues.
   */
  private abstract class AbstractConcurrentQueue[F[_]: Sync, A] extends ConcurrentQueue[F, A] {

    override def poll: F[A] =
      pollN(1).map(_.head)

    override def peek: F[A] =
      peekN(1).map(_.head)

    override def put(a: A): F[Unit] =
      putN(a :: Nil)

    override def tryPoll: F[Option[A]] =
      tryPollN(1).map(_.map(_.head))

    override def tryPeek: F[Option[A]] =
      tryPeekN(1).map(_.map(_.head))

    override def tryPut(a: A): F[Boolean] =
      tryPutN(a :: Nil)

    override def isEmpty: F[Boolean] =
      size.map(_ == 0)

  }

  /**
   * Create a bounded queue.
   */
  def bounded[F[_]: Concurrent: Sync, A](size: Int): F[BoundedConcurrentQueue[F, A]] =
    for {
      lock   <- Semaphore[F](1)
      filled <- Semaphore[F](0)
      empty  <- Semaphore[F](size)
    } yield buildBounded(new mutable.ListBuffer[A], lock, filled, empty)

  private def buildBounded[F[_], A](buffer: mutable.ListBuffer[A], lock: Semaphore[F], filled: Semaphore[F], empty: Semaphore[F])(implicit F: Sync[F]): BoundedConcurrentQueue[F, A] =
    new AbstractConcurrentQueue[F, A] with BoundedConcurrentQueue[F, A] {

      override def pollN(n: Int): F[List[A]] =
        if(n == 0) F.pure(List.empty[A])
        else
          for {
            _ <- assertNonNegative(n)
            _ <- filled.acquireN(n)
            as <- lock.withPermit {
              F.delay {
                val as = buffer.take(n)
                buffer.trimStart(n)
                as
              }
            }
            _ <- empty.releaseN(n)
          } yield as.toList

      override def peekN(n: Int): F[List[A]] =
        if(n == 0) F.pure(List.empty[A])
        else
          for {
            _ <- assertNonNegative(n)
            _ <- filled.acquireN(n)
            as <- lock.withPermit {
              F.delay(buffer.take(n))
            }
            _ <- filled.releaseN(n)
          } yield as.toList

      override def putN(as: List[A]): F[Unit] =
        if(as.isEmpty) F.unit
        else
          for {
            _ <- empty.acquireN(as.size)
            _ <- lock.withPermit {
              F.delay(buffer.appendAll(as))
            }
            _ <- filled.releaseN(as.size)
          } yield ()

      override def tryPollN(n: Int): F[Option[List[A]]] =
        if(n == 0) F.pure(Option(List.empty[A]))
        else
          for {
            _ <- assertNonNegative(n)
            acquired <- filled.tryAcquireN(n)
            asO <- if(!acquired) F.pure(Option.empty[List[A]])
            else
              lock.withPermit {
                F.delay {
                  val as = buffer.take(n)
                  buffer.trimStart(n)
                  as
                }.map(Option(_))
              } >>= { asO =>
                empty.releaseN(n).as(asO)
              }
          } yield asO.map(_.toList)

      override def tryPeekN(n: Int): F[Option[List[A]]] =
        if(n == 0) F.pure(Option(List.empty[A]))
        else
          for {
            _ <- assertNonNegative(n)
            acquired <- filled.tryAcquireN(n)
            asO <- if(!acquired) F.pure(None)
            else
              lock.withPermit {
                F.delay(buffer.take(n)).map(Option(_))
              } >>= { asO =>
                filled.releaseN(n).as(asO)
              }
          } yield asO.map(_.toList)

      override def tryPutN(as: List[A]): F[Boolean] =
        if(as.isEmpty) F.pure(true)
        else
          for {
            acquired <- empty.tryAcquireN(as.size)
            _ <- if(!acquired) F.unit
            else
              lock.withPermit {
                F.delay(buffer.appendAll(as))
              } >> filled.releaseN(as.size)
          } yield acquired

      override def size: F[Long] =
        filled.available

      override def emptyBuckets: F[Long] =
        empty.available

      override def isFull: F[Boolean] =
        emptyBuckets.map(_ == 0)

    }

  /**
   * Create an unbounded queue.
   */
  def unbounded[F[_]: Concurrent: Sync, A]: F[ConcurrentQueue[F, A]] =
    for {
      lock   <- Semaphore[F](1)
      filled <- Semaphore[F](0)
    } yield buildUnbounded(new mutable.ListBuffer[A], lock, filled)

  private def buildUnbounded[F[_], A](buffer: mutable.ListBuffer[A], lock: Semaphore[F], filled: Semaphore[F])(implicit F: Sync[F]): ConcurrentQueue[F, A] =
    new AbstractConcurrentQueue[F, A] {

      override def pollN(n: Int): F[List[A]] =
        if(n == 0) F.pure(List.empty[A])
        else
          for {
            _ <- assertNonNegative(n)
            _ <- filled.acquireN(n)
            as <- lock.withPermit {
              F.delay {
                val as = buffer.take(n)
                buffer.trimStart(n)
                as
              }
            }
          } yield as.toList

      override def peekN(n: Int): F[List[A]] =
        if(n == 0) F.pure(List.empty[A])
        else
          for {
            _ <- assertNonNegative(n)
            _ <- filled.acquireN(n)
            as <- lock.withPermit {
              F.delay(buffer.take(n))
            }
          } yield as.toList

      override def putN(as: List[A]): F[Unit] =
        if(as.isEmpty) F.unit
        else
          for {
            _ <- lock.withPermit {
              F.delay(buffer.appendAll(as))
            }
            _ <- filled.releaseN(as.size)
          } yield ()

      override def tryPollN(n: Int): F[Option[List[A]]] =
        if(n == 0) F.pure(Option(List.empty[A]))
        else
          for {
            _ <- assertNonNegative(n)
            acquired <- filled.tryAcquireN(n)
            aO <-
              if(!acquired) F.pure(Option.empty[List[A]])
              else
                lock.withPermit {
                  F.delay {
                    val as = buffer.take(n)
                    buffer.trimStart(n)
                    as
                  }.map(Option(_))
                }
          } yield aO.map(_.toList)

      override def tryPeekN(n: Int): F[Option[List[A]]] =
        if(n == 0) F.pure(Option(List.empty[A]))
        else
          for {
            _ <- assertNonNegative(n)
            acquired <- filled.tryAcquireN(n)
            asO <-
              if(!acquired) F.pure(None)
              else
                lock.withPermit {
                  F.delay(buffer.take(n)).map(Option(_))
                } >>= { asO =>
                  filled.releaseN(n).as(asO)
                }
          } yield asO.map(_.toList)

      override def tryPutN(as: List[A]): F[Boolean] =
        putN(as).as(true)

      override def size: F[Long] =
        filled.available

    }

}

/**
 * Example of how to use a (bounded) concurrent queue.
 */
object ConcurrentQueueRunner extends IOApp {

  def process[F[_] : Sync](i: Int): F[Unit] =
    Sync[F].delay {
      if (i % 10000 == 0)
        println(s"Processed $i elements")
    }

  val counter = new AtomicInteger(0)
  def create[F[_] : Sync]: F[Int] =
    Sync[F].delay(counter.incrementAndGet())

  def producer[F[_]: Sync](cq: ConcurrentQueue[F, Int]): F[Unit] =
    (create >>= cq.put) >> producer(cq)

  def consumer[F[_]: Sync](cq: ConcurrentQueue[F, Int]): F[Unit] =
    (cq.poll >>= process[F]) >> consumer(cq)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      cq <- ConcurrentQueue.bounded[IO, Int](1000)
      producers <- List.range(1, 11).as(producer[IO](cq)).pure[IO] // 10 producers
      consumers <- List.range(1, 11).as(consumer[IO](cq)).pure[IO] // 10 consumers
      res <- (producers ++ consumers)
        .parSequence.as(ExitCode.Success) // Run producers and consumers in parallel until done (likely by user cancelling with CTRL-C)
        .handleErrorWith { t =>
          IO(println(s"Error caught: ${t.getMessage}")).as(ExitCode.Error)
        }
    } yield res
}

