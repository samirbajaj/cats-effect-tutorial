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

trait SimpleConcurrentQueue[F[_], A] {
  /** Get and remove first element from queue, blocks if queue emptyV. */
  def poll: F[A]
  /** Put element at then end of queue, blocks if queue is bounded and full. */
  def put(a: A): F[Unit]
}

object SimpleConcurrentQueue {

  def unbounded[F[_]: Concurrent: Sync, A]: F[SimpleConcurrentQueue[F, A]] =
    for {
      lock   <- Semaphore[F](1)
      filled <- Semaphore[F](0)
    } yield buildUnbounded(new mutable.ListBuffer[A], lock, filled)

  private def buildUnbounded[F[_], A](buffer: mutable.ListBuffer[A], lock: Semaphore[F], filled: Semaphore[F])(implicit F: Sync[F]): SimpleConcurrentQueue[F, A] =
    new SimpleConcurrentQueue[F, A] {
      override def poll: F[A] =
        for {
          _ <- filled.acquire
          a <- lock.withPermit(Sync[F].delay(buffer.remove(0)))
        } yield a

      override def put(a: A): F[Unit] =
        for {
          _ <- lock.withPermit(Sync[F].delay(buffer.append(a)))
          _ <- filled.release
        } yield ()
    }

  def bounded[F[_]: Concurrent: Sync, A](size: Int): F[SimpleConcurrentQueue[F, A]] =
    for {
      lock <- Semaphore[F](1)
      filled <- Semaphore[F](0)
      empty <- Semaphore[F](size)
    } yield buildBounded(new mutable.ListBuffer[A], lock, filled, empty)

  private def buildBounded[F[_], A](buffer: mutable.ListBuffer[A], lock: Semaphore[F], filled: Semaphore[F], empty: Semaphore[F])(implicit F: Sync[F]): SimpleConcurrentQueue[F, A] =
    new SimpleConcurrentQueue[F, A] {
      override def poll: F[A] =
        for {
          _ <- filled.acquire
          a <- lock.withPermit(Sync[F].delay(buffer.remove(0)))
          _ <- empty.release
        } yield a

      override def put(a: A): F[Unit] =
      for {
        _ <- empty.acquire
        _ <- lock.withPermit(Sync[F].delay(buffer.append(a)))
        _ <- filled.release
      } yield ()
    }
}

object SimpleConcurrentQueueRunner extends IOApp {

  def process[F[_] : Sync](i: Int): F[Unit] =
    Sync[F].delay {
      if (i % 10000 == 0)
        println(s"Processed $i elements")
    }

  val counter = new AtomicInteger(0)

  def create[F[_] : Sync]: F[Int] =
    Sync[F].delay(counter.incrementAndGet())

  def producer[F[_] : Sync](scq: SimpleConcurrentQueue[F, Int]): F[Unit] =
    (create >>= scq.put) >> producer(scq)

  def consumer[F[_] : Sync](scq: SimpleConcurrentQueue[F, Int]): F[Unit] =
    (scq.poll >>= process[F]) >> consumer(scq)

  override def run(args: List[String]): IO[ExitCode] = {
    for {
      scq <- SimpleConcurrentQueue.bounded[IO, Int](100)
      producers <- List.range(1, 11).as(producer[IO](scq)).pure[IO] // 10 producers
      consumers <- List.range(1, 11).as(consumer[IO](scq)).pure[IO] // 10 consumers
      res <- (producers ++ consumers)
        .parSequence.as(ExitCode.Success) // Run producers and consumers in parallel until done (likely by user cancelling with CTRL-C)
        .handleErrorWith { t =>
          IO(println(s"Error caught: ${t.getMessage}")).as(ExitCode.Error)
        }
    } yield res
  }
}
