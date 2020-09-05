package catsEffectTutorial.producerconsumer

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.concurrent.Semaphore
import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.implicits._

import scala.collection.mutable

/**
 * Single producer - single consumer system using an unbounded concurrent queue.
 * Conturrent queue is built using semaphores.
 *
 * Second part of cats-effect tutorial at https://typelevel.org/cats-effect/tutorial/tutorial.html
 */
object SemaphoreProducerConsumerUnboundedQueue extends IOApp {

  val prev = new AtomicInteger(0)

  def process[F[_] : Sync](i: Int): F[Unit] =
    Sync[F].delay {
      if (!prev.compareAndSet(i - 1, i))
        throw new Exception(s"Race condition, it should be ${prev.incrementAndGet()} but is $i")
      if (i % 10000 == 0)
        println(s"Processed $i elements")
    }

  val counter = new AtomicInteger(0)

  def create[F[_] : Sync]: F[Int] =
    Sync[F].delay(counter.incrementAndGet())

  def producer[F[_] : Sync](buffer: mutable.Buffer[Int], lock: Semaphore[F]): F[Unit] =
    for {
      i <- create
      _ <- lock.acquire // Enter critical section, wait if lock already acquired
      _ <- Sync[F].delay(buffer.append(i))
      _ <- lock.release // Leave critical section
      _ <- producer(buffer, lock)
    } yield ()

  def consumer[F[_] : Sync](buffer: mutable.Buffer[Int], lock: Semaphore[F]): F[Unit] =
    for {
      _ <- lock.acquire // Enter critical section, wait if lock already acquired
      iO <- Sync[F].delay {
        if (buffer.nonEmpty) Option(buffer.remove(0))
        else None
      }
      _ <- lock.release // Leave critical section
      _ <- iO.fold(Sync[F].unit)(i => process(i))
      _ <- consumer(buffer, lock)
    } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    for {
      buffer <- IO(new mutable.ListBuffer[Int]())
      lock <- Semaphore[IO](1)
      res <- (producer(buffer, lock), consumer(buffer, lock))
        .parMapN((_, _) => ExitCode.Success) // Run producer and consumer in parallel until done (likely by user cancelling with CTRL-C)
        .handleErrorWith { t =>
          IO(println(s"Error caught: ${t.getMessage}")).as(ExitCode.Error)
        }
    } yield res
}
