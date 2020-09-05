package catsEffectTutorial.producerconsumer

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.concurrent.Semaphore
import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.implicits._

import scala.collection.mutable

/**
 * Single producer - single consumer system using an unbounded concurrent queue. Conturrent queue is built
 * using semaphores. Similar to [[SemaphoreProducerConsumerUnboundedQueue]], but a bit improved as
 * [[SemaphoreProducerConsumerUnboundedQueuev2.consumer]] function will block when queue is empty, instead
 * of running constantly.
 *
 * Second part of cats-effect tutorial at https://typelevel.org/cats-effect/tutorial/tutorial.html
 */
object SemaphoreProducerConsumerUnboundedQueuev2 extends IOApp {

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

  def producer[F[_] : Sync](buffer: mutable.Buffer[Int], lock: Semaphore[F], filled: Semaphore[F]): F[Unit] =
    for {
      i <- create
      _ <- lock.withPermit { // Enter critical section, wait if lock already acquired, release when done
        Sync[F].delay(buffer.append(i))
      }
      _ <- filled.release // Signal new item in buffer
      _ <- producer(buffer, lock, filled)
    } yield ()

  def consumer[F[_] : Sync](buffer: mutable.Buffer[Int], lock: Semaphore[F], filled: Semaphore[F]): F[Unit] =
    for {
      _ <- filled.acquire // Wait for some item in buffer
      i <- lock.withPermit { // Enter critical section, wait if lock already acquired, release when done
        Sync[F].delay(buffer.remove(0))
      }
      _ <- process(i)
      _ <- consumer(buffer, lock, filled)
    } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    for {
      buffer <- IO(new mutable.ListBuffer[Int]())
      lock <- Semaphore[IO](1)
      filled <- Semaphore[IO](0)
      res <- (producer(buffer, lock, filled), consumer(buffer, lock, filled))
        .parMapN((_, _) => ExitCode.Success) // Run producer and consumer in parallel until done (likely by user cancelling with CTRL-C)
        .handleErrorWith { t =>
          IO(println(s"Error caught: ${t.getMessage}")).as(ExitCode.Error)
        }
    } yield res
}
