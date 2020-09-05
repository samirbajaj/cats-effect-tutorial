package catsEffectTutorial.producerconsumer

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.concurrent.Semaphore
import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.implicits._

import scala.collection.mutable

/**
 * Multiple producer - multiple consumer system using a concurrent queue bounded in size.
 * Conturrent queue is built using semaphores.
 *
 * Second part of cats-effect tutorial at https://typelevel.org/cats-effect/tutorial/tutorial.html
 */
object SemaphoreMultiplesProducerConsumerBoundedQueue extends IOApp {

  def process[F[_] : Sync](i: Int): F[Unit] =
    Sync[F].delay {
      if (i % 10000 == 0)
        println(s"Processed $i elements")
    }

  val counter = new AtomicInteger(0)

  def create[F[_] : Sync]: F[Int] =
    Sync[F].delay(counter.incrementAndGet())

  def producer[F[_] : Sync](id: Int, count: Int, buffer: mutable.Buffer[Int], lock: Semaphore[F], empty: Semaphore[F], filled: Semaphore[F]): F[Unit] =
    for {
      i <- create
      _ <- empty.acquire // Wait for some empty bucket
      _ <- lock.withPermit { // Enter critical section, wait if lock already acquired, release when done
        Sync[F].delay(buffer.append(i))
      }
      _ <- filled.release // Signal new item in buffer
      newCount = count + 1
      _ <- if (newCount % 1000 == 0) Sync[F].delay(println(s"Producer $id has produced $newCount items")) else Sync[F].unit
      _ <- producer(id, newCount, buffer, lock, empty, filled)
    } yield ()

  def consumer[F[_] : Sync](id: Int, count: Int, buffer: mutable.Buffer[Int], lock: Semaphore[F], empty: Semaphore[F], filled: Semaphore[F]): F[Unit] =
    for {
      _ <- filled.acquire // Wait for some item in buffer
      i <- lock.withPermit { // Enter critical section, wait if lock already acquired, release when done
        Sync[F].delay(buffer.remove(0))
      }
      _ <- empty.release // Signal new empty bucket
      _ <- process(i)
      newCount = count + 1
      _ <- if (newCount % 1000 == 0) Sync[F].delay(println(s"Consumer $id has consumed $newCount items")) else Sync[F].unit
      _ <- consumer(id, newCount, buffer, lock, empty, filled)
    } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    for {
      buffer <- IO(new mutable.ListBuffer[Int]())
      lock <- Semaphore[IO](1)
      empty <- Semaphore[IO](100)
      filled <- Semaphore[IO](0)
      producers <- List.range(1, 11).map(producer(_, 0, buffer, lock, empty, filled)).pure[IO] // 10 producers
      consumers <- List.range(1, 11).map(consumer(_, 0, buffer, lock, empty, filled)).pure[IO] // 10 consumers
      res <- (producers ++ consumers)
        .parSequence.as(ExitCode.Success) // Run producers and consumers in parallel until done (likely by user cancelling with CTRL-C)
        .handleErrorWith { t =>
          IO(println(s"Error caught: ${t.getMessage}")).as(ExitCode.Error)
        }
    } yield res
}
