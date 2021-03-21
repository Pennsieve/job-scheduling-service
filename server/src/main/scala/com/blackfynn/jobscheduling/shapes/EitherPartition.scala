// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.shapes

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic }
import akka.stream._

class EitherPartition[Left, Right]()
    extends GraphStage[FanOutShape2[Either[Left, Right], Left, Right]] {
  private val name = "EitherPartitionFanOut"

  override val shape =
    new FanOutShape2[Either[Left, Right], Left, Right](name)

  val in: Inlet[Either[Left, Right]] = shape.in
  val left: Outlet[Left] = shape.out0
  val right: Outlet[Right] = shape.out1

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private def bothAvailable: Boolean =
        isAvailable(left) && isAvailable(right)

      private def attemptPull(): Unit = {
        if (bothAvailable && isAvailable(in)) grabAndPush()
        else if (bothAvailable && !hasBeenPulled(in)) pull(in)
      }

      private def grabAndPush(): Unit =
        grab(in) match {
          case Left(errorWithPromise) =>
            push(left, errorWithPromise)
          case Right(jobWithPromise) =>
            push(right, jobWithPromise)
        }

      setHandler(in = in, handler = () => {
        if (bothAvailable) grabAndPush()
      })

      setHandler(out = left, handler = () => attemptPull())

      setHandler(out = right, handler = () => attemptPull())
    }
}

object EitherPartition {
  def apply[Left, Right]: EitherPartition[Left, Right] = new EitherPartition[Left, Right]()

  def apply[Left, Right](leftHandler: Left => Unit): Flow[Either[Left, Right], Right, NotUsed] =
    Flow.fromGraph {
      GraphDSL.create(EitherPartition[Left, Right], Sink.foreach(leftHandler))(Keep.left) {
        implicit builder => (eitherPartition, leftSink) =>
          import GraphDSL.Implicits._

          eitherPartition.out0 ~> leftSink.in

          FlowShape(eitherPartition.in, eitherPartition.out1)
      }
    }

  implicit class EitherPartitionFlowOps[In, Left, Right, +Mat](
    flow: Flow[In, Either[Left, Right], Mat]
  ) {

    def collectRightHandleLeftWith(leftHandler: Left => Unit): Flow[In, Right, Mat] =
      flow.via(EitherPartition(leftHandler))
  }

  implicit class EitherPartitionSourceOps[Left, Right, Mat](
    source: Source[Either[Left, Right], Mat]
  ) {

    def collectRightHandleLeftWith(leftHandler: Left => Unit): Source[Right, Mat] =
      source.via(EitherPartition(leftHandler))
  }
}
