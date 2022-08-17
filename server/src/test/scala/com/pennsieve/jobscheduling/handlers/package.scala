// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

import org.scalactic.source
import org.scalatest.exceptions.TestFailedException

import scala.reflect.ClassTag

package object handlers {

  implicit class ResponseUnwrapper[T](response: T) {

    def getOrFail[R <: Product, V](
      implicit
      pos: source.Position,
      extractorClassTag: ClassTag[R],
      valueClassTag: ClassTag[V]
    ): V =
      response match {
        case extractorClassTag(typedResponse) =>
          if (typedResponse.productArity == 1) {
            typedResponse.productElement(0).asInstanceOf[V]
          } else {
            throw new TestFailedException(
              _ =>
                Some(
                  s"Unable to extract ${valueClassTag.runtimeClass.getCanonicalName} from ${valueClassTag.runtimeClass.getCanonicalName}"
                ),
              None,
              pos
            )
          }
        case invalid =>
          throw new TestFailedException(
            _ =>
              Some(
                s"Invalid response $invalid; expected ${extractorClassTag.runtimeClass.getCanonicalName}"
              ),
            None,
            pos
          )
      }
  }
}
