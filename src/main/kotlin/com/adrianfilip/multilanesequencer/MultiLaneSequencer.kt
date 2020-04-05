package com.adrianfilip.multilanesequencer

import arrow.core.Either
import arrow.core.Option
import arrow.core.getOrElse
import arrow.effects.IO
import reactor.core.publisher.EmitterProcessor
import reactor.core.scheduler.Schedulers
import java.util.*
import java.util.concurrent.CompletableFuture

interface MultiLaneSequencer {
    fun <T> sequence(lanes: Set<Lanes.Lane>, program: IO<T>): IO<CompletableFuture<T>>
}

/**
 * The point of providedRequestResponseEventBus is to give you the option to subscribe to the requestResponseEventBus
 * for monitoring and testing purposes.
 */
open class MultiLaneSequencerImpl(providedRequestResponseEventBus: Option<EmitterProcessor<Message>>) :
    MultiLaneSequencer {

    private val requestResponseEventBus: EmitterProcessor<Message> =
        providedRequestResponseEventBus.getOrElse { EmitterProcessor.create(false) }
    private val requestsEventBus: EmitterProcessor<Message.RequestMessage> = EmitterProcessor.create(false)

    sealed class Message {
        data class RequestMessage(
            val lanes: Set<Lanes.Lane>,
            val program: IO<*>,
            val key: String
        ) : Message()

        data class ResponseMessage(
            val lanes: Set<Lanes.Lane>,
            val key: String,
            val result: Either<Throwable, *>
        ) : Message()
    }

    data class WrappedMessage(
        val message: Message.RequestMessage,
        val lanes: MutableMap<Lanes.Lane, Int>
    )


    override fun <T> sequence(lanes: Set<Lanes.Lane>, program: IO<T>): IO<CompletableFuture<T>> = IO {
        val key = UUID.randomUUID().toString()

        // Here I subscribe for the answer to the request.
        val f = requestResponseEventBus
            .publishOn(Schedulers.elastic())
            .filter { it is Message.ResponseMessage }
            .filter {
                it as Message.ResponseMessage
                it.key == key
            }
            .take(1).collectList().toFuture()
            .thenApply {
                val m = it.first()
                m as Message.ResponseMessage
                m.result as Either<Throwable, T>
            }
            .toCompletableFuture()

        /**
         * I put synchronized here to make sure there are no 2 onNext happening at the same time. When I tested
         * the code using hundreds of thousands of messages the lack of this synchronisation caused
         * unexpected behaviours.
         */
        synchronized(requestResponseEventBus) {
            requestResponseEventBus.onNext(Message.RequestMessage(lanes, program, key))
        }

        f.thenCompose { it.toCompletableFuture() }
    }


    init {
        subscribeEventBus()
        subscribeProcessor()
    }

    private fun subscribeEventBus() {
        val lanes: MutableMap<Lanes.Lane, Int> = mutableMapOf()
        val pending: MutableList<WrappedMessage> = mutableListOf()

        requestResponseEventBus
            .publishOn(Schedulers.newSingle("MultiLaneSequencerImpl ${UUID.randomUUID()}"))
            .doOnNext { message ->
                when (message) {
                    is Message.RequestMessage -> {

                        val hasToWait = message.lanes.any { lanes[it] != null && lanes[it]!! > 0 }
                        if (hasToWait) {
                            val currentLanes = lanes.filterKeys { message.lanes.contains(it) }.toMutableMap()
                            pending.add(WrappedMessage(message, currentLanes))
                            message.lanes.forEach { lane ->
                                lanes[lane] = (lanes[lane] ?: 0) + 1
                            }
                        } else {
                            message.lanes.forEach { lane ->
                                lanes[lane] = (lanes[lane] ?: 0) + 1
                            }
                            synchronized(requestsEventBus) {
                                requestsEventBus.onNext(message)
                            }
                        }
                    }
                    is Message.ResponseMessage -> {

                        message.lanes.forEach { lane ->
                            lanes[lane] = lanes[lane]!! - 1
                        }

                        pending.forEach { wrapped ->
                            message.lanes.forEach { lane ->
                                if (wrapped.lanes.containsKey(lane)) {
                                    wrapped.lanes[lane] = wrapped.lanes[lane]!! - 1
                                }
                            }
                        }


                        val readyForProcessing = pending.filter {
                            it.lanes.values.all { lv -> lv == 0 }
                        }

                        pending.removeIf { it.lanes.values.all { lv -> lv == 0 } }

                        readyForProcessing.forEach {
                            synchronized(requestsEventBus) {
                                requestsEventBus.onNext(it.message)
                            }

                        }

                    }
                }

            }
            .subscribe()
    }


    private fun subscribeProcessor() {
        requestsEventBus
            .parallel()
            .runOn(Schedulers.parallel())
            .doOnNext {
                val res = it.program.attempt().unsafeRunSync()
                synchronized(this) {
                    requestResponseEventBus.onNext(Message.ResponseMessage(it.lanes, it.key, res))
                }
            }
            .subscribe()
    }

}

fun <T> Either<Throwable, T>.toCompletableFuture(): CompletableFuture<T> = this.fold(
    { err ->
        val future = CompletableFuture<T>()
        future.completeExceptionally(err)
        future
    },
    { r ->
        val future = CompletableFuture<T>()
        future.complete(r)
        future
    }
)