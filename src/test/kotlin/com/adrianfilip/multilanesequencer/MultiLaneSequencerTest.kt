package com.adrianfilip.multilanesequencer

import arrow.core.Some
import arrow.effects.IO
import org.junit.Test
import reactor.core.publisher.EmitterProcessor
import reactor.core.scheduler.Schedulers
import java.util.concurrent.ForkJoinPool
import kotlin.random.Random

class MultiLaneSequencerTest {

    private fun program(index: Int, delay: Long) = IO {
        Thread.sleep(delay)
        index
    }

    data class CustomLane(val label: String) : Lanes.Lane {
        override fun toString(): String {
            return label
        }
    }

    @Test
    fun test() {
        val lanesLineup = setOf("A", "B", "C", "D", "E").toList()

        val requests = (1..200).map {
            //			[from, to)
            val lanes = mutableSetOf<String>()
            (1..Random.nextInt(1, 6)).forEach { _ ->
                lanes.add(lanesLineup[Random.nextInt(0, 5)])
            }
            val imSet = lanes.map { l -> CustomLane(l) }.toSet()
            (imSet to it) to program(it, Random.nextLong(1, 101))
        }

        val monitorBus: EmitterProcessor<MultiLaneSequencerImpl.Message> = EmitterProcessor.create(false)

        val target = MultiLaneSequencerImpl(Some(monitorBus))

        val requestsMap: MutableMap<Lanes.Lane, List<String>> = mutableMapOf()
        val responsesMap: MutableMap<Lanes.Lane, List<String>> = mutableMapOf()


        monitorBus
            .publishOn(Schedulers.newSingle("TestSingle"))
            .doOnNext { message ->
                when (message) {
                    is MultiLaneSequencerImpl.Message.RequestMessage -> {
                        message.lanes.forEach {
                            requestsMap[it] = (requestsMap[it] ?: listOf()).plus(message.key)
                        }
                    }
                    is MultiLaneSequencerImpl.Message.ResponseMessage -> {
                        message.lanes.forEach {
                            responsesMap[it] = (responsesMap[it] ?: listOf()).plus(message.key)
                        }
                    }
                }
            }
            .subscribe()

        val customThreadPool = ForkJoinPool(8)
        customThreadPool.submit {
                requests.parallelStream().forEach {
                    target.sequence(it.first.first, it.second).unsafeRunSync().get()
                }
            }
            .get()

        println(requestsMap)
        println(responsesMap)

        assert(requestsMap.values.flatten().size == requests.map { it.first.first }.flatten().size)
        assert(requestsMap == responsesMap)
    }

}