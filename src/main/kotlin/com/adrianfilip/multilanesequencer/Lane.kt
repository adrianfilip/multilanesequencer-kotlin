package com.adrianfilip.multilanesequencer

object Lanes {

    interface Lane

    enum class Static : Lane {
        LANE1,
        LANE2,
        LANE3
    }

    data class CustomLane(val name: String) : Lane
}