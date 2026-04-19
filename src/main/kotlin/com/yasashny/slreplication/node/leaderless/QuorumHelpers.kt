package com.yasashny.slreplication.node.leaderless

import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration.Companion.milliseconds

internal suspend fun waitForQuorum(
    progress: () -> Int,
    required: Int,
    jobs: List<Job>,
    timeoutMs: Long
) {
    try {
        withTimeout(timeoutMs.milliseconds) {
            while (progress() < required) {
                if (jobs.all { it.isCompleted }) break
                delay(5.milliseconds)
            }
        }
    } catch (_: TimeoutCancellationException) {
    }
}
