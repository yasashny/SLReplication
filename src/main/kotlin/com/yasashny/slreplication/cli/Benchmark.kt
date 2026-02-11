package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.ReplicationMode
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

data class OperationResult(
    val type: String,
    val success: Boolean,
    val latencyMs: Long
)

data class BenchmarkResult(
    val replicationMode: String,
    val rf: Int,
    val k: Int,
    val threads: Int,
    val putRatio: Double,
    val totalOps: Int,
    val successfulOps: Int,
    val failedOps: Int,
    val durationMs: Long,
    val throughputOpsSec: Double,
    val avgMs: Double,
    val p50Ms: Double,
    val p75Ms: Double,
    val p95Ms: Double,
    val p99Ms: Double,
    val putAvgMs: Double,
    val putP50Ms: Double,
    val putP95Ms: Double,
    val getAvgMs: Double,
    val getP50Ms: Double,
    val getP95Ms: Double
)

fun runBenchmark(manager: ClusterManager, args: List<String>) {
    var threads = 16
    var opsPerThread = 100
    var putRatio = 0.5
    var warmupOps = 50
    var outputFile: String? = null
    var runAll = false

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--threads" -> threads = args.getOrNull(++i)?.toIntOrNull() ?: threads
            "--ops" -> opsPerThread = args.getOrNull(++i)?.toIntOrNull() ?: opsPerThread
            "--put-ratio" -> putRatio = args.getOrNull(++i)?.toDoubleOrNull() ?: putRatio
            "--warmup" -> warmupOps = args.getOrNull(++i)?.toIntOrNull() ?: warmupOps
            "--output" -> outputFile = args.getOrNull(++i)
            "--run-all" -> runAll = true
            "--help" -> {
                println("""
                    Benchmark options:
                      --threads <n>      Number of concurrent threads (default: 16)
                      --ops <n>          Operations per thread (default: 100)
                      --put-ratio <f>    Ratio of PUT operations 0.0-1.0 (default: 0.5)
                      --warmup <n>       Warmup operations (default: 50)
                      --output <file>    Output CSV file (default: benchmarks/results.csv)
                      --run-all          Run all required benchmark scenarios
                      --help             Show this help
                """.trimIndent())
                return
            }
        }
        i++
    }

    val config = manager.getConfig()
    if (config.nodes.isEmpty()) {
        println("Error: No nodes in cluster. Add nodes first.")
        return
    }
    if (config.leaderId == null) {
        println("Error: No leader set. Use setLeader command first.")
        return
    }

    if (runAll) {
        runAllBenchmarks(manager, threads, opsPerThread, warmupOps, outputFile ?: "benchmarks/results.csv")
    } else {
        val result = runSingleBenchmark(manager, threads, opsPerThread, putRatio, warmupOps)
        printResult(result)

        if (outputFile != null) {
            saveResults(listOf(result), outputFile)
        }
    }
}

private fun runAllBenchmarks(
    manager: ClusterManager,
    threads: Int,
    opsPerThread: Int,
    warmupOps: Int,
    outputFile: String
) {
    val results = mutableListOf<BenchmarkResult>()

    println("Running all benchmark scenarios...")
    println()

    val modes = listOf(ReplicationMode.SYNC, ReplicationMode.ASYNC)
    val rfs = listOf(1, 2, 3)
    val putRatios = listOf(0.8, 0.2)

    for (mode in modes) {
        for (rf in rfs) {
            for (putRatio in putRatios) {
                println("=== ${mode.name}, RF=$rf, putRatio=$putRatio ===")

                manager.setReplicationMode(mode)
                manager.setReplicationFactor(rf)

                val result = runSingleBenchmark(manager, threads, opsPerThread, putRatio, warmupOps)
                results.add(result)
                printResultShort(result)
                println()

                Thread.sleep(1000)
            }
        }
    }

    val semiSyncModes = listOf(
        Triple(ReplicationMode.ASYNC, 3, 1),
        Triple(ReplicationMode.SEMI_SYNC, 3, 1),
        Triple(ReplicationMode.SYNC, 3, 1)
    )

    for ((mode, rf, k) in semiSyncModes) {
        for (putRatio in putRatios) {
            println("=== ${mode.name}, RF=$rf, K=$k, putRatio=$putRatio ===")

            manager.setReplicationFactor(rf)
            manager.setReplicationMode(mode)
            if (mode == ReplicationMode.SEMI_SYNC) {
                manager.setSemiSyncAcks(k)
            }

            val result = runSingleBenchmark(manager, threads, opsPerThread, putRatio, warmupOps)
            results.add(result)
            printResultShort(result)
            println()

            Thread.sleep(1000)
        }
    }

    saveResults(results, outputFile)
    println("Results saved to $outputFile")
}


private fun runSingleBenchmark(
    manager: ClusterManager,
    threads: Int,
    opsPerThread: Int,
    putRatio: Double,
    warmupOps: Int
): BenchmarkResult {
    val config = manager.getConfig()
    val results = ConcurrentLinkedQueue<OperationResult>()
    val keyCounter = AtomicInteger(0)
    val completedOps = AtomicLong(0)
    val totalOps = threads * opsPerThread

    if (warmupOps > 0) {
        print("Warming up... ")
        repeat(warmupOps) {
            val key = "warmup_${it}"
            manager.put(key, "warmup_value")
        }
        println("done")
    }

    print("Running benchmark: $threads threads, $opsPerThread ops/thread, ${(putRatio * 100).toInt()}% writes... ")

    val startTime = System.currentTimeMillis()

    runBlocking {
        val jobs = (1..threads).map { threadNum ->
            async(Dispatchers.IO) {
                repeat(opsPerThread) { opNum ->
                    val isPut = Random.nextDouble() < putRatio
                    val key = "key_${keyCounter.incrementAndGet()}"
                    val startOp = System.currentTimeMillis()

                    val success = try {
                        if (isPut) {
                            val result = manager.put(key, "value_$key")
                            result.isSuccess && result.getOrNull()?.status == "OK"
                        } else {
                            val result = manager.get(key)
                            result.isSuccess && result.getOrNull()?.status == "OK"
                        }
                    } catch (e: Exception) {
                        false
                    }

                    val latency = System.currentTimeMillis() - startOp
                    results.add(OperationResult(if (isPut) "PUT" else "GET", success, latency))
                    completedOps.incrementAndGet()
                }
            }
        }
        jobs.awaitAll()
    }

    val endTime = System.currentTimeMillis()
    val durationMs = endTime - startTime

    println("done")

    val allResults = results.toList()
    val successfulResults = allResults.filter { it.success }
    val failedResults = allResults.filter { !it.success }

    val allLatencies = successfulResults.map { it.latencyMs }.sorted()
    val putLatencies = successfulResults.filter { it.type == "PUT" }.map { it.latencyMs }.sorted()
    val getLatencies = successfulResults.filter { it.type == "GET" }.map { it.latencyMs }.sorted()

    fun percentile(sorted: List<Long>, p: Double): Double {
        if (sorted.isEmpty()) return 0.0
        val index = ((sorted.size - 1) * p).toInt()
        return sorted[index].toDouble()
    }

    fun avg(list: List<Long>): Double {
        if (list.isEmpty()) return 0.0
        return list.average()
    }

    return BenchmarkResult(
        replicationMode = config.replicationMode.name,
        rf = config.replicationFactor,
        k = config.semiSyncAcks,
        threads = threads,
        putRatio = putRatio,
        totalOps = totalOps,
        successfulOps = successfulResults.size,
        failedOps = failedResults.size,
        durationMs = durationMs,
        throughputOpsSec = if (durationMs > 0) successfulResults.size * 1000.0 / durationMs else 0.0,
        avgMs = avg(allLatencies),
        p50Ms = percentile(allLatencies, 0.50),
        p75Ms = percentile(allLatencies, 0.75),
        p95Ms = percentile(allLatencies, 0.95),
        p99Ms = percentile(allLatencies, 0.99),
        putAvgMs = avg(putLatencies),
        putP50Ms = percentile(putLatencies, 0.50),
        putP95Ms = percentile(putLatencies, 0.95),
        getAvgMs = avg(getLatencies),
        getP50Ms = percentile(getLatencies, 0.50),
        getP95Ms = percentile(getLatencies, 0.95)
    )
}

private fun printResult(result: BenchmarkResult) {
    println("""
        Benchmark Results:
          Mode: ${result.replicationMode}, RF: ${result.rf}, K: ${result.k}
          Threads: ${result.threads}, Put ratio: ${(result.putRatio * 100).toInt()}%

          Total ops: ${result.totalOps}
          Successful: ${result.successfulOps}
          Failed: ${result.failedOps}
          Duration: ${result.durationMs} ms

          Throughput: ${"%.2f".format(result.throughputOpsSec)} ops/sec

          Latency (all):
            avg: ${"%.2f".format(result.avgMs)} ms
            p50: ${"%.2f".format(result.p50Ms)} ms
            p75: ${"%.2f".format(result.p75Ms)} ms
            p95: ${"%.2f".format(result.p95Ms)} ms
            p99: ${"%.2f".format(result.p99Ms)} ms

          PUT latency:
            avg: ${"%.2f".format(result.putAvgMs)} ms
            p50: ${"%.2f".format(result.putP50Ms)} ms
            p95: ${"%.2f".format(result.putP95Ms)} ms

          GET latency:
            avg: ${"%.2f".format(result.getAvgMs)} ms
            p50: ${"%.2f".format(result.getP50Ms)} ms
            p95: ${"%.2f".format(result.getP95Ms)} ms
    """.trimIndent())
}

private fun printResultShort(result: BenchmarkResult) {
    println("  Throughput: ${"%.2f".format(result.throughputOpsSec)} ops/sec, " +
            "p50: ${"%.2f".format(result.p50Ms)} ms, p95: ${"%.2f".format(result.p95Ms)} ms, " +
            "Success: ${result.successfulOps}/${result.totalOps}")
}

private fun saveResults(results: List<BenchmarkResult>, filename: String) {
    val file = File(filename)
    file.parentFile?.mkdirs()

    val header = "replicationMode,rf,k,threads,putRatio,totalOps,successfulOps,failedOps," +
            "durationMs,throughputOpsSec,avgMs,p50Ms,p75Ms,p95Ms,p99Ms," +
            "putAvgMs,putP50Ms,putP95Ms,getAvgMs,getP50Ms,getP95Ms"

    val lines = results.map { r ->
        "${r.replicationMode},${r.rf},${r.k},${r.threads},${r.putRatio}," +
                "${r.totalOps},${r.successfulOps},${r.failedOps},${r.durationMs}," +
                "${"%.2f".format(r.throughputOpsSec)},${"%.2f".format(r.avgMs)}," +
                "${"%.2f".format(r.p50Ms)},${"%.2f".format(r.p75Ms)}," +
                "${"%.2f".format(r.p95Ms)},${"%.2f".format(r.p99Ms)}," +
                "${"%.2f".format(r.putAvgMs)},${"%.2f".format(r.putP50Ms)}," +
                "${"%.2f".format(r.putP95Ms)},${"%.2f".format(r.getAvgMs)}," +
                "${"%.2f".format(r.getP50Ms)},${"%.2f".format(r.getP95Ms)}"
    }

    file.writeText(header + "\n" + lines.joinToString("\n"))
}
