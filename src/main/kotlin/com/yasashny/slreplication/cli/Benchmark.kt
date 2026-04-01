package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.*
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
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
    val getP95Ms: Double,
    val mode: String = "SINGLE",
    val topology: String = "N/A",
    val keySpace: Int = 0,
    val convergenceTimeMs: Long = 0
)

fun runBenchmark(manager: ClusterManager, args: List<String>) {
    var threads = 16
    var opsPerThread = 100
    var putRatio = 0.5
    var warmupOps = 50
    var outputFile: String? = null
    var runAll = false
    var runAllMulti = false
    var keySpace = 10000

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--threads" -> threads = args.getOrNull(++i)?.toIntOrNull() ?: threads
            "--ops" -> opsPerThread = args.getOrNull(++i)?.toIntOrNull() ?: opsPerThread
            "--put-ratio" -> putRatio = args.getOrNull(++i)?.toDoubleOrNull() ?: putRatio
            "--warmup" -> warmupOps = args.getOrNull(++i)?.toIntOrNull() ?: warmupOps
            "--output" -> outputFile = args.getOrNull(++i)
            "--run-all" -> runAll = true
            "--run-all-multi" -> runAllMulti = true
            "--key-space" -> keySpace = args.getOrNull(++i)?.toIntOrNull() ?: keySpace
            "--help" -> {
                println("""
                    Benchmark options:
                      --threads <n>       Concurrent threads (default: 16)
                      --ops <n>           Operations per thread (default: 100)
                      --put-ratio <f>     PUT ratio 0.0-1.0 (default: 0.5)
                      --warmup <n>        Warmup operations (default: 50)
                      --output <file>     Output CSV file
                      --run-all           Run all single-leader scenarios
                      --run-all-multi     Run all multi-leader scenarios (Parts A, B, C)
                      --key-space <n>     Unique keys (default: 10000)
                      --help              Show this help
                """.trimIndent())
                return
            }
        }
        i++
    }

    val config = manager.getConfig()
    if (config.nodes.isEmpty()) {
        println("Error: No nodes in cluster.")
        return
    }

    if (runAllMulti) {
        runAllMultiBenchmarks(manager, threads, opsPerThread, warmupOps, outputFile ?: "benchmarks/results-multi.csv")
    } else if (runAll) {
        if (config.leaderId == null && config.mode == ClusterMode.SINGLE) {
            println("Error: No leader set.")
            return
        }
        runAllSingleBenchmarks(manager, threads, opsPerThread, warmupOps, outputFile ?: "benchmarks/results.csv")
    } else {
        val result = runSingleBenchmark(manager, threads, opsPerThread, putRatio, warmupOps, keySpace)
        printResult(result)
        if (outputFile != null) saveResults(listOf(result), outputFile)
    }
}

// ========== Single-leader benchmark scenarios (HW2) ==========

private fun runAllSingleBenchmarks(manager: ClusterManager, threads: Int, opsPerThread: Int, warmupOps: Int, outputFile: String) {
    val results = mutableListOf<BenchmarkResult>()
    println("Running all single-leader benchmark scenarios...")
    println()

    // Part A: sync/async x RF(1,2,3) x putRatio(0.8, 0.2) = 12 runs
    for (mode in listOf(ReplicationMode.SYNC, ReplicationMode.ASYNC)) {
        for (rf in listOf(1, 2, 3)) {
            for (putRatio in listOf(0.8, 0.2)) {
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

    // Part B: async/semi-sync(K=1)/sync x RF=3 x putRatio(0.8, 0.2) = 6 runs
    for ((mode, rf, k) in listOf(
        Triple(ReplicationMode.ASYNC, 3, 1),
        Triple(ReplicationMode.SEMI_SYNC, 3, 1),
        Triple(ReplicationMode.SYNC, 3, 1)
    )) {
        for (putRatio in listOf(0.8, 0.2)) {
            println("=== ${mode.name}, RF=$rf, K=$k, putRatio=$putRatio ===")
            manager.setReplicationFactor(rf)
            manager.setReplicationMode(mode)
            if (mode == ReplicationMode.SEMI_SYNC) manager.setSemiSyncAcks(k)
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

// ========== Multi-leader benchmark scenarios (HW3) ==========

private fun runAllMultiBenchmarks(manager: ClusterManager, threads: Int, opsPerThread: Int, warmupOps: Int, outputFile: String) {
    val results = mutableListOf<BenchmarkResult>()
    val config = manager.getConfig()

    val allNodeIds = config.nodes.map { it.nodeId }.sorted()
    if (allNodeIds.size < 10) {
        println("Warning: Multi-leader benchmarks expect 10 nodes, have ${allNodeIds.size}")
    }

    val leaders = allNodeIds.take(4)
    println("Running multi-leader benchmark scenarios...")
    println("Leaders: ${leaders.joinToString(", ")}")
    println()

    // --- Part A: Topology comparison (6 runs) ---
    // mode=multi, topology=mesh|ring|star, putRatio=0.8|0.2, threads=16, totalOps>=200000
    val multiOpsPerThread = maxOf(opsPerThread, 200000 / threads)

    manager.setMode(ClusterMode.MULTI)
    manager.setLeaders(leaders)

    for (topo in listOf(Topology.MESH, Topology.RING, Topology.STAR)) {
        manager.setTopology(topo)
        if (topo == Topology.STAR) manager.setStarCenter(allNodeIds.first())

        for (putRatio in listOf(0.8, 0.2)) {
            println("=== Part A: MULTI ${topo.name}, putRatio=$putRatio ===")
            val result = runSingleBenchmark(manager, threads, multiOpsPerThread, putRatio, warmupOps, 10000)
            results.add(result)
            printResultShort(result)
            println()
            Thread.sleep(1000)
        }
    }

    // --- Part B: Conflict comparison (2 runs) ---
    // mode=multi, topology=mesh, putRatio=1.0, threads=32, keySpace=5 and 10000
    manager.setTopology(Topology.MESH)

    for (ks in listOf(5, 10000)) {
        println("=== Part B: MULTI MESH, putRatio=1.0, threads=32, keySpace=$ks ===")
        val result = runSingleBenchmark(manager, 32, multiOpsPerThread, 1.0, warmupOps, ks)
        results.add(result)
        printResultShort(result)
        println()
        Thread.sleep(1000)
    }

    // --- Part C: Single vs Multi comparison (4 runs) ---
    // 1. single, async, RF=1
    manager.setMode(ClusterMode.SINGLE)
    manager.setLeader(allNodeIds.first())
    manager.setReplicationMode(ReplicationMode.ASYNC)
    manager.setReplicationFactor(1)

    println("=== Part C: SINGLE ASYNC RF=1 ===")
    var result = runSingleBenchmark(manager, threads, multiOpsPerThread, 0.8, warmupOps, 10000)
    results.add(result)
    printResultShort(result)
    println()
    Thread.sleep(1000)

    // 2. single, sync, RF=3
    manager.setReplicationMode(ReplicationMode.SYNC)
    manager.setReplicationFactor(3)

    println("=== Part C: SINGLE SYNC RF=3 ===")
    result = runSingleBenchmark(manager, threads, multiOpsPerThread, 0.8, warmupOps, 10000)
    results.add(result)
    printResultShort(result)
    println()
    Thread.sleep(1000)

    // 3. multi, mesh
    manager.setMode(ClusterMode.MULTI)
    manager.setLeaders(leaders)
    manager.setTopology(Topology.MESH)

    println("=== Part C: MULTI MESH ===")
    result = runSingleBenchmark(manager, threads, multiOpsPerThread, 0.8, warmupOps, 10000)
    results.add(result)
    printResultShort(result)
    println()
    Thread.sleep(1000)

    // 4. multi, star
    manager.setTopology(Topology.STAR)
    manager.setStarCenter(allNodeIds.first())

    println("=== Part C: MULTI STAR ===")
    result = runSingleBenchmark(manager, threads, multiOpsPerThread, 0.8, warmupOps, 10000)
    results.add(result)
    printResultShort(result)
    println()

    saveResults(results, outputFile)
    println("Results saved to $outputFile")
}

// ========== Core benchmark runner ==========

private fun runSingleBenchmark(
    manager: ClusterManager,
    threads: Int,
    opsPerThread: Int,
    putRatio: Double,
    warmupOps: Int,
    keySpace: Int = 10000
): BenchmarkResult {
    val config = manager.getConfig()
    val results = ConcurrentLinkedQueue<OperationResult>()
    val completedOps = AtomicLong(0)
    val totalOps = threads * opsPerThread

    // Warmup
    if (warmupOps > 0) {
        print("Warming up... ")
        val warmupTarget = if (config.mode == ClusterMode.MULTI) null else null // use default target
        repeat(warmupOps) { manager.put("warmup_$it", "warmup_value", warmupTarget) }
        println("done")
    }

    print("Running: $threads threads, $opsPerThread ops/thread, ${(putRatio * 100).toInt()}% writes, keySpace=$keySpace... ")

    val startTime = System.currentTimeMillis()

    runBlocking {
        val jobs = (1..threads).map {
            async(Dispatchers.IO) {
                repeat(opsPerThread) {
                    val isPut = Random.nextDouble() < putRatio
                    val key = "key_${Random.nextInt(keySpace)}"
                    val startOp = System.currentTimeMillis()

                    val success = try {
                        if (isPut) {
                            val r = manager.put(key, "val_${Random.nextInt(100000)}")
                            r.isSuccess && r.getOrNull()?.status == "OK"
                        } else {
                            val r = manager.get(key)
                            r.isSuccess && r.getOrNull()?.status == "OK"
                        }
                    } catch (_: Exception) { false }

                    val latency = System.currentTimeMillis() - startOp
                    results.add(OperationResult(if (isPut) "PUT" else "GET", success, latency))
                    completedOps.incrementAndGet()
                }
            }
        }
        jobs.awaitAll()
    }

    val durationMs = System.currentTimeMillis() - startTime
    println("done (${durationMs}ms)")

    // Measure convergence time for multi mode
    val convergenceTimeMs = if (config.mode == ClusterMode.MULTI) {
        measureConvergenceTime(manager)
    } else 0L

    val allResults = results.toList()
    val successfulResults = allResults.filter { it.success }
    val failedResults = allResults.filter { !it.success }
    val allLatencies = successfulResults.map { it.latencyMs }.sorted()
    val putLatencies = successfulResults.filter { it.type == "PUT" }.map { it.latencyMs }.sorted()
    val getLatencies = successfulResults.filter { it.type == "GET" }.map { it.latencyMs }.sorted()

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
        getP95Ms = percentile(getLatencies, 0.95),
        mode = config.mode.name,
        topology = if (config.mode == ClusterMode.MULTI) config.topology.name else "N/A",
        keySpace = keySpace,
        convergenceTimeMs = convergenceTimeMs
    )
}

private fun measureConvergenceTime(manager: ClusterManager): Long {
    val testKey = "_convergence_test_${System.currentTimeMillis()}"
    val testValue = "conv_${Random.nextInt(1000000)}"
    val config = manager.getConfig()

    // Write to one leader
    val leader = config.leaderNodeIds.firstOrNull() ?: return 0
    manager.put(testKey, testValue, leader)

    val start = System.currentTimeMillis()
    val deadline = start + 30000 // 30s max wait

    while (System.currentTimeMillis() < deadline) {
        val results = manager.getAll(testKey)
        val allConverged = results.values.all { r ->
            r.getOrNull()?.value == testValue
        }
        if (allConverged) {
            return System.currentTimeMillis() - start
        }
        Thread.sleep(50)
    }

    return System.currentTimeMillis() - start
}

// ========== Printing & saving ==========

private fun printResult(result: BenchmarkResult) {
    println("""
        Benchmark Results:
          Mode: ${result.mode}, Topology: ${result.topology}
          Replication: ${result.replicationMode}, RF: ${result.rf}, K: ${result.k}
          Threads: ${result.threads}, Put ratio: ${(result.putRatio * 100).toInt()}%, KeySpace: ${result.keySpace}

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

          PUT latency: avg=${"%.2f".format(result.putAvgMs)} p50=${"%.2f".format(result.putP50Ms)} p95=${"%.2f".format(result.putP95Ms)} ms
          GET latency: avg=${"%.2f".format(result.getAvgMs)} p50=${"%.2f".format(result.getP50Ms)} p95=${"%.2f".format(result.getP95Ms)} ms
          Convergence time: ${result.convergenceTimeMs} ms
    """.trimIndent())
}

private fun printResultShort(result: BenchmarkResult) {
    val conv = if (result.convergenceTimeMs > 0) ", conv=${result.convergenceTimeMs}ms" else ""
    println("  Throughput: ${"%.2f".format(result.throughputOpsSec)} ops/sec, " +
            "p50: ${"%.2f".format(result.p50Ms)} ms, p95: ${"%.2f".format(result.p95Ms)} ms, " +
            "Success: ${result.successfulOps}/${result.totalOps}$conv")
}

private fun saveResults(results: List<BenchmarkResult>, filename: String) {
    val file = File(filename)
    file.parentFile?.mkdirs()

    val header = "mode,topology,replicationMode,rf,k,threads,putRatio,keySpace,totalOps,successfulOps,failedOps," +
            "durationMs,throughputOpsSec,avgMs,p50Ms,p75Ms,p95Ms,p99Ms," +
            "putAvgMs,putP50Ms,putP95Ms,getAvgMs,getP50Ms,getP95Ms,convergenceTimeMs"

    val lines = results.map { r ->
        "${r.mode},${r.topology},${r.replicationMode},${r.rf},${r.k},${r.threads},${r.putRatio}," +
                "${r.keySpace},${r.totalOps},${r.successfulOps},${r.failedOps},${r.durationMs}," +
                "${"%.2f".format(r.throughputOpsSec)},${"%.2f".format(r.avgMs)}," +
                "${"%.2f".format(r.p50Ms)},${"%.2f".format(r.p75Ms)}," +
                "${"%.2f".format(r.p95Ms)},${"%.2f".format(r.p99Ms)}," +
                "${"%.2f".format(r.putAvgMs)},${"%.2f".format(r.putP50Ms)}," +
                "${"%.2f".format(r.putP95Ms)},${"%.2f".format(r.getAvgMs)}," +
                "${"%.2f".format(r.getP50Ms)},${"%.2f".format(r.getP95Ms)},${r.convergenceTimeMs}"
    }

    file.writeText(header + "\n" + lines.joinToString("\n"))
}

private fun percentile(sorted: List<Long>, p: Double): Double {
    if (sorted.isEmpty()) return 0.0
    val index = ((sorted.size - 1) * p).toInt()
    return sorted[index].toDouble()
}

private fun avg(list: List<Long>): Double {
    if (list.isEmpty()) return 0.0
    return list.average()
}
