import Foundation
import AsyncAlgorithms

// MARK: - Memory measurement (Mach APi boilerplate)
// Reads the processor's physical footprint - roughly the "Memory" column in Activity Monitor.

func memoryFootprintMB() -> Double {
    var info = task_vm_info_data_t()
    var count = mach_msg_type_number_t(
        MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
    let result = withUnsafeMutablePointer(to: &info) {
        $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
            task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count)
        }
    }
    guard result == KERN_SUCCESS else { return -1 }
    return Double(info.phys_footprint) / 1024 / 1024
}

// MARK: - Test payload
struct SpikeItem: Sendable {
    let index: Int
    let payload: String
}

let itemCount = 1_000_000
let logEvery = 100_000
// ~1KB per item so buffering shows up in memory. 1M items buffered ~ 1GB.
let payload = String(repeating: "x", count: 1024)

// MARK: - Experiment 1: does a plain AsyncStream apply backpressure?
// Hypothesis: no. The producer runs ahead of the consumer and items pile up
// in the stream's unbounded buffer. Primary evidence: produced/consumed counters
// diverging. Secondary evidence: memory footprint climbing.

print("=== Experiment 1: AsyncStream (default .unbounded) ===")
print(String(format: "start footprint: %.1f MB", memoryFootprintMB()))

let (stream, continuation) = AsyncStream.makeStream(of: SpikeItem.self)

let producer = Task {
    for i in 1...itemCount {
        continuation.yield(SpikeItem(index: i, payload: payload))
        if i % logEvery == 0 {
            print(String(format: "produced: %7d footprint: %.1f MB", i, memoryFootprintMB()))
        }
    }
    continuation.finish()
    print("producer finished")
}

var consumed = 0
for await _ in stream {
    consumed += 1
    // Deliberately slow consumer: ~1ms pause per 1000 items.
    if consumed % 1_000 == 0 {
        try? await Task.sleep(for: .milliseconds(1))
    }
    if consumed % logEvery == 0 {
        print(String(format: "consumed: %7d foorprint: %.1f MB", consumed, memoryFootprintMB()))
    }
}
await producer.value
print(String(format: "consumed total: %d final footprint: %.1f MB", consumed, memoryFootprintMB()))

// MARK: - Experiment 2: does cancellation propagate upstream?
// Cancel the task that iterates the stream, then observe:
//   1. does the for-await loop exit?
//   2. does continuation.onTermination fire, and with what reason?
//   3. what does yield() return afterwards? (expect .terminated)
// No payload here — an unthrottled producer with 1KB items would eat GBs within the 1s window.

print("\n=== Experiment 2: cancellation propagation ===")

let (stream2, continuation2) = AsyncStream.makeStream(of: Int.self)
continuation2.onTermination = { reason in
    print("onTermination fired: \(reason)")
}

let producer2 = Task.detached {
    var i = 0
    while true {
        i += 1
        await Task.yield()
        let result = continuation2.yield(i)
        if case .terminated = result {
            print("yield returned .terminated at item \(i)")
            break
        }
    }
}

let piepeline = Task.detached {
    var received = 0
    for await _ in stream2 {
        received += 1
    }
    print("consumer loop exited after \(received) items")
}

try? await Task.sleep(for: .seconds(1))
print("cancelling piepeline...")
piepeline.cancel()
await piepeline.value
await producer2.value

print("\ndone")

print("=== Experiment 3: AsyncChannel (default .unbounded) ===")
print(String(format: "start footprint: %.1f MB", memoryFootprintMB()))

let channel = AsyncChannel<SpikeItem>()

let producer3 = Task {
    for i in 1...itemCount {
        await channel.send(SpikeItem(index: i, payload: payload))
        if i % logEvery == 0 {
            print(String(format: "produced: %7d footprint: %.1f MB", i, memoryFootprintMB()))
        }
    }
    channel.finish()
    print("producer finished")
}

var consumed3 = 0
for await _ in channel {
    consumed3 += 1
    // Deliberately slow consumer: ~1ms pause per 1000 items.
    if consumed3 % 1_000 == 0 {
        try? await Task.sleep(for: .milliseconds(1))
    }
    if consumed3 % logEvery == 0 {
        print(String(format: "consumed: %7d foorprint: %.1f MB", consumed3, memoryFootprintMB()))
    }
}
await producer3.value
print(String(format: "consumed total: %d final footprint: %.1f MB", consumed, memoryFootprintMB()))
