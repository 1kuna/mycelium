// Compile before submission: swiftc -O -framework Metal SOURCE -o metal-probe
// Upload the binary under the qualification's trusted Worker.EnvironmentRoot.
// No compiler/tool installation or shader compilation occurs in the payload.
import Foundation
import Metal

enum ProbeError: Error { case failed(String) }

func probe() throws {
    guard ProcessInfo.processInfo.environment["MYCE_PLATFORM"] == "metal",
          let device = MTLCreateSystemDefaultDevice(),
          let queue = device.makeCommandQueue() else {
        throw ProbeError.failed("assigned Metal device/queue unavailable")
    }
    // A private GPU buffer plus shared readback proves real GPU blit completion.
    // ARC releases all buffers, commands and the queue before the receipt below.
    try autoreleasepool {
        let size = 4096
        guard let gpu = device.makeBuffer(length: size, options: .storageModePrivate),
              let host = device.makeBuffer(length: size, options: .storageModeShared),
              let command = queue.makeCommandBuffer(),
              let blit = command.makeBlitCommandEncoder() else {
            throw ProbeError.failed("Metal buffer/blit allocation failed")
        }
        blit.fill(buffer: gpu, range: 0..<size, value: 0xA5)
        blit.copy(from: gpu, sourceOffset: 0, to: host, destinationOffset: 0, size: size)
        blit.endEncoding()
        command.commit()
        command.waitUntilCompleted()
        guard command.status == .completed, command.error == nil else {
            throw ProbeError.failed("Metal blit failed: \(String(describing: command.error))")
        }
        let bytes = host.contents().bindMemory(to: UInt8.self, capacity: size)
        guard (0..<size).allSatisfy({ bytes[$0] == 0xA5 }) else {
            throw ProbeError.failed("Metal fill/read mismatch")
        }
    }
}

do {
    try probe()
    print("native-isolation-gpu-ok:metal")
} catch {
    fputs("Metal qualification failed: \(error)\n", stderr)
    exit(1)
}
