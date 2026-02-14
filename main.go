// loadgen.go — System resource stress tool
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	modeCPU  = "cpu"
	modeMem  = "mem"
	modeDisk = "disk"
)

func main() {
	cpuFlag := flag.Bool("p", false, "CPU exhaustion mode")
	memFlag := flag.Bool("m", false, "Memory exhaustion mode")
	diskFlag := flag.Bool("d", false, "Disk exhaustion mode")
	limit := flag.Float64("limit", 95.0, "Target resource usage in % of total system capacity (for MEM/DISK: % of free space)")
	speed := flag.Float64("speed", 20.0, "Ramp-up speed in PercentPerMinute")
	timeout := flag.Int("T", 600, "Time limit in seconds (program exits after)")
	diskPath := flag.String("disk-path", os.TempDir(), "Directory for temp file in disk mode")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `loadgen — System resource stress tool

Keys:
  -p            CPU exhaustion mode (default if none selected)
  -m            Memory exhaustion mode
  -d            Disk exhaustion mode
  -limit float  Target usage %% of capacity (default 95)
                For MEM/DISK: percentage of FREE space to consume
  -speed float  Ramp-up speed in PercentPerMinute (default 20)
  -T int        Time limit in seconds (default 600)
  -disk-path    Directory for temp file in disk mode (default: OS temp dir)

Examples:
  loadgen -p -limit 80 -speed 30 -T 300
  loadgen -m -limit 90
  loadgen -d -limit 50 -disk-path /tmp
`)
	}
	flag.Parse()

	mode := modeCPU
	selected := 0
	if *cpuFlag {
		mode = modeCPU
		selected++
	}
	if *memFlag {
		mode = modeMem
		selected++
	}
	if *diskFlag {
		mode = modeDisk
		selected++
	}
	if selected > 1 {
		fmt.Fprintln(os.Stderr, "Error: specify only one of -p, -m, -d")
		os.Exit(1)
	}

	cores := runtime.NumCPU()

	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║              loadgen — stress tool                  ║")
	fmt.Println("╠══════════════════════════════════════════════════════╣")
	fmt.Printf("║  Mode       : %-38s║\n", modeLabel(mode))
	fmt.Printf("║  Target     : %-5.1f%% of capacity                    ║\n", *limit)
	fmt.Printf("║  Speed      : %-5.1f%%/min                            ║\n", *speed)
	fmt.Printf("║  Time limit : %-4d seconds                          ║\n", *timeout)
	fmt.Printf("║  CPU cores  : %-4d                                  ║\n", cores)
	if mode == modeDisk {
		fmt.Printf("║  Disk path  : %-38s║\n", *diskPath)
	}
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Second)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigCh:
			fmt.Printf("\nReceived %v, shutting down...\n", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	switch mode {
	case modeCPU:
		runCPU(ctx, *limit, *speed)
	case modeMem:
		runMem(ctx, *limit, *speed)
	case modeDisk:
		runDisk(ctx, *limit, *speed, *diskPath)
	}

	fmt.Println("\nDone. Exiting.")
}

func modeLabel(m string) string {
	switch m {
	case modeCPU:
		return "CPU (threaded multi-core)"
	case modeMem:
		return "Memory"
	case modeDisk:
		return "Disk"
	}
	return m
}

// ──────────────────────────── CPU ────────────────────────────────────────────

func runCPU(ctx context.Context, limitPct, speedPPM float64) {
	cores := runtime.NumCPU()
	runtime.GOMAXPROCS(cores)

	fmt.Printf("[CPU] Spinning up %d worker goroutines (one per core)\n", cores)
	fmt.Printf("[CPU] Target: %.1f%% duty cycle, ramp: %.1f%%/min\n\n", limitPct, speedPPM)

	// Duty ratio in basis points (0–10000 = 0%–100%)
	var dutyBPS atomic.Int64

	const window = 50 * time.Millisecond // duty-cycle period

	var wg sync.WaitGroup
	for i := 0; i < cores; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ratio := float64(dutyBPS.Load()) / 10000.0
				if ratio <= 0 {
					// Check context before sleeping
					select {
					case <-ctx.Done():
						return
					case <-time.After(window):
					}
					continue
				}

				busyDur := time.Duration(float64(window) * ratio)
				sleepDur := window - busyDur

				// Busy spin
				deadline := time.Now().Add(busyDur)
				for time.Now().Before(deadline) {
					_ = math.Sqrt(2.34567890) * math.Log(9.87654321)
				}

				if sleepDur > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(sleepDur):
					}
				} else {
					// Still check cancellation periodically
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
			}
		}()
	}

	// Ramp-up controller
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n[CPU] Stopping workers...")
			wg.Wait()
			return
		case <-ticker.C:
			elapsed := time.Since(start).Minutes()
			pct := math.Min(elapsed*speedPPM, limitPct)
			dutyBPS.Store(int64(pct / 100.0 * 10000))
			fmt.Printf("\r[CPU] duty=%.1f%%  elapsed=%.0fs", pct, time.Since(start).Seconds())
		}
	}
}

// ──────────────────────────── MEMORY ─────────────────────────────────────────

func runMem(ctx context.Context, limitPct, speedPPM float64) {
	freeMem := getFreeMem()
	targetBytes := uint64(float64(freeMem) * limitPct / 100.0)

	fmt.Printf("[MEM] Free memory : %s\n", humanBytes(freeMem))
	fmt.Printf("[MEM] Target alloc: %s (%.1f%% of free)\n", humanBytes(targetBytes), limitPct)
	fmt.Printf("[MEM] Ramp-up     : %.1f%%/min\n\n", speedPPM)

	const chunkSize = 1 << 20 // 1 MiB
	var (
		chunks    [][]byte
		allocated uint64
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n[MEM] Releasing memory...")
			chunks = nil
			runtime.GC()
			return
		case <-ticker.C:
			elapsed := time.Since(start).Minutes()
			pct := math.Min(elapsed*speedPPM, limitPct)
			wantBytes := uint64(float64(freeMem) * pct / 100.0)

			for allocated < wantBytes && allocated < targetBytes {
				need := wantBytes - allocated
				sz := uint64(chunkSize)
				if need < sz {
					sz = need
				}
				buf := make([]byte, sz)
				// Touch every page so the OS actually commits it
				for j := 0; j < len(buf); j += 4096 {
					buf[j] = 0xAB
				}
				chunks = append(chunks, buf)
				allocated += sz
			}

			fmt.Printf("\r[MEM] %s / %s  (%.1f%%)  elapsed=%.0fs",
				humanBytes(allocated), humanBytes(targetBytes), pct, time.Since(start).Seconds())
		}
	}
}

// ──────────────────────────── DISK ───────────────────────────────────────────

func runDisk(ctx context.Context, limitPct, speedPPM float64, dir string) {
	freeBytes := getFreeDisk(dir)
	targetBytes := uint64(float64(freeBytes) * limitPct / 100.0)
	filePath := filepath.Join(dir, fmt.Sprintf("loadgen_%d.tmp", os.Getpid()))

	fmt.Printf("[DISK] Free space  : %s\n", humanBytes(freeBytes))
	fmt.Printf("[DISK] Target fill : %s (%.1f%% of free)\n", humanBytes(targetBytes), limitPct)
	fmt.Printf("[DISK] Temp file   : %s\n", filePath)
	fmt.Printf("[DISK] Ramp-up     : %.1f%%/min\n\n", speedPPM)

	f, err := os.Create(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[DISK] Error: cannot create temp file: %v\n", err)
		return
	}
	defer func() {
		_ = f.Close()
		fmt.Printf("[DISK] Cleaning up %s ...\n", filePath)
		if err := os.Remove(filePath); err != nil {
			fmt.Fprintf(os.Stderr, "[DISK] Warning: cleanup failed: %v\n", err)
		} else {
			fmt.Println("[DISK] Cleanup complete.")
		}
	}()

	const chunkSize = 4 << 20 // 4 MiB
	chunk := make([]byte, chunkSize)
	for i := range chunk {
		chunk[i] = 0xCD
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	start := time.Now()
	var written uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(start).Minutes()
			pct := math.Min(elapsed*speedPPM, limitPct)
			wantBytes := uint64(float64(freeBytes) * pct / 100.0)

			for written < wantBytes && written < targetBytes {
				need := wantBytes - written
				sz := uint64(chunkSize)
				if need < sz {
					sz = need
				}
				n, err := f.Write(chunk[:sz])
				if err != nil {
					fmt.Fprintf(os.Stderr, "\n[DISK] Write error: %v\n", err)
					return
				}
				written += uint64(n)
			}
			_ = f.Sync()

			fmt.Printf("\r[DISK] %s / %s  (%.1f%%)  elapsed=%.0fs",
				humanBytes(written), humanBytes(targetBytes), pct, time.Since(start).Seconds())
		}
	}
}

// ──────────────────────────── System Info ─────────────────────────────────────

func getFreeMem() uint64 {
	switch runtime.GOOS {
	case "darwin":
		return darwinFreeMem()
	case "linux":
		return linuxFreeMem()
	default:
		fmt.Fprintf(os.Stderr, "[MEM] Unsupported OS %q, assuming 8 GiB free\n", runtime.GOOS)
		return 8 << 30
	}
}

func darwinFreeMem() uint64 {
	out, err := exec.Command("vm_stat").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "[MEM] Warning: vm_stat failed, falling back to sysctl")
		return darwinFreeMemFallback()
	}

	var pageSize uint64 = 4096
	if runtime.GOARCH == "arm64" {
		pageSize = 16384
	}

	var freePages, inactivePages, purgeable uint64
	for _, line := range splitLines(string(out)) {
		if n, ok := parseVMStatLine(line, "Pages free:"); ok {
			freePages = n
		}
		if n, ok := parseVMStatLine(line, "Pages inactive:"); ok {
			inactivePages = n
		}
		if n, ok := parseVMStatLine(line, "Pages purgeable:"); ok {
			purgeable = n
		}
	}

	free := (freePages + inactivePages + purgeable) * pageSize
	if free == 0 {
		return darwinFreeMemFallback()
	}
	return free
}

func darwinFreeMemFallback() uint64 {
	val, err := syscall.Sysctl("hw.memsize")
	if err != nil || len(val) < 8 {
		return 8 << 30
	}
	var total uint64
	for i := 0; i < 8 && i < len(val); i++ {
		total |= uint64(val[i]) << (i * 8)
	}
	return total / 2 // rough estimate
}

func linuxFreeMem() uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		fmt.Fprintln(os.Stderr, "[MEM] Warning: cannot read /proc/meminfo, assuming 8 GiB")
		return 8 << 30
	}
	lines := splitLines(string(data))
	// Prefer MemAvailable
	for _, line := range lines {
		if val, ok := parseMeminfoLine(line, "MemAvailable:"); ok {
			return val * 1024
		}
	}
	for _, line := range lines {
		if val, ok := parseMeminfoLine(line, "MemFree:"); ok {
			return val * 1024
		}
	}
	return 8 << 30
}

func parseMeminfoLine(line, prefix string) (uint64, bool) {
	if len(line) < len(prefix) || line[:len(prefix)] != prefix {
		return 0, false
	}
	return parseUintFromString(line[len(prefix):])
}

func parseVMStatLine(line, prefix string) (uint64, bool) {
	if len(line) < len(prefix) || line[:len(prefix)] != prefix {
		return 0, false
	}
	return parseUintFromString(line[len(prefix):])
}

func parseUintFromString(s string) (uint64, bool) {
	var num uint64
	found := false
	for _, c := range s {
		if c >= '0' && c <= '9' {
			num = num*10 + uint64(c-'0')
			found = true
		}
	}
	return num, found && num > 0
}

func getFreeDisk(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		fmt.Fprintf(os.Stderr, "[DISK] Warning: statfs(%q) failed: %v, assuming 10 GiB\n", path, err)
		return 10 << 30
	}
	return stat.Bavail * uint64(stat.Bsize)
}

// ──────────────────────────── Utilities ──────────────────────────────────────

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}