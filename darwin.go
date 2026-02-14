// +build darwin freebsd openbsd
package main

import "syscall"

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
