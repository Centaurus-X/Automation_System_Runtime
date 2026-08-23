# -*- coding: utf-8 -*-

# /tools/gil-free_benchmark.py


"""
Benchmark
"""

import os, time, sys
from functools import partial
from concurrent.futures import ThreadPoolExecutor

def burn(iterations, seed):
    v = seed & 0xFFFFFFFF
    i = 0
    while i < iterations:
        v = (v * 1664525 + 1013904223) & 0xFFFFFFFF
        v ^= (v >> 13)
        v = (v * 1103515245 + 12345) & 0xFFFFFFFF
        i += 1
    return v

def _unknown_gil_state():
    return None

def bench(work, threads, tasks):
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        list(ex.map(work, range(tasks)))
    return time.perf_counter() - t0

def main():
    iterations = 500_000
    threads = max(2, os.cpu_count() or 2)
    tasks = threads  # eine Aufgabe pro Thread
    work = partial(burn, iterations)

    one = bench(work, 1, tasks)
    many = bench(work, threads, tasks)

    gil_flag = getattr(sys, "_is_gil_enabled", _unknown_gil_state)()
    print(f"gil_enabled={gil_flag}")
    print(f"1 Thread: {one:.3f}s | {threads} Threads: {many:.3f}s | Speedup: {one/many:.2f}×")

if __name__ == "__main__":
    main()
