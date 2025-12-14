import networkx as nx
import time
import psutil
import os
import sys
from typing import List, Dict, Tuple, Optional
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.models.graph import FlightGraph


class PerformanceTester:

    def __init__(self, flight_graph: FlightGraph, sample_pairs: List[Tuple[str, str]]):
        self.flight_graph = flight_graph
        self.sample_pairs = sample_pairs
        self.process = psutil.Process(os.getpid())

    # =====================================================
    # 1. GRAPH BUILD METRICS
    # =====================================================
    @staticmethod
    def measure_graph_build(data_loader) -> Tuple[FlightGraph, Dict]:

        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / (1024 * 1024)

        start_time = datetime.now()
        print("-> Measuring Graph Build Time and Memory Usage...")

        airports_df, routes_df = data_loader.load_all()
        flight_graph = FlightGraph(airports_df, routes_df)

        end_time = datetime.now()
        mem_after = process.memory_info().rss / (1024 * 1024)

        metrics = {
            'graph_build_time_s': round((end_time - start_time).total_seconds(), 4),
            'peak_memory_mb': round(mem_after - mem_before, 2),
            'num_nodes': flight_graph.graph.number_of_nodes(),
            'num_edges': flight_graph.graph.number_of_edges()
        }

        print(f"\tGraph Build Time: {metrics['graph_build_time_s']} s")
        print(f"\tMemory Usage (Peak): {metrics['peak_memory_mb']} MB")

        return flight_graph, metrics

    # =====================================================
    # 2. LATENCY & THROUGHPUT
    # =====================================================
    def _run_algorithm(self, func, source, target, weight=None, heuristic=None):
        start = time.perf_counter_ns()
        try:
            if heuristic:
                func(self.flight_graph.graph, source, target,
                     heuristic=heuristic, weight=weight)
            else:
                func(self.flight_graph.graph, source, target, weight=weight)
            end = time.perf_counter_ns()
            return (end - start) / 1_000_000  # ms
        except nx.NetworkXNoPath:
            return None

    def measure_latency_and_throughput(self, algorithm: str, weight_type: Optional[str]) -> Dict:

        print(f"\n-> Measuring Latency/Throughput for {algorithm} ({weight_type})...")

        if algorithm == 'Dijkstra':
            func = nx.shortest_path
            heuristic = None
        elif algorithm == 'A*':
            func = nx.astar_path
            heuristic = lambda u, v: self.flight_graph._heuristic(u, v, weight_type)
        else:
            raise ValueError("Unsupported algorithm")

        weight = weight_type if weight_type else None

        times = []
        for src, dst in self.sample_pairs:
            t = self._run_algorithm(func, src, dst, weight, heuristic)
            if t is not None:
                times.append(t)

        avg_latency = sum(times) / len(times)
        throughput = len(times) / (sum(times) / 1000)

        metrics = {
            'latency_ms': round(avg_latency, 4),
            'throughput_qps': round(throughput, 2)
        }

        print(f"\tLatency: {metrics['latency_ms']} ms, Throughput: {metrics['throughput_qps']} qps")
        return metrics

    # =====================================================
    # 3. DIJKSTRA VS A* (RUNTIME ONLY)
    # =====================================================
    def compare_dijkstra_vs_astar(self, source: str, target: str) -> Dict:

        print(f"\n-> Comparing Dijkstra vs A* for path {source} -> {target}...")

        dijkstra_time = self._run_algorithm(
            nx.shortest_path, source, target, weight='distance'
        )

        astar_time = self._run_algorithm(
            nx.astar_path, source, target,
            weight='distance',
            heuristic=lambda u, v: self.flight_graph._heuristic(u, v, 'distance')
        )

        print(f"\tDijkstra Time: {round(dijkstra_time,4)} ms | A* Time: {round(astar_time,4)} ms")

        return {
            'source': source,
            'target': target,
            'dijkstra_time_ms': round(dijkstra_time, 4),
            'astar_time_ms': round(astar_time, 4)
        }

    # =====================================================
    # 4. HUB REMOVAL BASELINE (LWCC ONLY)
    # =====================================================
    def baseline_hub_removal(self, hubs: List[str]) -> Dict:

        original = self.flight_graph.graph
        temp = original.copy()

        for h in hubs:
            if h in temp:
                temp.remove_node(h)

        lwcc_before = len(max(nx.weakly_connected_components(original), key=len))
        lwcc_after = len(max(nx.weakly_connected_components(temp), key=len))

        return {
            'hubs_removed': len(hubs),
            'lwcc_before': lwcc_before,
            'lwcc_after': lwcc_after,
            'lwcc_reduction_percent': round(
                (lwcc_before - lwcc_after) / lwcc_before * 100, 2
            )
        }
