import os
import sys
import random
import pandas as pd
from datetime import datetime
import json
import networkx as nx

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'backend'))

from config import Config
from app.services.data_loader import DataLoader
from app.models.graph import FlightGraph
from app.services.performance_tester import PerformanceTester
from app.services.hub_analysis import HubAnalyzer


def run_all_benchmarks(num_samples: int = 200):

    print("====== FLIGHT ROUTE ADVISOR: BENCHMARK ======")
    start_suite = datetime.now()
    results = {}

    data_loader = DataLoader()

    # 1. Graph build
    graph, build_metrics = PerformanceTester.measure_graph_build(data_loader)
    results['Graph_Build'] = build_metrics

    # 2. LWCC
    lwcc_nodes = max(nx.weakly_connected_components(graph.graph), key=len)
    lwcc_size = len(lwcc_nodes)
    print(f"\nLWCC size: {lwcc_size} / {graph.graph.number_of_nodes()}")

    results['LWCC'] = {
        'lwcc_size': lwcc_size,
        'total_nodes': graph.graph.number_of_nodes()
    }

    # 3. Sample pairs (chỉ trong LWCC)
    lwcc_list = list(lwcc_nodes)
    pairs = [
        tuple(random.sample(lwcc_list, 2))
        for _ in range(num_samples)
    ]

    tester = PerformanceTester(graph, pairs)

    # 4. Latency & Throughput
    results['Latency_Dijkstra_Distance'] = tester.measure_latency_and_throughput(
        'Dijkstra', 'distance'
    )

    results['Latency_AStar_Distance'] = tester.measure_latency_and_throughput(
        'A*', 'distance'
    )

    # 5. Runtime comparison
    src, dst = random.choice(pairs)
    results['Dijkstra_vs_AStar'] = tester.compare_dijkstra_vs_astar(src, dst)

    src, dst = random.choice(pairs)
    results['Dijkstra_vs_AStar'] = tester.compare_dijkstra_vs_astar(src, dst)

    src, dst = random.choice(pairs)
    results['Dijkstra_vs_AStar'] = tester.compare_dijkstra_vs_astar(src, dst)

    src, dst = random.choice(pairs)
    results['Dijkstra_vs_AStar'] = tester.compare_dijkstra_vs_astar(src, dst)

    # 6. Hub robustness baseline
    hub_analyzer = HubAnalyzer(graph)
    print("\nCalculating centrality metrics...")
    
    TOP_K_PRINT = 5
    
    print("\n--- 6. CENTRALITY METRICS RESULTS (TOP 5) ---")

    top_degree = hub_analyzer.get_top_hubs(metric='degree', top_k=TOP_K_PRINT)
    print(f"\nTop {TOP_K_PRINT} Hubs by Degree Centrality:")
    top_degree_df = pd.DataFrame(top_degree)
    print(top_degree_df[['iata', 'name', 'country', 'total_degree']].to_markdown(index=False))

    top_hubs = hub_analyzer.get_top_hubs(metric='betweenness', top_k=TOP_K_PRINT)
    print(f"\nTop {TOP_K_PRINT} Hubs by Betweenness Centrality:")
    top_betweenness_df = pd.DataFrame(top_hubs)
    print(top_betweenness_df[['iata', 'name', 'country', 'betweenness_centrality']].to_markdown(index=False))
    hub_iata = [h['iata'] for h in top_hubs]

    top_closeness = hub_analyzer.get_top_hubs(metric='closeness', top_k=TOP_K_PRINT)
    print(f"\nTop {TOP_K_PRINT} Hubs by Closeness Centrality:")
    top_closeness_df = pd.DataFrame(top_closeness)
    print(top_closeness_df[['iata', 'name', 'country', 'closeness_centrality']].to_markdown(index=False))

    top_pagerank = hub_analyzer.get_top_hubs(metric='pagerank', top_k=TOP_K_PRINT)
    print(f"\nTop {TOP_K_PRINT} Hubs by PageRank:")
    top_pagerank_df = pd.DataFrame(top_pagerank)
    print(top_pagerank_df[['iata', 'name', 'country', 'pagerank']].to_markdown(index=False))

    results['Hub_Robustness'] = tester.baseline_hub_removal(hub_iata)
    print("\nNetwork Robustness Baseline (after removing top 5 betweenness hubs) calculated.")
    results['Total_Runtime'] = str(datetime.now() - start_suite)
    
    print("\nBenchmark finished.")
    return results

if __name__ == "__main__":
    num_samples_to_run = 200
    final_results = run_all_benchmarks(num_samples=num_samples_to_run)