import networkx as nx
from typing import Dict, List, Optional
import sys
sys.path.append('../..')
from config import Config

class HubAnalyzer:
    """Analyze hub airports and their importance in the network"""
    
    def __init__(self, flight_graph):
        """
        Initialize hub analyzer
        
        Args:
            flight_graph: FlightGraph instance
        """
        self.graph = flight_graph.graph
        self.flight_graph = flight_graph
    
    def calculate_centrality_metrics(self) -> Dict[str, Dict]:
        """
        Calculate various centrality metrics for all airports
        
        Returns:
            Dictionary mapping airport codes to their centrality metrics
        """
        print("Calculating centrality metrics...")
        
        # Degree centrality (number of connections)
        print("  - Degree centrality...")
        degree_centrality = nx.degree_centrality(self.graph)
        
        # Betweenness centrality (how often airport is on shortest paths)
        print("  - Betweenness centrality...")
        betweenness_centrality = nx.betweenness_centrality(
            self.graph, 
            weight='time_cost',
            normalized=True
        )
        
        # Closeness centrality (average distance to all other airports)
        print("  - Closeness centrality...")
        try:
            closeness_centrality = nx.closeness_centrality(
                self.graph,
                distance='time_cost'
            )
        except:
            # If graph is not strongly connected, use empty dict
            closeness_centrality = {}
        
        # PageRank (importance based on connections)
        print("  - PageRank...")
        pagerank = nx.pagerank(self.graph, weight='time_cost')
        
        # Combine all metrics
        all_airports = set(self.graph.nodes())
        centrality_data = {}
        
        for airport in all_airports:
            airport_info = self.graph.nodes[airport]
            centrality_data[airport] = {
                'iata': airport,
                'name': airport_info.get('name', ''),
                'city': airport_info.get('city', ''),
                'country': airport_info.get('country', ''),
                'degree_centrality': round(degree_centrality.get(airport, 0), 6),
                'betweenness_centrality': round(betweenness_centrality.get(airport, 0), 6),
                'closeness_centrality': round(closeness_centrality.get(airport, 0), 6),
                'pagerank': round(pagerank.get(airport, 0), 6),
                'in_degree': self.graph.in_degree(airport),
                'out_degree': self.graph.out_degree(airport),
                'total_degree': self.graph.degree(airport)
            }
        
        print("Centrality metrics calculated successfully")
        return centrality_data
    
    def get_top_hubs(self, top_k: int = 20, metric: str = 'degree') -> List[Dict]:
        """
        Get top hub airports based on specified metric
        
        Args:
            top_k: Number of top hubs to return
            metric: 'degree', 'betweenness', 'closeness', or 'pagerank'
            
        Returns:
            List of top hub dictionaries sorted by metric
        """
        centrality_data = self.calculate_centrality_metrics()
        
        metric_map = {
            'degree': 'degree_centrality',
            'betweenness': 'betweenness_centrality',
            'closeness': 'closeness_centrality',
            'pagerank': 'pagerank'
        }
        
        sort_key = metric_map.get(metric, 'degree_centrality')
        
        sorted_hubs = sorted(
            centrality_data.values(),
            key=lambda x: x[sort_key],
            reverse=True
        )
        
        return sorted_hubs[:top_k]
    
    def analyze_hub_removal(
        self,
        source: str,
        target: str,
        hubs_to_remove: List[str]
    ) -> Dict:
        """
        Analyze impact of removing hub airports on a route (What-If analysis)
        
        Args:
            source: Source airport IATA code
            target: Target airport IATA code
            hubs_to_remove: List of hub airport IATA codes to remove
        
        Returns:
            Analysis results with original and alternative paths
        """
        # Find original shortest path
        original_path = self.flight_graph.find_shortest_path(source, target)
        
        # Create a copy of the graph
        temp_graph = self.graph.copy()
        
        # Remove specified hubs (but not source or destination)
        for hub in hubs_to_remove:
            if hub in temp_graph and hub != source and hub != target:
                temp_graph.remove_node(hub)
        
        # Find alternative path without the hubs
        alternative_path = None
        try:
            alt_path_nodes = nx.shortest_path(
                temp_graph, source, target, weight='time_cost'
            )
            
            temp_fg = self.flight_graph
            
            original_graph = temp_fg.graph
            temp_fg.graph = temp_graph
            
            alternative_path = temp_fg._calculate_path_metrics(alt_path_nodes)
            
            temp_fg.graph = original_graph
        
        except nx.NetworkXNoPath:
            alternative_path = None
        
        # Calculate impact
        impact = {
            'removed_hubs': hubs_to_remove,
            'original_path': original_path,
            'alternative_path': alternative_path,
            'path_exists': alternative_path is not None
        }
        
        # Calculate differences
        if original_path and alternative_path:
            impact['time_increase'] = round(
                alternative_path['total_time'] - original_path['total_time'], 2
            )
            impact['cost_increase'] = round(
                alternative_path['total_cost'] - original_path['total_cost'], 2
            )
            impact['distance_increase'] = round(
                alternative_path['total_distance'] - original_path['total_distance'], 2
            )
            impact['stops_increase'] = alternative_path['stops'] - original_path['stops']
        
        return impact
    
    def find_alternative_hubs(
        self,
        source: str,
        target: str,
        primary_hub: str,
        top_k: int = 5
    ) -> List[Dict]:
        """
        Find alternative hub airports for a route
        
        Args:
            source: Source airport
            target: Target airport
            primary_hub: Primary hub to avoid
            top_k: Number of alternatives to return
        
        Returns:
            List of alternative hub options
        """
        alternatives = []
        country_source = self.flight_graph._get_country_by_iata(source)
        country_target = self.flight_graph._get_country_by_iata(target)
        
        # Get all possible 2-stop paths (source -> hub -> target)
        for potential_hub in self.graph.nodes():
            # Skip if it's source, destination, or the primary hub
            if potential_hub in [source, target, primary_hub]:
                continue
            
            # Check if hub is connected to both source and target
            if not (self.graph.has_edge(source, potential_hub) and 
                    self.graph.has_edge(potential_hub, target)):
                continue
            
            country_hub = self.flight_graph._get_country_by_iata(potential_hub)
            
            is_international = False
            if country_source and country_hub and country_target:
                if country_source != country_hub or country_hub != country_target:
                    is_international = True
            
            transfer_time_hub = self.flight_graph.get_transfer_time(
                potential_hub, 
                is_international=is_international
            )

            # Calculate total cost via this hub
            src_to_hub = self.graph[source][potential_hub]
            hub_to_dst = self.graph[potential_hub][target]
            
            total_time = (
                src_to_hub['time_cost'] + 
                hub_to_dst['time_cost'] + 
                transfer_time_hub
                )
            total_distance = src_to_hub['distance'] + hub_to_dst['distance']
            total_cost = src_to_hub['monetary_cost'] + hub_to_dst['monetary_cost']
            
            hub_info = self.graph.nodes[potential_hub]
            
            alternatives.append({
                'hub': potential_hub,
                'hub_name': hub_info.get('name', ''),
                'hub_city': hub_info.get('city', ''),
                'hub_country': hub_info.get('country', ''),
                'is_international_transfer': is_international,
                'total_time': round(total_time, 2),
                'total_distance': round(total_distance, 2),
                'total_cost': round(total_cost, 2),
                'transfer_time': round(transfer_time_hub, 2),
                'segments': [
                    {
                        'from': source,
                        'to': potential_hub,
                        'distance': round(src_to_hub['distance'], 2),
                        'time': round(src_to_hub['time_cost'], 2),
                        'cost': round(src_to_hub['monetary_cost'], 2)
                    },
                    {
                        'from': potential_hub,
                        'to': target,
                        'distance': round(hub_to_dst['distance'], 2),
                        'time': round(hub_to_dst['time_cost'], 2),
                        'cost': round(hub_to_dst['monetary_cost'], 2)
                    }
                ]
            })
        
        # Sort by total time and return top k
        alternatives.sort(key=lambda x: x['total_time'])
        return alternatives[:top_k]
    
    def get_hub_connectivity(self, hub: str) -> Optional[Dict]:
        """
        Get detailed connectivity information for a hub
        
        Args:
            hub: Airport IATA code
            
        Returns:
            Hub connectivity information
        """
        if hub not in self.graph:
            return None
        
        hub_info = self.graph.nodes[hub]
        
        # Get all connections
        destinations = list(self.graph.successors(hub))
        origins = list(self.graph.predecessors(hub))
        
        # Get countries served
        countries = set()
        for dest in destinations:
            countries.add(self.graph.nodes[dest]['country'])
        for origin in origins:
            countries.add(self.graph.nodes[origin]['country'])
        
        return {
            'iata': hub,
            'name': hub_info.get('name', ''),
            'city': hub_info.get('city', ''),
            'country': hub_info.get('country', ''),
            'num_destinations': len(destinations),
            'num_origins': len(origins),
            'total_connections': len(set(destinations) | set(origins)),
            'countries_served': len(countries),
            'top_destinations': destinations[:10],
            'top_origins': origins[:10]
        }