import networkx as nx
import pandas as pd
from geopy.distance import geodesic
from typing import List, Dict, Tuple, Optional
import sys
sys.path.append('..')
from config import Config

class FlightGraph:
    """Graph representation of flight network using NetworkX"""
    
    def __init__(self, airports_df: pd.DataFrame, routes_df: pd.DataFrame):
        """
        Initialize flight graph
        
        Args:
            airports_df: DataFrame with airport information
            routes_df: DataFrame with route information
        """
        self.airports_df = airports_df
        self.routes_df = routes_df
        self.graph = nx.DiGraph()
        self._build_graph()
    
    def _calculate_distance(self, src_lat: float, src_lon: float, 
                           dst_lat: float, dst_lon: float) -> float:
        """Calculate great circle distance between two points in kilometers"""
        return geodesic((src_lat, src_lon), (dst_lat, dst_lon)).kilometers
    
    def _build_graph(self):
        """Build the flight network graph with airports as nodes and routes as edges"""
        print("Building graph nodes (airports)...")
        
        # Add airports as nodes
        for _, airport in self.airports_df.iterrows():
            self.graph.add_node(
                airport['iata'],
                name=airport['name'],
                city=airport['city'],
                country=airport['country'],
                latitude=airport['latitude'],
                longitude=airport['longitude'],
                altitude=airport['altitude'],
                type='airport'
            )
        
        print(f"Added {self.graph.number_of_nodes()} airport nodes")
        print("Building graph edges (routes)...")
        
        # Get valid IATA codes
        valid_iata_codes = set(self.airports_df['iata'].values)
        
        # Add routes as edges
        edge_count = 0
        for _, route in self.routes_df.iterrows():
            src = route['source_airport']
            dst = route['destination_airport']
            
            # Skip if airports not in our dataset
            if src not in valid_iata_codes or dst not in valid_iata_codes:
                continue
            
            # Get airport coordinates
            src_airport = self.airports_df[self.airports_df['iata'] == src].iloc[0]
            dst_airport = self.airports_df[self.airports_df['iata'] == dst].iloc[0]
            
            # Calculate distance
            distance = self._calculate_distance(
                src_airport['latitude'], src_airport['longitude'],
                dst_airport['latitude'], dst_airport['longitude']
            )
            
            # Calculate costs
            # Assume average speed of 800 km/h
            time_cost = distance / 800.0
            monetary_cost = distance * Config.BASE_COST_PER_KM
            
            # Add edge (if multiple routes exist, keep the one with lower cost)
            if self.graph.has_edge(src, dst):
                existing_cost = self.graph[src][dst]['monetary_cost']
                if monetary_cost < existing_cost:
                    self.graph[src][dst].update({
                        'distance': distance,
                        'time_cost': time_cost,
                        'monetary_cost': monetary_cost,
                        'airline': route['airline']
                    })
            else:
                self.graph.add_edge(
                    src, dst,
                    distance=distance,
                    time_cost=time_cost,
                    monetary_cost=monetary_cost,
                    airline=route['airline']
                )
                edge_count += 1
        
        print(f"Added {self.graph.number_of_edges()} route edges")
    
    def get_transfer_time(self, airport_code: str, is_international: bool = False) -> float:
        """
        Get transfer time for an airport in hours
        
        Args:
            airport_code: IATA airport code
            is_international: Whether it's an international transfer
            
        Returns:
            Transfer time in hours
        """
        if is_international:
            return Config.INTERNATIONAL_TRANSFER_TIME / 60.0
        return Config.DEFAULT_TRANSFER_TIME / 60.0
    
    def find_shortest_path(
        self, 
        source: str, 
        target: str, 
        cost_type: str = 'time',
        max_stops: Optional[int] = None
    ) -> Optional[Dict]:
        """
        Find shortest path between two airports using Dijkstra's algorithm
        
        Args:
            source: Source airport IATA code
            target: Target airport IATA code
            cost_type: 'time', 'distance', or 'cost'
            max_stops: Maximum number of stops allowed (None for no limit)
        
        Returns:
            Dictionary with path details or None if no path exists
        """
        if source not in self.graph or target not in self.graph:
            return None
        
        # Map cost type to edge attribute
        weight_map = {
            'time': 'time_cost',
            'distance': 'distance',
            'cost': 'monetary_cost'
        }
        weight = weight_map.get(cost_type, 'time_cost')
        
        try:
            # Find shortest path
            path = nx.shortest_path(self.graph, source, target, weight=weight)
            
            # Check max stops constraint
            num_stops = len(path) - 2
            if max_stops is not None and num_stops > max_stops:
                return None
            
            # Calculate path metrics
            return self._calculate_path_metrics(path)
        
        except nx.NetworkXNoPath:
            return None
    
    def find_k_shortest_paths(
        self,
        source: str,
        target: str,
        k: int = 5,
        cost_type: str = 'time'
    ) -> List[Dict]:
        """
        Find k shortest paths between two airports
        
        Args:
            source: Source airport IATA code
            target: Target airport IATA code
            k: Number of paths to find
            cost_type: 'time', 'distance', or 'cost'
            
        Returns:
            List of path dictionaries
        """
        if source not in self.graph or target not in self.graph:
            return []
        
        weight_map = {
            'time': 'time_cost',
            'distance': 'distance',
            'cost': 'monetary_cost'
        }
        weight = weight_map.get(cost_type, 'time_cost')
        
        try:
            paths = []
            # Use NetworkX's shortest_simple_paths generator
            for path in nx.shortest_simple_paths(self.graph, source, target, weight=weight):
                # Check if path exceeds max stops
                if len(path) - 2 > Config.MAX_STOPS:
                    continue
                
                # Calculate metrics for this path
                path_metrics = self._calculate_path_metrics(path)
                if path_metrics:
                    paths.append(path_metrics)
                
                # Stop when we have k paths
                if len(paths) >= k:
                    break
            
            return paths
        
        except nx.NetworkXNoPath:
            return []
    
    def _calculate_path_metrics(self, path: List[str]) -> Dict:
        """
        Calculate metrics for a given path
        
        Args:
            path: List of airport IATA codes
            
        Returns:
            Dictionary with path metrics
        """
        total_distance = 0
        total_time = 0
        total_cost = 0
        segments = []
        
        # Calculate metrics for each segment
        for i in range(len(path) - 1):
            edge_data = self.graph[path[i]][path[i + 1]]
            total_distance += edge_data['distance']
            total_time += edge_data['time_cost']
            total_cost += edge_data['monetary_cost']
            
            segments.append({
                'from': path[i],
                'to': path[i + 1],
                'distance': round(edge_data['distance'], 2),
                'time': round(edge_data['time_cost'], 2),
                'cost': round(edge_data['monetary_cost'], 2),
                'airline': edge_data.get('airline', 'N/A')
            })
        
        # Add transfer times
        transfer_time = 0
        if len(path) > 2:  # Has transfers
            for i in range(1, len(path) - 1):
                transfer_time += self.get_transfer_time(path[i])
        
        return {
            'path': path,
            'segments': segments,
            'stops': len(path) - 2,
            'total_distance': round(total_distance, 2),
            'total_flight_time': round(total_time, 2),
            'total_transfer_time': round(transfer_time, 2),
            'total_time': round(total_time + transfer_time, 2),
            'total_cost': round(total_cost, 2)
        }
    
    def get_graph_stats(self) -> Dict:
        """Get graph statistics"""
        return {
            'num_airports': self.graph.number_of_nodes(),
            'num_routes': self.graph.number_of_edges(),
            'avg_degree': round(sum(dict(self.graph.degree()).values()) / self.graph.number_of_nodes(), 2),
            'is_connected': nx.is_weakly_connected(self.graph),
            'num_components': nx.number_weakly_connected_components(self.graph)
        }
    
    def export_to_gexf(self, filepath: str):
        """Export graph to GEXF format for Gephi visualization"""
        nx.write_gexf(self.graph, filepath)
        print(f"Graph exported to: {filepath}")