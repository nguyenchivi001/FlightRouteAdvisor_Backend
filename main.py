from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
import uvicorn
from pathlib import Path

from config import Config
from app.services.data_loader import DataLoader
from app.models.graph import FlightGraph
from app.services.hub_analysis import HubAnalyzer

# Ensure directories exist
Config.ensure_directories()

# Validate data files
if not Config.validate_data_files():
    print("\nERROR: Required data files are missing!")
    print("Please download data files from https://openflights.org/data.html")
    print("and place them in the backend/data/ folder.\n")
    exit(1)

# Initialize FastAPI app
app = FastAPI(
    title=Config.API_TITLE,
    description=Config.API_DESCRIPTION,
    version=Config.API_VERSION
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
data_loader = DataLoader()
flight_graph = None
hub_analyzer = None

# Pydantic models for request/response
class RouteRequest(BaseModel):
    source: str = Field(..., description="Source airport IATA code", min_length=3, max_length=3)
    destination: str = Field(..., description="Destination airport IATA code", min_length=3, max_length=3)
    cost_type: str = Field("time", description="Optimization metric: time, distance, or cost")
    max_stops: Optional[int] = Field(None, description="Maximum number of stops", ge=0, le=5)
    k_paths: int = Field(5, description="Number of alternative routes", ge=1, le=10)

class HubRemovalRequest(BaseModel):
    source: str = Field(..., min_length=3, max_length=3)
    destination: str = Field(..., min_length=3, max_length=3)
    hubs_to_remove: List[str] = Field(..., description="List of hub IATA codes to remove")

class AlternativeHubsRequest(BaseModel):
    source: str = Field(..., min_length=3, max_length=3)
    destination: str = Field(..., min_length=3, max_length=3)
    primary_hub: str = Field(..., description="Primary hub to avoid", min_length=3, max_length=3)
    top_k: int = Field(5, description="Number of alternatives", ge=1, le=20)

@app.on_event("startup")
async def startup_event():
    """Initialize data on startup"""
    global flight_graph, hub_analyzer
    
    print("\n" + "="*80)
    print("FLIGHT ROUTE ADVISOR - STARTING UP")
    print("="*80)
    
    print("\n[1/4] Loading OpenFlights data...")
    airports_df, routes_df, airlines_df = data_loader.load_all()
    print(f"      ✓ Loaded {len(airports_df)} airports")
    print(f"      ✓ Loaded {len(routes_df)} routes")
    print(f"      ✓ Loaded {len(airlines_df)} airlines")
    
    print("\n[2/4] Building flight network graph...")
    flight_graph = FlightGraph(airports_df, routes_df)
    
    print("\n[3/4] Initializing hub analyzer...")
    hub_analyzer = HubAnalyzer(flight_graph)
    
    stats = flight_graph.get_graph_stats()
    print("\n[4/4] Graph Statistics:")
    print(f"      • Airports (nodes): {stats['num_airports']}")
    print(f"      • Routes (edges): {stats['num_routes']}")
    print(f"      • Average degree: {stats['avg_degree']}")
    print(f"      • Is connected: {stats['is_connected']}")
    print(f"      • Components: {stats['num_components']}")
    
    # Export to GEXF for Gephi
    print("\n[5/5] Exporting graph to GEXF format...")
    gexf_file = Config.GEPHI_DIR / "flight_network.gexf"
    flight_graph.export_to_gexf(str(gexf_file))
    
    print("\n" + "="*80)
    print(f"✓ API READY - Listening on http://{Config.HOST}:{Config.PORT}")
    print(f"✓ API Docs available at http://localhost:{Config.PORT}/docs")
    print("="*80 + "\n")

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "name": Config.API_TITLE,
        "version": Config.API_VERSION,
        "description": Config.API_DESCRIPTION,
        "endpoints": {
            "documentation": "/docs",
            "search_airports": "/airports/search?q=<query>",
            "airport_info": "/airports/{iata}",
            "find_route": "/routes/find",
            "alternative_routes": "/routes/alternatives",
            "top_hubs": "/hubs/top",
            "hub_info": "/hubs/{iata}",
            "hub_removal_analysis": "/hubs/removal-analysis",
            "alternative_hubs": "/hubs/alternatives",
            "graph_stats": "/graph/stats"
        }
    }

@app.get("/airports/search")
async def search_airports(q: str = Query(..., min_length=1, description="Search query")):
    """Search for airports by name, city, country, or IATA code"""
    results = data_loader.search_airports(q, limit=20)
    return {
        "query": q,
        "results": results,
        "count": len(results)
    }

@app.get("/airports/{iata}")
async def get_airport_info(iata: str):
    """Get detailed information about an airport"""
    iata = iata.upper()
    airport = data_loader.get_airport_by_iata(iata)
    
    if not airport:
        raise HTTPException(status_code=404, detail=f"Airport {iata} not found")
    
    # Get connectivity info
    connectivity = hub_analyzer.get_hub_connectivity(iata)
    
    return {
        "airport": airport,
        "connectivity": connectivity
    }

@app.post("/routes/find")
async def find_route(request: RouteRequest):
    """Find the shortest route between two airports"""
    source = request.source.upper()
    destination = request.destination.upper()
    
    if source not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Source airport {source} not found")
    
    if destination not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Destination airport {destination} not found")
    
    if source == destination:
        raise HTTPException(status_code=400, detail="Source and destination cannot be the same")
    
    route = flight_graph.find_shortest_path(
        source, 
        destination, 
        cost_type=request.cost_type,
        max_stops=request.max_stops
    )
    
    if not route:
        raise HTTPException(
            status_code=404, 
            detail=f"No route found from {source} to {destination}"
        )
    
    # Add airport details to path
    path_with_details = []
    for iata in route['path']:
        airport = data_loader.get_airport_by_iata(iata)
        if airport:
            path_with_details.append({
                'iata': iata,
                'name': airport['name'],
                'city': airport['city'],
                'country': airport['country'],
                'latitude': airport['latitude'],
                'longitude': airport['longitude']
            })
    
    route['path_details'] = path_with_details
    
    return route

@app.post("/routes/alternatives")
async def find_alternative_routes(request: RouteRequest):
    """Find k alternative routes between two airports"""
    source = request.source.upper()
    destination = request.destination.upper()
    
    if source not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Source airport {source} not found")
    
    if destination not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Destination airport {destination} not found")
    
    if source == destination:
        raise HTTPException(status_code=400, detail="Source and destination cannot be the same")
    
    routes = flight_graph.find_k_shortest_paths(
        source,
        destination,
        k=request.k_paths,
        cost_type=request.cost_type
    )
    
    if not routes:
        raise HTTPException(
            status_code=404,
            detail=f"No routes found from {source} to {destination}"
        )
    
    # Add airport details to each route
    for route in routes:
        path_with_details = []
        for iata in route['path']:
            airport = data_loader.get_airport_by_iata(iata)
            if airport:
                path_with_details.append({
                    'iata': iata,
                    'name': airport['name'],
                    'city': airport['city'],
                    'country': airport['country'],
                    'latitude': airport['latitude'],
                    'longitude': airport['longitude']
                })
        route['path_details'] = path_with_details
    
    return {
        "source": source,
        "destination": destination,
        "cost_type": request.cost_type,
        "routes": routes,
        "count": len(routes)
    }

@app.get("/hubs/top")
async def get_top_hubs(
    metric: str = Query("degree", regex="^(degree|betweenness|closeness|pagerank)$"),
    top_k: int = Query(20, ge=1, le=100)
):
    """Get top hub airports based on centrality metrics"""
    hubs = hub_analyzer.get_top_hubs(top_k=top_k, metric=metric)
    return {
        "metric": metric,
        "top_k": top_k,
        "hubs": hubs
    }

@app.get("/hubs/{iata}")
async def get_hub_info(iata: str):
    """Get detailed hub information including connectivity and centrality"""
    iata = iata.upper()
    
    if iata not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Airport {iata} not found")
    
    connectivity = hub_analyzer.get_hub_connectivity(iata)
    
    # Get centrality metrics
    all_centrality = hub_analyzer.calculate_centrality_metrics()
    centrality = all_centrality.get(iata, {})
    
    return {
        "connectivity": connectivity,
        "centrality": centrality
    }

@app.post("/hubs/removal-analysis")
async def analyze_hub_removal(request: HubRemovalRequest):
    """Analyze the impact of removing hub airports on a route (What-If Analysis)"""
    source = request.source.upper()
    destination = request.destination.upper()
    hubs = [h.upper() for h in request.hubs_to_remove]
    
    if source not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Source airport {source} not found")
    
    if destination not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Destination airport {destination} not found")
    
    if source == destination:
        raise HTTPException(status_code=400, detail="Source and destination cannot be the same")
    
    analysis = hub_analyzer.analyze_hub_removal(source, destination, hubs)
    
    # Add airport details
    for route_key in ['original_path', 'alternative_path']:
        if analysis.get(route_key) and 'path' in analysis[route_key]:
            path_with_details = []
            for iata in analysis[route_key]['path']:
                airport = data_loader.get_airport_by_iata(iata)
                if airport:
                    path_with_details.append({
                        'iata': iata,
                        'name': airport['name'],
                        'city': airport['city'],
                        'country': airport['country']
                    })
            analysis[route_key]['path_details'] = path_with_details
    
    return analysis

@app.post("/hubs/alternatives")
async def find_alternative_hubs(request: AlternativeHubsRequest):
    """Find alternative hub airports for a route"""
    source = request.source.upper()
    destination = request.destination.upper()
    primary_hub = request.primary_hub.upper()
    
    if source not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Source airport {source} not found")
    
    if destination not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Destination airport {destination} not found")
    
    if primary_hub not in flight_graph.graph:
        raise HTTPException(status_code=404, detail=f"Primary hub {primary_hub} not found")
    
    alternatives = hub_analyzer.find_alternative_hubs(
        source, destination, primary_hub, top_k=request.top_k
    )
    
    return {
        "source": source,
        "destination": destination,
        "primary_hub": primary_hub,
        "alternatives": alternatives,
        "count": len(alternatives)
    }

@app.get("/graph/stats")
async def get_graph_stats():
    """Get graph statistics"""
    stats = flight_graph.get_graph_stats()
    return stats

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": Config.API_VERSION
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=Config.HOST,
        port=Config.PORT,
        reload=Config.RELOAD
    )