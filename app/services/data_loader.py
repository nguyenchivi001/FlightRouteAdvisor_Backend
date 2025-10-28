import pandas as pd
from pathlib import Path
from typing import Tuple, List, Dict, Optional
import sys
sys.path.append('../..')
from config import Config

class DataLoader:
    """Load and process OpenFlights data from local files"""
    
    def __init__(self):
        self.airports_df: Optional[pd.DataFrame] = None
        self.routes_df: Optional[pd.DataFrame] = None
        self.airlines_df: Optional[pd.DataFrame] = None
    
    def load_airports(self) -> pd.DataFrame:
        """Load airports data from local file"""
        if self.airports_df is not None:
            return self.airports_df
        
        if not Config.AIRPORTS_FILE.exists():
            raise FileNotFoundError(
                f"Airports file not found at {Config.AIRPORTS_FILE}. "
                f"Please download from https://openflights.org/data.html"
            )
        
        columns = [
            'airport_id', 'name', 'city', 'country', 'iata', 'icao',
            'latitude', 'longitude', 'altitude', 'timezone', 'dst',
            'tz_database', 'type', 'source'
        ]
        
        print(f"Loading airports from {Config.AIRPORTS_FILE}...")
        self.airports_df = pd.read_csv(
            Config.AIRPORTS_FILE,
            names=columns,
            na_values=['\\N'],
            encoding='utf-8',
            on_bad_lines='skip'
        )
        
        # Filter out airports without IATA codes
        self.airports_df = self.airports_df[
            self.airports_df['iata'].notna() & 
            (self.airports_df['iata'] != '\\N') &
            (self.airports_df['iata'].str.len() == 3)
        ].copy()
        
        # Convert to uppercase
        self.airports_df['iata'] = self.airports_df['iata'].str.upper()
        
        print(f"Loaded {len(self.airports_df)} airports with valid IATA codes")
        return self.airports_df
    
    def load_routes(self) -> pd.DataFrame:
        """Load routes data from local file"""
        if self.routes_df is not None:
            return self.routes_df
        
        if not Config.ROUTES_FILE.exists():
            raise FileNotFoundError(
                f"Routes file not found at {Config.ROUTES_FILE}. "
                f"Please download from https://openflights.org/data.html"
            )
        
        columns = [
            'airline', 'airline_id', 'source_airport', 'source_airport_id',
            'destination_airport', 'destination_airport_id', 'codeshare',
            'stops', 'equipment'
        ]
        
        print(f"Loading routes from {Config.ROUTES_FILE}...")
        self.routes_df = pd.read_csv(
            Config.ROUTES_FILE,
            names=columns,
            na_values=['\\N'],
            encoding='utf-8',
            on_bad_lines='skip'
        )
        
        # Filter direct flights only (stops = 0)
        self.routes_df = self.routes_df[self.routes_df['stops'] == 0].copy()
        
        # Filter routes with valid IATA codes
        self.routes_df = self.routes_df[
            (self.routes_df['source_airport'].notna()) &
            (self.routes_df['destination_airport'].notna()) &
            (self.routes_df['source_airport'] != '\\N') &
            (self.routes_df['destination_airport'] != '\\N') &
            (self.routes_df['source_airport'].str.len() == 3) &
            (self.routes_df['destination_airport'].str.len() == 3)
        ].copy()
        
        # Convert to uppercase
        self.routes_df['source_airport'] = self.routes_df['source_airport'].str.upper()
        self.routes_df['destination_airport'] = self.routes_df['destination_airport'].str.upper()
        
        print(f"Loaded {len(self.routes_df)} direct routes")
        return self.routes_df
    
    def load_airlines(self) -> pd.DataFrame:
        """Load airlines data from local file"""
        if self.airlines_df is not None:
            return self.airlines_df
        
        if not Config.AIRLINES_FILE.exists():
            raise FileNotFoundError(
                f"Airlines file not found at {Config.AIRLINES_FILE}. "
                f"Please download from https://openflights.org/data.html"
            )
        
        columns = [
            'airline_id', 'name', 'alias', 'iata', 'icao',
            'callsign', 'country', 'active'
        ]
        
        print(f"Loading airlines from {Config.AIRLINES_FILE}...")
        self.airlines_df = pd.read_csv(
            Config.AIRLINES_FILE,
            names=columns,
            na_values=['\\N'],
            encoding='utf-8',
            on_bad_lines='skip'
        )
        
        print(f"Loaded {len(self.airlines_df)} airlines")
        return self.airlines_df
    
    def load_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all data from local files"""
        airports = self.load_airports()
        routes = self.load_routes()
        airlines = self.load_airlines()
        return airports, routes, airlines
    
    def get_airport_by_iata(self, iata: str) -> Optional[Dict]:
        """Get airport information by IATA code"""
        if self.airports_df is None:
            self.load_airports()
        
        iata = iata.upper()
        airport = self.airports_df[self.airports_df['iata'] == iata]
        
        if airport.empty:
            return None
        
        return airport.iloc[0].to_dict()
    
    def search_airports(self, query: str, limit: int = 20) -> List[Dict]:
        """
        Search airports by name, city, country, or IATA code
        
        Args:
            query: Search query string
            limit: Maximum number of results
            
        Returns:
            List of airport dictionaries
        """
        if self.airports_df is None:
            self.load_airports()
        
        query = query.lower()
        
        # Search in multiple fields
        mask = (
            self.airports_df['name'].str.lower().str.contains(query, na=False) |
            self.airports_df['city'].str.lower().str.contains(query, na=False) |
            self.airports_df['iata'].str.lower().str.contains(query, na=False) |
            self.airports_df['country'].str.lower().str.contains(query, na=False)
        )
        
        results = self.airports_df[mask].head(limit)
        return results.to_dict('records')