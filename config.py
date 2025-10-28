import os
from pathlib import Path
from typing import List

class Config:
    """Configuration settings for Flight Route Advisor backend"""
    
    # Base directory
    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / "data"
    GEPHI_DIR = BASE_DIR / "gephi"
    
    # Data files (local - no download needed)
    AIRPORTS_FILE = DATA_DIR / "airports.dat"
    ROUTES_FILE = DATA_DIR / "routes.dat"
    AIRLINES_FILE = DATA_DIR / "airlines.dat"
    
    # Transfer time settings (in minutes)
    MIN_TRANSFER_TIME: int = 60
    DEFAULT_TRANSFER_TIME: int = 90
    INTERNATIONAL_TRANSFER_TIME: int = 120
    
    # Flight cost parameters
    BASE_COST_PER_KM: float = 0.1
    TRANSFER_COST_MULTIPLIER: float = 1.5
    
    # Graph analysis parameters
    MAX_STOPS: int = 3
    TOP_K_ROUTES: int = 5
    
    # API settings
    API_TITLE: str = "Flight Route Advisor API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "API for flight route optimization and hub analysis using graph theory"
    
    # CORS settings - Add your frontend URLs here
    CORS_ORIGINS: List[str] = [
        "http://localhost:8080",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ]
    
    # Hub analysis
    TOP_HUBS_COUNT: int = 20
    
    # Server settings
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    RELOAD: bool = True
    
    @classmethod
    def ensure_directories(cls):
        """Ensure required directories exist"""
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.GEPHI_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def validate_data_files(cls) -> bool:
        """Check if all required data files exist"""
        files = [cls.AIRPORTS_FILE, cls.ROUTES_FILE, cls.AIRLINES_FILE]
        missing = [f for f in files if not f.exists()]
        
        if missing:
            print("Missing data files:")
            for f in missing:
                print(f"  - {f}")
            print("\nPlease download from: https://openflights.org/data.html")
            return False
        
        return True