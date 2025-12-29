# Flight Route Advisor Backend

## Thông tin dự án

**Tên dự án**: Flight Route Advisor - Hệ thống Tư vấn Tuyến Bay  
**Môn học**: IS353 - Mạng Xã Hội  
**Mô tả**: Backend API sử dụng lý thuyết đồ thị để tìm kiếm và phân tích tuyến bay

## Thành viên nhóm

- Nguyễn Chí Vĩ - 22521656
- Võ Đức Vĩnh - 22521684
- Dương Văn Súa - 22521267

---

## Yêu cầu hệ thống

- **Python**: 3.8 trở lên
- **Hệ điều hành**: Windows, Linux, macOS
- **Bộ nhớ**: Tối thiểu 2GB RAM (khuyến nghị 4GB+)
- **Dung lượng**: ~100MB cho dependencies và data

---

## Cài đặt

### 1. Clone repository

```bash
git clone <repository-url>
cd FlightRouteAdvisor_Backend
```

### 2. Tạo virtual environment (khuyến nghị)

**Windows:**
```powershell
python -m venv venv
venv\Scripts\activate
```

**Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Cài đặt dependencies

```bash
pip install -r requirement.txt
```

### 4. Chuẩn bị dữ liệu

Đảm bảo có 2 file dữ liệu trong thư mục `data/`:
- `airports.dat` - Danh sách sân bay (OpenFlights format)
- `routes.dat` - Danh sách tuyến bay (OpenFlights format)

**Lưu ý**: Dữ liệu được lấy từ [OpenFlights Database](https://openflights.org/data.html)

---

## Cấu trúc dự án

```
FlightRouteAdvisor_Backend/
├── main.py                      # FastAPI application, API endpoints
├── config.py                    # Configuration settings
├── requirement.txt              # Python dependencies
├── README.md                    # File này
│
├── app/
│   ├── models/
│   │   └── graph.py            # FlightGraph class (đồ thị chuyến bay)
│   ├── services/
│   │   ├── data_loader.py      # Tải và xử lý dữ liệu
│   │   ├── hub_analysis.py     # Phân tích hub airports
│   │   └── performance_tester.py # Kiểm thử hiệu năng
│   └── test/
│       └── run_benchmark.py    # Script benchmark
│
├── data/                        # Dữ liệu đầu vào
│   ├── airports.dat
│   └── routes.dat
│
├── gephi/                       # Export graph cho Gephi visualization
│   └── flight_network.gexf
│
└── results/                     # Kết quả benchmark
    └── benchmark_results_*.json
```

---

## Hướng dẫn chạy

### 1. Chạy API Server

**Cách 1: Sử dụng Python trực tiếp**
```bash
python main.py
```

**Cách 2: Sử dụng Uvicorn (khuyến nghị cho production)**
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

**Các tham số:**
- `--host 0.0.0.0`: Lắng nghe trên tất cả interfaces
- `--port 8000`: Port mặc định (có thể thay đổi trong `config.py`)
- `--reload`: Tự động reload khi code thay đổi (chỉ dùng cho development)

**Output log mẫu khi khởi động thành công:**
```
================================================================================
FLIGHT ROUTE ADVISOR - STARTING UP
================================================================================

Loading OpenFlights data...
      Loaded 6072 airports
      Loaded 67663 routes

Building graph nodes (airports)...
Added 6072 airport nodes
Building graph edges (routes)...
Added 37042 route edges

Initializing hub analyzer...

Graph Statistics:
      Airports (nodes): 6072
      Routes (edges): 37042
      Average degree: 12.2
      Is connected: False
      Components: 2822

Exporting graph to GEXF format...
Graph exported to: gephi/flight_network.gexf

================================================================================
API READY - Listening on http://0.0.0.0:8000
API Docs available at http://localhost:8000/docs
================================================================================

INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [xxxxx] using WatchFiles
INFO:     Started server process [xxxxx]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

### 2. Truy cập API Documentation

Sau khi server chạy, mở trình duyệt và truy cập:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **API Root**: http://localhost:8000/

### 3. Chạy Performance Benchmark

Để chạy các test hiệu năng và so sánh thuật toán:

```bash
python app/test/run_benchmark.py
```

**Output log mẫu:**
```
====== FLIGHT ROUTE ADVISOR: BENCHMARK ======

-> Measuring Graph Build Time and Memory Usage...
      Graph Build Time: 55.0384 s
      Memory Usage (Peak): 40.29 MB

LWCC size: 3231 / 6072

-> Measuring Latency/Throughput for Dijkstra (distance)...
      Latency: 9.307 ms, Throughput: 107.45 qps

-> Measuring Latency/Throughput for A* (distance)...
      Latency: 56.4326 ms, Throughput: 17.72 qps

-> Comparing Dijkstra vs A* for path PDS -> MAF...
      Dijkstra Time: 0.9876 ms | A* Time: 20.597 ms

Calculating centrality metrics...
  - Degree centrality...
  - Betweenness centrality...
  - Closeness centrality...
  - PageRank...
Centrality metrics calculated successfully

--- 6. CENTRALITY METRICS RESULTS (TOP 5) ---

Top 5 Hubs by Degree Centrality:
| iata | name                                  | country        | total_degree |
|------|---------------------------------------|----------------|--------------|
| ATL  | Hartsfield Jackson Atlanta Intl      | United States  | 233          |
| DXB  | Dubai International                  | United Arab Emirates | 227    |
| IST  | Istanbul Airport                     | Turkey         | 217          |
| ...  | ...                                  | ...            | ...          |

Network Robustness Baseline (after removing top 5 betweenness hubs) calculated.

Benchmark finished.
```

Kết quả benchmark sẽ được lưu trong thư mục `results/` (nếu có script export).

---

## API Endpoints chính

### Airport Endpoints

- `GET /airports/search?q={query}` - Tìm kiếm sân bay
- `GET /airports/{iata}` - Thông tin chi tiết sân bay

### Route Endpoints

- `POST /routes/find` - Tìm đường bay ngắn nhất
- `POST /routes/alternatives` - Tìm k đường bay thay thế
- `POST /routes/compare-algorithms` - So sánh Dijkstra vs A*

### Hub Analysis Endpoints

- `GET /hubs/top?metric={metric}&top_k={k}` - Top hub airports
- `GET /hubs/{iata}` - Thông tin hub
- `POST /hubs/removal-analysis` - Phân tích tác động khi loại bỏ hubs
- `POST /hubs/alternatives` - Tìm alternative hubs

### Graph Statistics

- `GET /graph/stats` - Thống kê đồ thị
- `GET /health` - Health check

Chi tiết đầy đủ xem tại: http://localhost:8000/docs

---

## Ví dụ sử dụng API

### Tìm đường bay ngắn nhất

**Request:**
```bash
curl -X POST "http://localhost:8000/routes/find" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "SGN",
    "destination": "HAN",
    "cost_type": "time",
    "max_stops": 2
  }'
```

**Response:**
```json
{
  "path": ["SGN", "HAN"],
  "segments": [
    {
      "from": "SGN",
      "to": "HAN",
      "distance": 1170.45,
      "time": 1.46,
      "cost": 117.05
    }
  ],
  "stops": 0,
  "total_distance": 1170.45,
  "total_flight_time": 1.46,
  "total_transfer_time": 0.0,
  "total_time": 1.46,
  "total_cost": 117.05,
  "path_details": [...]
}
```

### Phân tích hub removal

**Request:**
```bash
curl -X POST "http://localhost:8000/hubs/removal-analysis" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "SGN",
    "destination": "JFK",
    "hubs_to_remove": ["DXB", "IST"]
  }'
```

---

## Experiment Diary / Ghi chú thực nghiệm

### 2024-12-14: Benchmark và Performance Testing

**Mục tiêu**: Đo lường hiệu năng các thuật toán và phân tích network

**Kết quả**:

1. **Graph Build Performance**
   - Build time: ~55 giây
   - Memory usage: ~40 MB
   - Graph stats: 6,072 nodes, 37,042 edges

2. **Algorithm Performance** (200 test cases)
   - **Dijkstra**: Avg latency 9.31 ms, Throughput 107.45 qps
   - **A***: Avg latency 56.43 ms, Throughput 17.72 qps
   - **Nhận xét**: Dijkstra nhanh hơn A* trên dataset này do overhead của heuristic calculation

3. **Network Analysis**
   - Largest Weakly Connected Component (LWCC): 3,231 nodes (53% của tổng số)
   - Top hubs: ATL, DXB, IST, PEK, LHR (dựa trên betweenness centrality)
   - Network robustness: Khi remove top 5 hubs, LWCC giảm ~8.26%

4. **Success Rate**: 99% queries thành công trên LWCC

### 2024-12-15: Optimization và Cải thiện

**Cải tiến**:
- Implemented fast search cho `max_stops <= 1` (brute-force enumeration)
- Added early termination checks
- Optimized graph construction (chỉ giữ edge tốt nhất cho duplicate routes)

**Kết quả**: Giảm thời gian query cho các trường hợp đơn giản (~50% faster)

### Observations

1. **Dijkstra vs A***:
   - Trên graph này (6K nodes), Dijkstra thực tế nhanh hơn
   - A* có overhead từ geodesic distance calculation
   - A* sẽ hữu ích hơn trên graph lớn hơn (100K+ nodes)

2. **Hub Analysis**:
   - Betweenness centrality tốt nhất để identify critical hubs
   - Top hubs tập trung ở các trung tâm giao thông lớn (US, Middle East, Europe)
   - Network khá resilient: cần remove nhiều hubs để phân mảnh đáng kể

3. **Data Quality**:
   - OpenFlights data khá đầy đủ nhưng có một số routes thiếu
   - Cần filter invalid IATA codes và null values
   - Direct flights only (stops=0) đủ cho analysis

---

## Troubleshooting

### Lỗi: "Required data files are missing"

**Nguyên nhân**: Thiếu file `airports.dat` hoặc `routes.dat` trong thư mục `data/`

**Giải pháp**: 
- Đảm bảo có 2 file data trong `data/`
- Kiểm tra tên file chính xác (airports.dat, routes.dat)

### Lỗi: ModuleNotFoundError

**Nguyên nhân**: Chưa cài đặt dependencies hoặc virtual environment chưa được activate

**Giải pháp**:
```bash
# Activate virtual environment
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/macOS

# Cài đặt lại dependencies
pip install -r requirement.txt
```

### API chạy chậm

**Nguyên nhân**: Graph build mất thời gian (~55 giây)

**Giải pháp**: 
- Đây là behavior bình thường khi khởi động
- Graph được build một lần và giữ trong memory
- Queries sau đó sẽ rất nhanh (9-56ms)

### Port 8000 đã được sử dụng

**Giải pháp**: 
- Đổi port trong `config.py`: `PORT = 8001`
- Hoặc dùng: `uvicorn main:app --port 8001`

---

## Cấu hình

Các settings có thể thay đổi trong `config.py`:

- **Transfer times**: MIN_TRANSFER_TIME, DEFAULT_TRANSFER_TIME, INTERNATIONAL_TRANSFER_TIME
- **Cost parameters**: BASE_COST_PER_KM, TRANSFER_FEE_PER_STOP
- **API settings**: HOST, PORT, CORS_ORIGINS
- **Analysis parameters**: MAX_STOPS, TOP_K_ROUTES, TOP_HUBS_COUNT

---

## Tài liệu tham khảo

- **NetworkX Documentation**: https://networkx.org/
- **FastAPI Documentation**: https://fastapi.tiangolo.com/
- **OpenFlights Data**: https://openflights.org/data.html
- **Graph Theory**: Introduction to Algorithms (CLRS)

---

## License

Dự án này được phát triển cho mục đích học tập - IS353 Mạng Xã Hội

---

## Liên hệ

Nếu có vấn đề hoặc câu hỏi, vui lòng liên hệ:
- **Thanh viên nhóm thực hiện: Dương Văn Súa** (Backend Developer): 22521267@gm.uit.edu.vn
