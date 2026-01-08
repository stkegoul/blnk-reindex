# Typesense Reindex Script

## Prerequisites

- Go installed on your system
- **Blnk version 0.12.1** - This script is designed for and tested with Blnk 0.12.1

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/stkegoul/blnk-reindex
   cd blnk-reindex
   ```

2. Update `config.json` with your database and Typesense connection details:
   ```json
   {
     "database": {
       "dns": "postgres://user:password@host:port/database?sslmode=disable"
     },
     "typesense": {
       "host": "localhost",
       "port": "8108",
       "protocol": "http",
       "api_key": "your-api-key"
     }
   }
   ```

3. (Optional) Set time range for selective reindexing:
   - Set `start_time_utc` and `end_time_utc` in UTC to reindex only records within that time range
   - Leave both empty to reindex everything
   - Example: `"start_time_utc": "2024-01-01T00:00:00Z"`, `"end_time_utc": "2024-12-31T23:59:59Z"`

## Running

```bash
go mod tidy
go run .
```
