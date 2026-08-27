# NASA APOD Data Pipeline with Apache Airflow

This project creates an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow to fetch NASA's Astronomy Picture of the Day (APOD) data and store it in a PostgreSQL database.

## 📋 Project Overview

The pipeline performs the following tasks daily:
1. **Create Table**: Creates a PostgreSQL table if it doesn't exist
2. **Extract**: Fetches APOD data from NASA API
3. **Transform**: Processes and structures the API response
4. **Load**: Inserts the transformed data into PostgreSQL database

## 🏗️ Architecture

```
NASA API → Airflow DAG → PostgreSQL Database
    ↓         ↓              ↓
  APOD Data  ETL Process   Stored Data
```

## 📁 Project Structure

```
nasa-apod-pipeline/
├── dags/
│   └── nasa_apod_postgres.py    # Main DAG file
├── logs/                        # Airflow logs
├── plugins/                     # Custom plugins (if any)
├── docker-compose.yml           # Docker services configuration
├── requirements.txt             # Python dependencies
└── README.md                   # This file
```

## 🛠️ Prerequisites

- Docker CLI installed
- Docker Compose installed
- NASA API key (free from https://api.nasa.gov/)

## 🚀 Setup Instructions

### Option 1: Using Docker Compose (Recommended)

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd nasa-apod-pipeline
   ```

2. **Create project directories**
   ```bash
   mkdir -p dags logs plugins
   ```

3. **Place your DAG file**
   ```bash
   cp nasa_apod_postgres.py dags/
   ```

4. **Create docker-compose.yml**
   ```yaml
   version: '3.8'
   services:
     postgres:
       image: postgres:13
       environment:
         POSTGRES_USER: airflow
         POSTGRES_PASSWORD: airflow
         POSTGRES_DB: airflow
       volumes:
         - postgres_db_volume:/var/lib/postgresql/data
       ports:
         - "5432:5432"

     airflow-webserver:
       image: apache/airflow:2.7.0
       depends_on:
         - postgres
       environment:
         AIRFLOW__CORE__EXECUTOR: LocalExecutor
         AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
         AIRFLOW__CORE__FERNET_KEY: ''
         AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
         AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
         AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
       volumes:
         - ./dags:/opt/airflow/dags
         - ./logs:/opt/airflow/logs
         - ./plugins:/opt/airflow/plugins
       ports:
         - "8080:8080"
       command: webserver

     airflow-scheduler:
       image: apache/airflow:2.7.0
       depends_on:
         - postgres
       environment:
         AIRFLOW__CORE__EXECUTOR: LocalExecutor
         AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
         AIRFLOW__CORE__FERNET_KEY: ''
       volumes:
         - ./dags:/opt/airflow/dags
         - ./logs:/opt/airflow/logs
         - ./plugins:/opt/airflow/plugins
       command: scheduler

   volumes:
     postgres_db_volume:
   ```

5. **Initialize Airflow**
   ```bash
   # Initialize database
   docker-compose run --rm airflow-webserver airflow db init

   # Create admin user
   docker-compose run --rm airflow-webserver airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com \
       --password admin
   ```

6. **Start services**
   ```bash
   docker-compose up -d
   ```

### Option 2: Native Installation

1. **Install dependencies**
   ```bash
   pip install apache-airflow[postgres,http]==2.7.0
   ```

2. **Install PostgreSQL**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib
   sudo systemctl start postgresql
   ```

3. **Initialize Airflow**
   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com \
       --password admin
   ```

4. **Start Airflow services**
   ```bash
   # Terminal 1
   airflow scheduler

   # Terminal 2
   airflow webserver --port 8080
   ```

## ⚙️ Configuration

### 1. NASA API Connection

1. Access Airflow UI at `http://localhost:8080`
2. Login with `admin/admin`
3. Go to **Admin → Connections**
4. Create new connection:
   - **Connection Id**: `nasa_api`
   - **Connection Type**: `HTTP`
   - **Host**: `https://api.nasa.gov`
   - **Extra**: `{"api_key": "YOUR_NASA_API_KEY"}`

### 2. PostgreSQL Connection

Create another connection:
- **Connection Id**: `my_postgres_connection`
- **Connection Type**: `Postgres`
- **Host**: `localhost` (or `postgres` if using Docker)
- **Schema**: `airflow`
- **Login**: `airflow`
- **Password**: `airflow`
- **Port**: `5432`

## 📊 Database Schema

The pipeline creates a table `apod_data` with the following structure:

```sql
CREATE TABLE apod_data (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    explanation TEXT,
    url TEXT,
    date DATE,
    media_type VARCHAR(50)
);
```

## 🔄 DAG Details

- **DAG ID**: `nasa_apod_postgres`
- **Schedule**: Daily (`@daily`)
- **Start Date**: Yesterday
- **Catchup**: Disabled

### Task Dependencies

```
create_table() → extract_apod → transform_apod_data() → load_data_to_postgres()
```

## 📝 Usage

1. **Access Airflow UI**: Navigate to `http://localhost:8080`
2. **Enable DAG**: Toggle the `nasa_apod_postgres` DAG to ON
3. **Trigger manually**: Click the play button to run immediately
4. **Monitor**: Check task status and logs in the UI

## 🔍 Monitoring and Troubleshooting

### Viewing Logs
- **Docker**: `docker-compose logs airflow-scheduler`
- **Native**: Check `~/airflow/logs/`

### Common Issues

1. **Connection errors**: Verify API key and database credentials
2. **Import errors**: Ensure all required packages are installed
3. **Database connection**: Check PostgreSQL service status

### Useful Commands

```bash
# Check running containers
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler

# Restart services
docker-compose restart

# Stop all services
docker-compose down
```

## 🧪 Testing

Test the DAG manually:
```bash
# Test individual tasks
docker-compose run --rm airflow-webserver airflow tasks test nasa_apod_postgres create_table 2024-01-01

# Test entire DAG
docker-compose run --rm airflow-webserver airflow dags test nasa_apod_postgres 2024-01-01
```

## 📦 Dependencies

- `apache-airflow[postgres,http]>=2.7.0`
- `psycopg2-binary`
- `requests`

## 🔐 Security Notes

- Store API keys securely using Airflow Connections
- Use environment variables for sensitive data
- Regularly rotate API keys and database passwords

## 📈 Future Enhancements

- Add data validation and quality checks
- Implement error handling and retry logic
- Add email notifications for failures
- Create data visualization dashboard
- Add unit tests for DAG tasks

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [NASA Open Data Portal](https://api.nasa.gov/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review Airflow logs
3. Open an issue in the repository
4. Consult the official Airflow documentation