# Mini Data Platform
This project sets up a basic data platform using Astronomer Airflow

The platform demonstrating the four core layer of a data pipeline:
1. Collect Data
2. Process Data 
3. Store Data
4. Visualize data


## Services Used
- **Airflow (Astro CLI)**: Orchestrates data ingestion and processing
- **PostgreSQL**: Database to stores processed data
- **MinIO**:  - file storage (like AWS S3)
- **Metabase**: creates charts and reports


## Setup
1. Install Dependancies
    - Docker
    - Astronemer

2. Clone this repo
```bash
git clone https://github.com/your-username/mini-data-platform.git
cd mini-data-platform
```
3. Set envronment variables.
Create a .env file in the project root directory and copy the evnironment in the the .env.example and paste in the .env file

4. Start the platform
```
astro dev
```
This will spin up the services below:
- 
- 

---

### Project Sructure
```
├── dags/                  # Your Airflow DAGs
├── include/               # Extra files for DAGs
├── plugins/               # Custom operators/hooks
├── tests/                 # Unit tests for DAGs
├── docker-compose.override.yml   # Extra services (Postgres)
├── .env                   # Environment variables (not committed)
└── readme.md
```
## Next Steps
- Add MinIO for S3-like storage
- Add Metabase for BI and visualization
- Sample data generator