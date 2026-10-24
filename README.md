\# NYC Taxi Data Pipeline using PySpark



\## Project Overview



This project implements a batch data processing pipeline using \*\*Python and PySpark\*\* to process NYC Yellow Taxi trip data for January 2025.



The pipeline reads raw Parquet data, validates and filters records, derives trip-level metrics, and generates business-level summaries by pickup location. It writes both the cleaned trip data and aggregated results in Parquet format.



The project demonstrates practical data engineering concepts, including distributed data processing, data-quality filtering, feature engineering, aggregation, and structured data output.



\*\*Execution environment:\*\* Local Windows machine using PySpark. AWS services and cloud integration are not currently implemented.



\## Architecture



```text

NYC Yellow Taxi Raw Data (Parquet)

&#x20;               |

&#x20;               v

&#x20;        PySpark Ingestion

&#x20;               |

&#x20;               v

&#x20;      Calculate Trip Duration

&#x20;               |

&#x20;               v

&#x20;     Data Quality Filtering

&#x20;               |

&#x20;               v

&#x20;       Calculate Average Speed

&#x20;               |

&#x20;               v

&#x20;         Valid Trip Records

&#x20;               |

&#x20;         +-----+------+

&#x20;         |            |

&#x20;         v            v

&#x20;  Cleaned Trip    Pickup Location

&#x20;      Data          Aggregation

&#x20;         |            |

&#x20;         v            v

&#x20;   Parquet Output  Parquet Output

```



\## Technology Stack



\- \*\*Programming language:\*\* Python

\- \*\*Processing engine:\*\* Apache Spark / PySpark

\- \*\*Data format:\*\* Parquet

\- \*\*Processing approach:\*\* Batch processing

\- \*\*Execution environment:\*\* Local Windows machine

\- \*\*Dependencies:\*\* PySpark 4.0.4, Py4J 0.10.9.9



\## Dataset



The pipeline processes NYC Yellow Taxi trip data for January 2025.



\*\*Expected input file:\*\*



`data/raw/yellow\_tripdata\_2025-01.parquet`



The source dataset is stored locally and excluded from GitHub to avoid committing large data files.



\## Pipeline Implementation



\### 1. Data Ingestion



The pipeline reads the source Parquet dataset into a PySpark DataFrame. It counts the input records and prints the source schema for inspection.



\### 2. Feature Engineering: Trip Duration



The pipeline calculates trip duration in minutes using the pickup and drop-off timestamps:



`trip\_duration\_minutes = drop-off time - pickup time`



The calculated duration is rounded to two decimal places.



\### 3. Data Quality Filtering



The pipeline retains records that meet all four validation conditions:



\- Trip duration is greater than zero.

\- Trip distance is greater than zero.

\- Fare amount is greater than or equal to zero.

\- Passenger count is greater than zero.



Records that fail one or more conditions are excluded from further processing.



The pipeline reports the total input records, valid records, and filtered records. It does not currently save rejected records separately or categorize them by rejection reason.



\### 4. Feature Engineering: Average Speed



The pipeline calculates average trip speed in miles per hour using trip distance and duration:



`avg\_speed\_mph = trip\_distance / (trip\_duration\_minutes / 60)`



The result is rounded to two decimal places.



\### 5. Cleaned Dataset Generation



The valid, transformed records are written to a Parquet output directory. The output retains the original columns and adds:



\- `trip\_duration\_minutes`

\- `avg\_speed\_mph`



\### 6. Business Aggregation



The pipeline groups valid records by `PULocationID` and calculates:



\- Total number of trips

\- Average trip distance

\- Average fare amount

\- Average calculated speed



The aggregated metrics are rounded to two decimal places where applicable.



\### 7. Output Generation



Both the cleaned trip records and pickup-location summary are written in Parquet format. The script uses overwrite mode, replacing existing output datasets when rerun.



\## Execution Results



The following results were observed during a successful local execution of the pipeline.



| Metric | Result |

|---|---:|

| Input records | 3,475,226 |

| Valid records | 2,816,365 |

| Filtered records | 658,861 |

| Valid-record retention | 81.04% |

| Records filtered out | 18.96% |



The valid-record retention rate represents the percentage of input records that passed the implemented validation rules. Filtered records do not necessarily represent corrupt data; they may simply fail one or more of the project's defined conditions.



\## Output Datasets



\### 1. Cleaned Taxi Data



\*\*Output directory:\*\* `data/processed/cleaned\_taxi\_data/`



Contains valid trip records with the additional calculated columns:



\- `trip\_duration\_minutes`

\- `avg\_speed\_mph`



Spark writes the output as a Parquet dataset that may contain multiple part files.



\### 2. Pickup Location Summary



\*\*Output directory:\*\* `data/processed/taxi\_summary/`



Contains aggregated metrics grouped by pickup location.



| Column | Description |

|---|---|

| `PULocationID` | Pickup location identifier |

| `total\_trips` | Number of valid trips for the pickup location |

| `avg\_distance` | Average trip distance |

| `avg\_fare` | Average fare amount |

| `avg\_speed` | Average calculated speed in miles per hour |



Successful Spark writes create `\_SUCCESS` marker files in the output directories.



\## Project Structure



```text

nyc-taxi-data-pipeline/

├── src/

│   └── ingestion.py

├── data/

│   ├── raw/                 # Local source data; not committed

│   └── processed/           # Generated outputs; not committed

├── screenshots/             # Pipeline execution screenshots

├── .gitignore

├── requirements.txt

└── README.md

```



\## Run Locally



\### Prerequisites



\- Python 3.9 or another compatible Python version

\- Java 17 or later, compatible with PySpark 4.x

\- Git

\- Hadoop native utilities configured on Windows if required by your local Spark setup



\### 1. Clone the Repository



```bash

git clone https://github.com/DnyaneshwariRaskar/nyc-taxi-data-pipeline.git

cd nyc-taxi-data-pipeline

```



\### 2. Create and Activate a Virtual Environment



In Windows Command Prompt:



```cmd

python -m venv .venv

.venv\\Scripts\\activate

```



\### 3. Install Dependencies



```cmd

python -m pip install -r requirements.txt

```



\### 4. Add the Input Dataset



Create the following directory if it does not exist:



`data/raw/`



Place the January 2025 Yellow Taxi Parquet file inside it:



`data/raw/yellow\_tripdata\_2025-01.parquet`



\### 5. Run the Pipeline



Run the following command from the project root:



```cmd

python src\\ingestion.py

```



The script prints the source schema, record counts, sample cleaned records, sample aggregated results, and a final execution report.



On successful completion, the outputs are written to:



\- `data/processed/cleaned\_taxi\_data/`

\- `data/processed/taxi\_summary/`



\*\*Windows note:\*\* If Spark reports that `HADOOP\_HOME` or `winutils.exe` is missing, configure the Hadoop native utilities for your local environment before rerunning the pipeline.





\## Future Enhancements



The following improvements are potential next steps and are not part of the current implementation:



\- \*\*Amazon S3 integration:\*\* Read source data from and write processed data to cloud storage.

\- \*\*AWS CloudWatch:\*\* Add centralized logging, monitoring, and failure alerts.

\- \*\*Enhanced data quality:\*\* Record rejected rows separately and report rejection reasons.

\- \*\*Workflow orchestration:\*\* Add scheduling, dependency management, and automated execution.

\- \*\*Automated testing:\*\* Add tests for transformation logic, validation rules, and aggregation results.

\- \*\*Pipeline optimization:\*\* Evaluate caching, partitioning, and execution performance for larger workloads.



\## Key Learning Outcomes



\- Processing millions of records using PySpark.

\- Reading and writing Parquet datasets.

\- Applying data-quality filters to structured data.

\- Creating derived columns using timestamp and numerical calculations.

\- Performing group-by aggregations to generate business metrics.

\- Configuring a local Python and Spark environment on Windows.

\- Structuring and documenting a data engineering project for reproducibility and future extension.





