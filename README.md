# watchlists-extraction

## Setup

1. **Create a virtual environment and install dependencies:**

    ```sh
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2. **Update the configuration file:**

    Edit `src/ofac/config.json` to update the file paths as needed.

3. **Unzip the data file:**

    Unzip `sdn_advanced.xml.zip` to `sdn_advanced.xml` within the same folder under `source_data/ofac`:

    ```sh
    unzip source_data/ofac/sdn_advanced.xml.zip -d source_data/ofac/
    ```

## Execution

1. **Run the bronze compute process:**

    ```sh
    python src/ofac/medallion/bronze/bronze_compute.py
    ```

2. **Run the silver compute process:**

    ```sh
    python src/ofac/medallion/silver/silver_compute.py
    ```

3. **Run the gold compute process:**

    ```sh
    python src/ofac/medallion/gold/gold_compute.py
    ```
4. **Run the query process:**

   Use the `query.py` files to run Spark SQL queries against the Iceberg tables that are created:

    ```sh
    python src/ofac/medallion/bronze/bronze_query.py
    python src/ofac/medallion/silver/silver_query.py
    python src/ofac/medallion/gold/gold_query.py
    ```
