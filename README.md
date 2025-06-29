# Job Board Scraper

## Overview

Job Board Scraper is a Python-based tool that fetches job offers from a configurable API endpoint and stores them in a local SQLite database. It serves as a practical example of:

-   Web scraping implementation
-   Data extraction and transformation
-   Local database management
-   ETL pipeline development

## Setup and Configuration

1. **Environment Setup**

    ```sh
    conda env create -f environment.yaml
    conda activate job-board-scraper
    ```

2. **Environment Variables**
   Copy the example environment file and configure your settings:

    ```sh
    cp .env.example .env
    ```

    Then edit the `.env` file with your configuration:

    ```sh
    # API Configuration
    OFFERS_URL=<your_job_offers_api_url>

    # Database Settings
    DB_PATH=<path_to_your_sqlite_db>
    DB_NAME=job_board.db
    ```

## Usage

To run the scraper:

```sh
python run.py --pages <number_of_pages>
```

If `--pages` is not specified, all available pages will be fetched.

## Technologies

-   Python 3.11
-   requests & requests-cache - for API communication with caching
-   python-dotenv - for environment management
-   pandas - for data manipulation
-   jupyterlab - for interactive data analysis
-   sqlite3 (built-in) - for data storage

## Project Structure

-   `modules/` - Core logic for fetching and storing job offers
-   `run.py` - Main entry point for the ETL process
-   `notebooks/` - Jupyter notebooks for data analysis and conversion
-   `data/` - (Optional) Data output directory

## Disclaimer

This project is a non-commercial, educational tool designed to demonstrate web scraping techniques and ETL (Extract, Transform, Load) processes using publicly available job listings data. The scraper is intended solely for learning purposes and should be used in accordance with the terms of service of any targeted websites.
