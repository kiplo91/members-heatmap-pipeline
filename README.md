# Customer Heatmap Pipeline 🚀

## 📊 Overview

This project builds an end-to-end data pipeline using Apache Airflow to process customer insurance data and visualize geographic distribution.

## 🔧 Features

* Data cleaning and preprocessing (handling missing values, formatting issues)
* Customer deduplication using unique identifiers
* Aggregation of customer data by state
* Data enrichment with latitude and longitude
* Interactive heatmap visualization using Folium
* Business dashboard using Power BI

## ⚙️ Tech Stack

* Python (Pandas)
* Apache Airflow (Docker)
* Folium (Geospatial visualization)
* Power BI (Dashboarding)

## 🔄 Pipeline Flow

Raw Data → Clean → Deduplicate → Aggregate → Merge → Output

## 🗺️ Output

* Cleaned dataset
* Aggregated dataset
* Heatmap (HTML)
* Power BI dashboard

## 📁 Project Structure

* dags/ → Airflow pipeline code
* data/ → processed output
* visualize_heatmap.py → Folium visualization

## 👨‍💻 Author

Ahmad Faisal
