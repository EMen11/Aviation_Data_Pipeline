# Aviation_Data_Pipeline - Aviation Analysis : From Python ETL to Power BI Insights
Data pipeline and Power BI dashboard analyzing flight performance (2019–2020). Python for ETL, SQL star schema, and DAX for KPI insights on delays and cancellations during COVID-19.

From Python ETL to Power BI Insights

---
## 1. Aviation Analysis (2019–2020) -  Overview

This project presents an end-to-end data analytics workflow designed to assess how the COVID-19 pandemic impacted flight operations across U.S. airlines and airports between 2019 and 2020.  
It combines three layers of analysis:
- Python for data cleaning and standardization.
- SQL for relational modeling and aggregation.
- Power BI for visualization and business-oriented KPI tracking.

The dashboard highlights key operational metrics such as total flights, delay rates, average delay duration, and cancellation rates, providing a comparative view between the pre-pandemic year (2019) and the first year of COVID-19 (2020).

---

### 1.1 Problem Statement

How did the COVID-19 pandemic affect airline and airport performance in terms of punctuality, delays, and cancellations?  
Beyond the reduction in total flights, did operational reliability actually improve or deteriorate during the crisis?

---

### 1.2 Hypothesis

While the number of flights dropped significantly in 2020, it is hypothesized that:
- Delay rates and total delay minutes decreased due to lower congestion in the airspace and airports.
- Cancellation rates increased due to uncertainty, schedule adjustments, and crew availability issues.

---

## 2. Objectives

This section defines the specific analytical goals of the project — the questions that guided the data preparation and modeling phases.

1. Measure how overall flight volume evolved between 2019 and 2020.
2. Identify and quantify the main causes of flight delays.
3. Compare airline and airport performance year-over-year.
4. Evaluate whether lower air traffic improved punctuality and reliability.
5. Build an interactive Power BI dashboard to visualize and communicate these results effectively.

---

### Project Folder Structure
```

aviation_analytics/
│
├── README.md
│
├── data/
│ ├── raw/ → raw datasets (From Kaggle, unmodified)
│ ├── processed/ → cleaned and transformed datasets exported from Python
│
├── reports/
│ ├── aviation_analytics_Page_*.jpg → Power BI dashboard screenshots
│ └── aviation_analytics_report.pdf → exported Power BI report 
│
├── powerbi/
│ ├── aviation_analytics.pbix → main Power BI project file
│ ├── measures.txt → exported DAX measures 
│ └── model_schema.png → relational model schema
│
├── src/
│ ├── create_star_schema.sql → SQL script for data modeling and table relationships
│ └── cleaning_pipeline.py → optional script for sanity checks or quick exploration
```


## 3. Dataset & Data Model

### 3.1 Dataset Overview

The raw dataset was obtained from the public Kaggle repository  
**[Airline Delays (2019–2020)](https://www.kaggle.com/datasets/eugeniyosetrov/airline-delays/data)**.  
It contains monthly aggregated flight statistics by **carrier** and **airport**, including:

| Column | Description |
|--------|--------------|
| `year` | Year of observation |
| `month` | Month of observation |
| `carrier` | Airline IATA code |
| `airport` | Airport IATA code |
| `arr_flights` | Total number of arriving flights |
| `arr_del15` | Number of flights delayed by more than 15 minutes |
| `arr_cancelled` | Number of cancelled flights |
| `arr_delay` | Total delay minutes |
| `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay` | Breakdown of delay causes |

This dataset serves as the foundation for analyzing operational performance across U.S. airlines and airports during the COVID-19 period.

---

### 3.2 Data Preparation — Python (ETL Layer)

The Python ETL process prepared the raw Kaggle CSVs for SQL modeling and Power BI.  
It automatically organizes the data into two folders:

- `data/raw/` → original Kaggle files  
- `data/processed/` → cleaned, standardized outputs ready for SQL ingestion

The main steps include column normalization, missing-value handling, and date parsing.

```bash 
import pandas as pd
import numpy as np

df = pd.read_csv("data/raw/flights_2019_2020.csv")

# Normalize column names
df.columns = (df.columns.str.strip()
              .str.lower()
              .str.replace(r"[^a-z0-9]+", "_", regex=True))

# Handle missing numeric values
num_cols = ["arr_flights", "arr_del15", "arr_cancelled", "arr_delay"]
df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

# Parse year/month into a single date column
df["flight_date"] = pd.to_datetime(
    df["year"].astype(str) + "-" + df["month"].astype(str) + "-01",
    errors="coerce"
)

# Derived KPIs
df["delayed_rate"] = np.where(df["arr_flights"] > 0,
                              df["arr_del15"] / df["arr_flights"], 0)

df["cancellation_rate"] = np.where(df["arr_flights"] > 0,
                                   df["arr_cancelled"] / df["arr_flights"], 0)

df.to_csv("data/processed/fact_delay_clean.csv", index=False)

```

### 3.3 Data Modeling — SQL (Star Schema)

After cleaning, the processed CSVs were imported into a local SQL database to build a **star schema** suitable for Power BI analysis.

**Schema structure:**
- `dim_date` → year, month
- `dim_carrier` → carrier codes
- `dim_airport` → airport codes and names
- `fact_delay` → main fact table containing aggregated delay and cancellation metrics

The SQL script below aggregates the monthly data by carrier and airport, calculating weighted averages and delay rates.

```sql
CREATE TABLE fact_delay AS
SELECT
  year,
  month,
  carrier,
  airport,
  SUM(arr_flights) AS arr_flights,
  SUM(arr_del15) AS arr_del15,
  SUM(arr_cancelled) AS arr_cancelled,
  SUM(arr_delay) AS arr_delay,
  CASE WHEN SUM(arr_del15) > 0
       THEN SUM(arr_delay) / SUM(arr_del15)
       ELSE 0 END AS avg_delay_min_per_delayed_flight,
  CASE WHEN SUM(arr_flights) > 0
       THEN SUM(arr_del15)::numeric / SUM(arr_flights)
       ELSE 0 END AS delayed_rate,
  CASE WHEN SUM(arr_flights) > 0
       THEN SUM(arr_cancelled)::numeric / SUM(arr_flights)
       ELSE 0 END AS cancellation_rate
FROM fact_delay_clean
GROUP BY year, month, carrier, airport;
```
This step creates a clean, relational model optimized for Power BI , ensuring one-to-many relationships between the dimensions (`date` , `darrier`,`airport`) and the fact table (`fact_delay`).
> Note: After data cleaning in Python, the processed datasets were loaded into a local SQL environment (via DBeaver) to create relational tables — `dim_date`, `dim_carrier`, `dim_airport`, and `fact_delay`.  
> The SQL script below reproduces the same star schema used in Power BI, allowing full reproducibility of the relational model across environments.


### 3.4 Before / After Snapshot

**Before (raw CSV excerpt)** — heterogeneous headers, missing values, no derived KPIs:

| FlightDate | CARRIER | AIRPORT | Arr_Flights | Arr_Del15 | Arr_Cancelled | Arr_Delay | Weather_Delay | NAS_Delay | Late_Aircraft_Delay |
|------------|---------|---------|------------:|----------:|--------------:|----------:|--------------:|----------:|--------------------:|
| 2019/01/01 | DL      | ATL     |         900 |       210 |            16 |    14750  |          1200 |       980 |                 4200 |
| 2019/01/01 | UA      | ORD     |         870 |       200 |            14 |    13110  |           900 |       840 |                 3950 |
| 2020/04/12 | AA      | DFW     |         350 |        60 |            22 |     4020  |           120 |       310 |                 1140 |

**After (processed & modeled)** — normalized columns, typed fields, derived KPIs, aggregated by year–month–carrier–airport:

| year | month | carrier | airport | arr_flights | arr_del15 | arr_cancelled | arr_delay | delayed_rate | cancellation_rate | avg_delay_min_per_delayed_flight |
|-----:|------:|---------|---------|------------:|----------:|--------------:|----------:|-------------:|------------------:|---------------------------------:|
| 2019 |     1 | DL      | ATL     |         900 |       210 |            16 |    14750  |        0.233 |             0.018 |                             70.2 |
| 2019 |     1 | UA      | ORD     |         870 |       200 |            14 |    13110  |        0.230 |             0.016 |                             65.6 |
| 2020 |     4 | AA      | DFW     |         350 |        60 |            22 |     4020  |        0.171 |             0.063 |                             67.0 |

---

### 3.5 Data Flow Summary

1) **Ingestion (raw)**
   - Source: Kaggle CSVs
   - Location: `data/raw/`

2) **Python ETL (clean & enrich)**
   - Normalize headers, cast numeric fields, handle missing values
   - Parse `year`/`month` → `flight_date`
   - Derive KPIs: `delayed_rate`, `cancellation_rate`
   - Output: `data/processed/fact_delay_clean.csv`

3) **SQL Modeling (star schema)**
   - Load processed CSVs to staging
   - Build dimensions: `dim_date`, `dim_carrier`, `dim_airport`
   - Aggregate into `fact_delay` (year, month, carrier, airport)
   - Recompute weighted averages and rates for robustness

4) **Power BI (analytics & storytelling)**
   - Import star schema
   - Create DAX measures (KPIs, YoY deltas, insights)
   - Build dashboards:
     - Overview
     - Delay Causes Analysis
     - Airline Performance
     - Airport Performance

**Pipeline:** `Kaggle (raw) → Python (clean) → SQL (model) → Power BI (insights)`

## 4. Key Measures (DAX)

All DAX measures used in the dashboard are stored in:  
`/powerbi/measures.txt`

This project relies on four main families of measures:

1. **Base KPIs** — flight volumes and averages  
2. **Year-over-year comparisons (2019 → 2020)** — tracking operational performance  
3. **Cause-based analytics** — delay minutes and shares by category  
4. **Narrative insights** — dynamic text cards summarizing trends

Only a few representative examples are shown below.  
The full DAX code can be found in the companion file `powerbi/measures.txt`.

---

## 5. Dashboard Pages

The Power BI report is structured into **four analytical pages**, each focusing on a specific layer of the aviation dataset.  
Every page provides a distinct perspective, from global KPIs to detailed breakdowns by delay cause, airline, and airport performance.

All visuals below are extracted from the Power BI file:  
`/powerbi/aviation_analytics.pbix`  
Screenshots are stored in:  
`/reports/aviation_analytics_Page_*.jpg`

---

### 5.1 Overview — Global Performance (2019 vs 2020)

![Overview Dashboard](/reports/aviation_analytics_Page_1.jpg)

**Objective**  
Provide a high-level comparison of flight operations between 2019 and 2020 to measure the impact of the COVID-19 pandemic on U.S. aviation performance.

**Key Visuals & KPIs**
- **Dynamic Insight Text Card (`Insight Overview`)**:  
  Automatically summarizes traffic evolution and delay trends based on filters (Airline, Airport).  
  Example:  
  *“In 2020, total flight traffic decreased by 40.7% compared to 2019. However, operational performance significantly improved: the delay rate fell by 0.42 percentage points.”*
- **Main KPI Section** (3 columns: 2019 | 2020 | Variation YoY):
  - Total Flights  
  - Delay Rate (%)  
  - Average Delay (min)  
  - Cancellation Rate (%)
- **Color Coding:**
  - **Green:** Improvement (delay reduction, efficiency gains)
  - **Red:** Deterioration (traffic or cancellations)

**Filters**
- *Airline*  
- *Airport*  

**Interpretation**
The page shows that while total flight volume dropped by ~40%, operational reliability improved significantly — delay rates decreased by 42%, and the average delay per flight shortened from 67.7 to 57.8 minutes.  
However, the cancellation rate rose slightly (+11.9%), mainly due to schedule disruptions and uncertainty during the pandemic.

---

### 5.2 Delay Causes Analysis

![Delay Causes Analysis](/reports/aviation_analytics_Page_2.jpg)

**Objective**  
Identify and compare the main operational causes of delays and cancellations between 2019 and 2020.  
This page allows users to analyze whether the reduction in total flights during the pandemic was accompanied by structural changes in delay patterns.

---

**Key Visuals**
1. **Delay Share (%) by Year and Cause**  
   - 100% stacked bar chart showing the relative contribution of each cause to total delay minutes.  
   - Causes: *Carrier, Late Aircraft, NAS (National Airspace System), Security, Weather*.  
   - The color legend highlights operational vs. environmental factors.

2. **Total Delay Minutes by Cause (2019 vs 2020)**  
   - Bar chart comparing absolute delay minutes across years.  
   - Shows a dramatic drop for *Late Aircraft* and *Carrier* delays (most operationally linked causes).

3. **Cancelled Flights by Cause and Year**  
   - Matrix combining raw counts and percentages to show which causes led to the most cancellations.  
   - For instance, *Carrier* and *Late Aircraft* represent nearly 12% of all cancellations in 2019.

---

**Filters**
- *Airline*  
- *Airport*  

---

**Interpretation**
Despite the global decrease in air traffic, the distribution of delay causes remained largely similar.  
However, the **total volume of delays collapsed**, with “Late Aircraft” delays dropping from 3.58 million to 0.62 million minutes (–83%).  
This suggests that congestion-related issues fell sharply, while weather and airspace constraints remained proportionally stable.

*Operational takeaway:*  
Efficiency gains in 2020 were not purely random — they stemmed from reduced traffic density and simplified schedules, confirming that “Carrier” and “Late Aircraft” are strongly correlated with flight volume.

---
### 5.3 Airline Performance

![Airline Performance Dashboard](/reports/aviation_analytics_Page_3.jpg)

**Objective**  
Evaluate how major U.S. airlines adapted to the pandemic by comparing their operational reliability in 2019 vs 2020.  
This page isolates airline-level performance to determine which carriers managed to maintain efficiency despite traffic collapse.

---

**Key Visuals**
1. **YoY Change in Delay Rate (%) — Airlines**  
   - Horizontal bar chart comparing the year-over-year variation in delay rates for each carrier.  
   - All major airlines show substantial improvement (green bars), with *Southwest* and *United* achieving the strongest delay reduction.

2. **YoY Change in Cancellation Rate (%) — Airlines**  
   - Red/green bar chart illustrating variations in cancellation rates.  
   - *Delta* and *United* experienced sharp increases, while *SkyWest* and *Southwest* slightly improved.

3. **Airline Summary Table**  
   - Combines 2019 and 2020 flight counts, delay rate variations, and cancellation rate variations for all major U.S. airlines.  
   - Displays both per-year metrics and overall YoY percentage change.

---

**Filters**
- *Airline*  
- *Airport*  

---

**Interpretation**
In 2020, major U.S. airlines saw a **40.7% drop in total flights**, yet most improved their **delay performance** significantly (average –42.2%).  
This improvement was driven by reduced congestion and simplified scheduling during COVID-19 restrictions.  

However, the **cancellation rate** surged by **+12%**, particularly among large network carriers (*Delta*, *United*), due to frequent operational adjustments and unpredictable demand.  
Low-cost carriers such as *Southwest* displayed more stable patterns, reflecting flexible route structures and leaner operations.

*Key takeaway:*  
Airlines adapted to lower demand by optimizing punctuality, but cancellations remained a major operational challenge due to fluctuating travel restrictions.

---
### 5.4 Airport Performance

![Airport Performance Dashboard](/reports/aviation_analytics_Page_4.jpg)

**Objective**  
Analyze the operational impact of COVID-19 across major U.S. airports by comparing delay and cancellation trends between 2019 and 2020.  
This view highlights which hubs managed to sustain reliability under drastically reduced traffic conditions.

---

**Key Visuals**
1. **YoY Change in Delay Rate (%) — Airports**  
   - Green bar chart displaying improvements in delay performance per airport.  
   - *Chicago O’Hare (ORD)* and *Charlotte Douglas (CLT)* showed the most notable improvements, exceeding –45%.

2. **YoY Change in Cancellation Rate (%) — Airports**  
   - Red/green horizontal bar chart showing how cancellation rates evolved.  
   - *Atlanta (ATL)* experienced a sharp rise (+187%), whereas *Charlotte* and *Denver* slightly improved.

3. **Airport Summary Table**  
   - Combines 2019 and 2020 key metrics for the five busiest airports in the dataset:  
     ATL, CLT, ORD, DFW, DEN.  
   - Includes total flights, delay rate variation, and cancellation rate variation, both per year and aggregated.

---

**Filters**
- *Airline*  
- *Airport*  

---

**Interpretation**
U.S. airports recorded a **40.7% decline in total flights**, mirroring the national trend.  
Despite this sharp drop, **delay rates improved** considerably (–42.2%), confirming that lower congestion translated into better on-time performance.  

However, **cancellation rates increased by around +12%**, especially in large hub airports like *Atlanta*, where schedule restructuring and reduced staffing created operational instability.

*Key takeaway:*  
The data shows that while smaller airports remained relatively stable, major hubs faced tougher conditions balancing reduced capacity with dynamic scheduling.

---
## 6. Main Insights

### 6.1 Global Findings

Across the dataset, 2020 marked a turning point for U.S. aviation performance:

- **Total flight traffic fell by 40.7%** compared to 2019, directly reflecting the COVID-19 travel restrictions.  
- Despite this, **operational efficiency improved** — the average **delay rate decreased by 42.2%**, and the **average delay duration dropped by nearly 15%**.  
- However, **cancellations rose by 12%**, indicating that uncertainty and last-minute schedule adjustments continued to disrupt planning.

This combination reveals a paradox:  
While fewer flights reduced congestion and improved punctuality, overall stability suffered due to unpredictable demand and capacity management issues.

---

### 6.2 Airlines Perspective

- **Southwest Airlines** and **SkyWest** achieved the strongest punctuality improvements, benefiting from simplified networks and lean operations.  
- **Delta** and **United**, in contrast, faced sharp increases in cancellation rates (up to +300%), caused by more complex route structures and crew rotations.  
- The results confirm that **smaller or low-cost carriers were more agile** in adapting to the crisis context.

---

### 6.3 Airports Perspective

- Major hubs like **Atlanta (ATL)** and **Chicago O’Hare (ORD)** saw large decreases in delay rates (–27% to –50%), but rising cancellation ratios linked to traffic rescheduling.  
- **Charlotte (CLT)** and **Denver (DEN)** displayed more stable behavior, showing that medium-sized airports better absorbed operational shocks.

---

### 6.4 Strategic Takeaway

The aviation sector’s 2020 data tells a clear story:  
**Lower flight volumes improved punctuality but exposed structural fragilities in cancellation management.**

From a data analytics standpoint, this project demonstrates how integrating **Python, SQL, and Power BI** provides a complete view — from raw operational data to actionable insights for decision-makers.

---

### 6.5 Conclusion

This analysis confirms that:
- **Performance metrics must be interpreted in context.**
- A global crisis can produce apparent operational improvements (less congestion) while hiding deeper systemic challenges (cancellations, coordination issues).  
- Data-driven monitoring remains essential for maintaining resilience during high-uncertainty periods.

The project therefore illustrates not only an analytical result but also a **methodological framework** applicable to any industry facing operational volatility.

---

> **End of Report — Aviation Analytics (2019–2020)**  
> Created with Python, SQL, and Power BI.






