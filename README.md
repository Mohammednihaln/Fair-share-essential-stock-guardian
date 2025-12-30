# Fair-Share Essential Stock Guardian

## Overview
Fair-Share Essential Stock Guardian is a Snowflake-native AI-for-Good application
that predicts stock-outs of essential goods and prioritizes fair distribution
to vulnerable regions.

## Problem
Hospitals and NGOs often face shortages of medicines and food.
Existing systems detect issues late and lack fairness in allocation.

## Solution
The solution combines stock risk with population vulnerability to:
- Predict stock-outs early
- Prioritize high-impact locations
- Recommend reorder and redistribution actions
- Explain decisions in plain language

## Architecture
- Snowflake SQL & Worksheets
- Dynamic Tables for auto-refresh
- Streams & Tasks for automation
- Streamlit in Snowflake for visualization

## How It Works
1. Daily stock and vulnerability data are ingested
2. Stock health metrics are calculated
3. Fair priority scores are generated
4. Action recommendations are produced
5. Results are visualized in a Streamlit dashboard

## Running the App
This application is designed to run **inside Snowflake** using
**Streamlit in Snowflake**.

Steps:
1. Execute SQL files in order from `/sql`
2. Create a Streamlit app in Snowflake
3. Paste `streamlit/app.py` into the editor
4. Run the app

## AI for Good Impact
- Prevents stock-outs of critical supplies
- Ensures vulnerable populations are prioritized
- Enables transparent, ethical decision-making

## Tech Stack
Snowflake SQL, Dynamic Tables, Streams & Tasks, Streamlit

