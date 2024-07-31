"""
Author - Mridul Kumar Gangwar
Last updated - 16.05.2024
Purpose - 
C. Use dashboard to visualise the daily average PM2.5 data for a chosen Site from a drop-down list, 
by using a line chart. Users should be able to choose any Sydney site, e.g., via a streamlit selectbox.

Python version: 3.12.3
"""

#Importing required libraries
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras
import streamlit as st
import json # part of standard python library

# Function to connect to the postgresql database using credential from credentials.json file (reference - Lecture slides)
def pgconnect(credential_filepath, df_schema="public"):
  with open(credential_filepath) as f:
    db_conn_dict = json.load(f)
    host = db_conn_dict["database"]['host']
    db_user = db_conn_dict["database"]['user']
    db_pw = db_conn_dict["database"]['password']
    default_db = db_conn_dict['database']["db_name"]
    try:
        db = create_engine('postgresql+psycopg2://' + db_user + ':' + db_pw + '@' + host + '/' + default_db, echo=False)
        conn = db.connect()
        print('Connected Successfully')
    except Exception as e:
        print("Unable to connect to the database")
        print(e)
        db, conn = None, None
    return db,conn

credentials = "Credentials.json"
db, conn = pgconnect(credentials)


# Function to execute a database query
def run_query(db, query, params=None):
    try:
        with db.connect() as conn:
            if params:
                result = pd.read_sql(query, conn, params=params)
            else:
                result = pd.read_sql(query, conn)
        return result
    except Exception as e:
        print("Error running query:", e)
        return None

# Function for fetching data and visualisation
def main(db):
    st.title("Sydney Region PM2.5 Historic Data")

    # Fetch unique sites for the select box
    category_query = """
                    SELECT DISTINCT "SiteName"
                    FROM obs_history
                    """
    categories = run_query(db, category_query)

    if categories is not None and not categories.empty:
        # Displaying categories in a selectbox
        selected_site = st.selectbox("Select a Site", categories['SiteName'])

        # SQL query to fetch data based on the selected site
        data_query = """
                    SELECT obs_history."Date", obs_history."Value"
                    FROM obs_history
                    WHERE "SiteName" = %s;
                    """
        params = (selected_site,)

        # Fetching and displaying data
        data = run_query(db, data_query, params)
        
        data.set_index('Date', inplace=True)
        
        if data is not None and not data.empty:
            st.line_chart(data)
        else:
            st.write("No data available for the selected site.")
    else:
        st.write("No sites found or error in fetching sites.")


if __name__ == "__main__":
    credentials = "Credentials.json"
    db, conn = pgconnect(credentials) 
    if db is not None:
        main(db)
    else:
        print("Failed to connect to the database.")
