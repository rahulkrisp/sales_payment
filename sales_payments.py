import streamlit as st
import pandas as pd
import requests
import json
import datetime
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()


api_key = os.getenv("MIXPANEL_API_KEY") or st.secrets.get("MIXPANEL_API_KEY")
project_id = os.getenv("MIXPANEL_PROJECT_ID") or st.secrets.get("MIXPANEL_PROJECT_ID")


st.title("Sales Payment Data")

# Use session state to store the fetched payment data
if 'payment_data' not in st.session_state:
    st.session_state.payment_data = None

# 1. Date input - CORRECTED DEFAULT DATES
# Set the default end_date to today and the start_date to 7 days ago
today = datetime.date.today()
seven_days_ago = today - datetime.timedelta(days=7)

start_date = st.date_input("Select start date", seven_days_ago)
end_date = st.date_input("Select end date", today)

# Add a check to ensure the end date is not in the future
if start_date > today or end_date > today:
    st.warning("Dates cannot be in the future. Please select a valid date range.")
elif start_date > end_date:
    st.error("Error: Start date cannot be after the end date.")
else:
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    if st.button("Filter"):
        try:
            with st.spinner('Fetching and processing data...'):
                events_to_export = ["New Payment Made"]
                # The event parameter should be a JSON string, which your code does correctly.
                # No changes needed here.
                event_array_json = json.dumps(events_to_export)

                url = (
                    f"https://data-eu.mixpanel.com/api/2.0/export?project_id={project_id}&from_date={start_date_str}&to_date={end_date_str}&event="
                    + event_array_json
                )

                headers = {
                    "accept": "text/plain",
                    "authorization": f"Basic {api_key}",
                }

                response = requests.get(url, headers=headers)
                response.raise_for_status()

                data_lines = response.text.strip().split("\n")

                # Check if the API returned any data
                if not data_lines or not data_lines[0]:
                    st.warning("No data returned from the API for the selected date range.")
                    st.session_state.payment_data = None # Clear previous data
                else:
                    data_json = [json.loads(line) for line in data_lines]
                    
                    certain_json1 = pd.DataFrame(data_json)
                    properties_df1 = pd.json_normalize(certain_json1['properties'])
                    payments = pd.concat([certain_json1.drop(columns=['properties']), properties_df1], axis=1)
                    payments = payments.drop_duplicates('$insert_id')
                    
                    payment = payments.copy()
                    payment['email'] = payment.apply(
                        lambda row: str(row['distinct_id']) if '@' in str(row.get('distinct_id')) 
                        else (str(row.get('$distinct_id_before_identity')) if '@' in str(row.get('$distinct_id_before_identity')) else None),
                        axis=1
                    )
                    
                    payment = payment.dropna(subset=['email'])
                    payment = payment[['email', 'Amount']]
                    payment['Amount'] = pd.to_numeric(payment['Amount'], errors='coerce')
                    payment = payment.groupby('email', as_index=False)['Amount'].sum()
                    payment.sort_values(by='Amount', ascending=False, inplace=True)
                    
                    st.session_state.payment_data = payment

        except requests.exceptions.HTTPError as e:
            st.error(f"API Request Error: {e}")
            st.info("This might be due to an invalid date range (e.g., dates in the future) or incorrect API credentials/project ID.")
        except Exception as e:
            st.error(f"An error occurred during data processing: {e}")


# Display the filtered data if it exists in the session state
if st.session_state.payment_data is not None:
    st.subheader("Filtered Payment Data")
    st.write(st.session_state.payment_data.head())

    # 2. File Uploader
    st.subheader("Upload File to Merge with Filtered Data")
    uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=["csv", "xlsx"])

    if uploaded_file is not None:
        try:
            # Read Excel or CSV
            if uploaded_file.name.endswith('.xlsx'):
                user_df = pd.read_excel(uploaded_file)
            else:
                # Read CSV with fallback encoding
                try:
                    user_df = pd.read_csv(uploaded_file, encoding='utf-8')
                except UnicodeDecodeError:
                    uploaded_file.seek(0)
                    user_df = pd.read_csv(uploaded_file, encoding='latin1')

            st.subheader("Uploaded File Preview")
            st.write(user_df.head())

            # Create a unified email column
            # Ensure the columns exist before trying to combine them
            email_cols = ['Person - Email - Work', 'Person - Email - Other']
            existing_email_cols = [col for col in email_cols if col in user_df.columns]
            
            if not existing_email_cols:
                 st.error("Uploaded file must contain 'Person - Email - Work' or 'Person - Email - Other' column.")
            else:
                user_df['email'] = user_df[existing_email_cols[0]]
                if len(existing_email_cols) > 1:
                    user_df['email'] = user_df[existing_email_cols[0]].combine_first(user_df[existing_email_cols[1]])

                # Perform the merge
                merged_df = pd.merge(user_df, st.session_state.payment_data, on='email', how='left')
                merged_df['Amount'] = merged_df['Amount'].fillna(0) # Fill NaN with 0 for users with no payments

                st.subheader("Merged Data")
                st.write(merged_df)

                st.subheader("Total Amount by Deal Owner")
                # Check if the 'Deal - Owner' column exists before grouping
                if 'Deal - Owner' in merged_df.columns:
                    # Group by 'Deal - Owner' and sum the 'Amount'
                    owner_summary_df = merged_df.groupby('Deal - Owner')['Amount'].sum().reset_index()
                    
                    # Sort the results for better readability
                    owner_summary_df.sort_values(by='Amount', ascending=False, inplace=True)
                    
                    # Display the new aggregated dataframe
                    st.write(owner_summary_df)
                else:
                    st.warning("The column 'Deal - Owner' was not found in the uploaded file, so the summary cannot be generated.")

                # Option to download the merged data
                csv = merged_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Download Merged CSV",
                    csv,
                    "merged_output.csv",
                    "text/csv",
                    key='download-csv'
                )

        except Exception as e:
            st.error(f"Error reading or processing file: {e}")