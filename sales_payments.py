import streamlit as st
import pandas as pd
import requests
import json
import datetime
from dotenv import load_dotenv
import os

# Load API credentials from Streamlit secrets
api_key = st.secrets["MIXPANEL_API_KEY"]
project_id = st.secrets["MIXPANEL_PROJECT_ID"]

st.title("Sales Payment and Refund Data")

# Use session state to store the fetched data
if 'combined_data' not in st.session_state:
    st.session_state.combined_data = None

# 1. Date input
today = datetime.date.today()
seven_days_ago = today - datetime.timedelta(days=7)

start_date = st.date_input("Select start date", seven_days_ago)
end_date = st.date_input("Select end date", today)

# Helper function to fetch and process Mixpanel data
def fetch_mixpanel_data(events, start_date_str, end_date_str):
    """Fetches data for specified events from Mixpanel and returns a DataFrame."""
    event_array_json = json.dumps(events)
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
    if not data_lines or not data_lines[0]:
        return pd.DataFrame() # Return empty DataFrame if no data
    data_json = [json.loads(line) for line in data_lines]
    df = pd.DataFrame(data_json)
    properties_df = pd.json_normalize(df['properties'])
    combined_df = pd.concat([df.drop(columns=['properties']), properties_df], axis=1)
    return combined_df.drop_duplicates('$insert_id')


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
                # Fetch Payment Data
                payments_df = fetch_mixpanel_data(["New Payment Made"], start_date_str, end_date_str)
                if not payments_df.empty:
                    payments_df['email'] = payments_df.apply(
                        lambda row: str(row.get('distinct_id')) if '@' in str(row.get('distinct_id')) 
                        else (str(row.get('$distinct_id_before_identity')) if '@' in str(row.get('$distinct_id_before_identity')) else None),
                        axis=1
                    )
                    payments_df = payments_df.dropna(subset=['email'])
                    payments_df = payments_df[['email', 'Amount']]
                    payments_df['Amount'] = pd.to_numeric(payments_df['Amount'], errors='coerce')
                    payments_df = payments_df.groupby('email', as_index=False)['Amount'].sum()
                
                # Fetch Refund Data
                refunds_df = fetch_mixpanel_data(["Refund Granted"], start_date_str, end_date_str)
                if not refunds_df.empty:
                    # Rename columns to match expected output before processing
                    refunds_df.rename(columns={'User Email': 'email'}, inplace=True)
                    refunds_df = refunds_df.dropna(subset=['email'])
                    refunds_df = refunds_df[['email', 'Refund Amount']]
                    refunds_df['Refund Amount'] = pd.to_numeric(refunds_df['Refund Amount'], errors='coerce')
                    refunds_df = refunds_df.groupby('email', as_index=False)['Refund Amount'].sum()

                # Merge payment and refund data
                if not payments_df.empty and not refunds_df.empty:
                    combined_data = pd.merge(payments_df, refunds_df, on='email', how='outer')
                elif not payments_df.empty:
                    combined_data = payments_df
                    combined_data['Refund Amount'] = 0
                elif not refunds_df.empty:
                    combined_data = refunds_df
                    combined_data['Amount'] = 0
                else:
                    combined_data = pd.DataFrame(columns=['email', 'Amount', 'Refund Amount'])

                combined_data.fillna(0, inplace=True)
                
                if combined_data.empty:
                     st.warning("No payment or refund data returned from the API for the selected date range.")
                     st.session_state.combined_data = None
                else:
                    st.session_state.combined_data = combined_data

        except requests.exceptions.HTTPError as e:
            st.error(f"API Request Error: {e}")
            st.info("This might be due to an invalid date range or incorrect API credentials/project ID.")
        except Exception as e:
            st.error(f"An error occurred during data processing: {e}")


# Display the filtered data if it exists in the session state
if st.session_state.combined_data is not None:
    st.subheader("Filtered Payment and Refund Data")
    st.write(st.session_state.combined_data.head())

    # 2. File Uploader
    st.subheader("Upload File to Merge with Filtered Data")
    uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=["csv", "xlsx"])

    if uploaded_file is not None:
        try:
            # Read Excel or CSV
            if uploaded_file.name.endswith('.xlsx'):
                user_df = pd.read_excel(uploaded_file)
            else:
                try:
                    user_df = pd.read_csv(uploaded_file, encoding='utf-8')
                except UnicodeDecodeError:
                    uploaded_file.seek(0)
                    user_df = pd.read_csv(uploaded_file, encoding='latin1')

            st.subheader("Uploaded File Preview")
            st.write(user_df.head())

            # Create a unified email column
            email_cols = ['Person - Email - Work', 'Person - Email - Other']
            existing_email_cols = [col for col in email_cols if col in user_df.columns]
            
            if not existing_email_cols:
                 st.error("Uploaded file must contain 'Person - Email - Work' or 'Person - Email - Other' column.")
            else:
                user_df['email'] = user_df[existing_email_cols[0]]
                if len(existing_email_cols) > 1:
                    user_df['email'] = user_df[existing_email_cols[0]].combine_first(user_df[existing_email_cols[1]])

                # Perform the merge
                merged_df = pd.merge(user_df, st.session_state.combined_data, on='email', how='left')
                merged_df[['Amount', 'Refund Amount']] = merged_df[['Amount', 'Refund Amount']].fillna(0)

                st.subheader("Merged Data")
                st.write(merged_df)

                st.subheader("Total Amount by Deal Owner")
                if 'Deal - Owner' in merged_df.columns:
                    # Group by 'Deal - Owner' and sum the 'Amount' and 'Refund Amount'
                    owner_summary_df = merged_df.groupby('Deal - Owner')[['Amount', 'Refund Amount']].sum().reset_index()
                    owner_summary_df.sort_values(by='Amount', ascending=False, inplace=True)
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