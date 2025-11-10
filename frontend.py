import streamlit as st
import requests
import pandas as pd
from io import BytesIO
import os

# --- FastAPI Configuration ---
# Update this if your FastAPI server is running on a different port or host
BACKEND_URL = "http://localhost:8000/api" 

# -------------------- 🌐 Streamlit UI --------------------
st.set_page_config(page_title="Data Processing App", layout="wide")
st.title("📊 Data Processing and QC Automation")

st.markdown("""
This application interfaces with a FastAPI backend to handle both heavy-duty BSR QC processing and Laliga QC.
""")

# --- Use Tabs for Clear Separation ---
qc_tab, sales_tab, market_checks_tab = st.tabs(["✅ Main QC Automation (BSR)", " Laliga ", " F1 Market Specific Checks"])

# --- Define all market check keys globally for management ---
all_market_check_keys = {
    # 1. Channel and Territory Review
    "check_latam_espn": "LATAM ESPN Channels: Ecuador and Venezuela missing",
    "check_italy_mexico": "Italy and Mexico: Duplications/consolidations",
    "check_channel4plus1": "Specific Channel Checks: Channel 4+1",
    "check_espn4_bsa": "ESPN 4: Latam channel extract from BSA",
    "check_f1_obligations": "Formula 1 Obligations: Missing channels", # <--- F1 Check
    "apply_duplication_weights": "Apply Market Duplication and Upweight Rules (Germany, SA, UK, Brazil, etc.)",
    "check_session_completeness": "Session Count Check: Flag duplicate/over-reported Qualifying, Race, or Training sessions",
    "impute_program_type": "Impute Program Type: Assign Live/Repeat/Highlights/Support based on time matching",
    "duration_limits": "Duration Limits Check: Flag broadcasts outside 5 minutes to 5 hours (QC)",
    "live_date_integrity": "Live Session Date Integrity: Check Live Race/Quali/Train against fixed schedule date",
    "update_audience_from_overnight": "Audience Upscale Check: Update BSR with higher Max Overnight data", 
    # NEW CHECK
    "dup_channel_existence": "Duplication Channel Existence: Check if all target channels are in BSR",

    # 2. Broadcaster/Platform Coverage
    "check_youtube_global": "YOUTUBE: ADD YOUTUBE AS PAN-GLOBAL (CPT 8.14)",
    "check_pan_mena": "Pan MENA: BROADCASTER",
    "check_china_tencent": "China Tencent: BROADCASTER",
    "check_czech_slovakia": "Czech Rep and Slovakia: BROADCASTER",
    "check_ant1_greece": "ANT1+ Greece: BROADCASTER (CPT 3.23)",
    "check_india": "India: BROADCASTER",
    "check_usa_espn": "USA ESPN Mail: BROADCASTER",
    "check_dazn_japan": "DAZN Japan: BROADCASTER",
    "check_aztv": "AZTV / IDMAN TV: BROADCASTER",
    "check_rush_caribbean": "RUSH Caribbean: BROADCASTER",
    
    # 3. Removals and Recreations
    "remove_andorra": "Remove Andorra",
    "remove_serbia": "Remove Serbia",
    "remove_montenegro": "Remove Montenegro",
    "remove_brazil_espn_fox": "Remove any ESPN/Fox from Brazil",
    "remove_switz_canal": "Remove Switzerland Canal+ / ServusTV",
    "remove_viaplay_baltics": "Remove viaplay from Latvia, Lithuania, Poland, and Estonia",
    "recreate_viaplay": "Viaplay: Recreate based on a full market of lives",
    "recreate_disney_latam": "Disney+ Latam: Recreate based on a full market of lives",
}


# -----------------------------------------------------------
#        ✅ QC AUTOMATION TAB (UNCHANGED)
# -----------------------------------------------------------

with qc_tab:
    st.header("QC File Uploader")
    st.markdown("""
    Upload your **Rosco**, **BSR**, and (optional) **Client Data file** below. 
    QC checks will be run on the backend, and the result will be available for download.
    """)

    # --- File Upload Section ---
    col1, col2, col3 = st.columns(3)
    with col1:
        rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="rosco")
    with col2:
        bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx"], key="bsr")
    with col3:
        data_file = st.file_uploader("📙 Upload Optional Data File (.xlsx)", type=["xlsx"], key="data")
    
    st.write("---")

    # --- Run Button Logic (Calls Backend /run_qc) ---
    if st.button("🚀 Run QC Checks on Backend"):
        if not rosco_file or not bsr_file:
            st.error("⚠️ Please upload both Rosco and BSR files before running QC.")
        else:
            with st.spinner("Uploading files and running QC checks on backend... Please wait ⏳"):
                
                # 1. Prepare files for multipart/form-data request
                files = {
                    'rosco_file': (rosco_file.name, rosco_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
                    'bsr_file': (bsr_file.name, bsr_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
                }
                
                if data_file:
                    files['data_file'] = (data_file.name, data_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                try:
                    # 2. Make the POST request to the QC endpoint
                    response = requests.post(f"{BACKEND_URL}/run_qc", files=files, timeout=600) 

                    if response.status_code == 200:
                        # 3. Extract filename and serve the downloaded file
                        content_disposition = response.headers.get("Content-Disposition")
                        output_filename = "QC_Result_Download.xlsx" 
                        if content_disposition:
                            try:
                                output_filename = content_disposition.split('filename=')[1].strip('"')
                            except IndexError:
                                pass 
                        
                        st.success("✅ QC completed successfully!")
                        st.download_button(
                            label="📥 Download QC Result Excel",
                            data=response.content, 
                            file_name=output_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        # Handle backend errors
                        try:
                            error_detail = response.json().get("detail", "Unknown error occurred on the backend.")
                        except requests.JSONDecodeError:
                            error_detail = response.text
                            
                        st.error(f"❌ Backend Error ({response.status_code}): {error_detail}")

                except requests.exceptions.RequestException as e:
                    st.error(f"❌ Connection or Timeout Error: Could not reach the backend. Check if FastAPI is running. Error: {e}")

# -----------------------------------------------------------
#         📈 LaligaDATA MANAGEMENT TAB (UNCHANGED)
# -----------------------------------------------------------

with sales_tab:
    st.header("LaligaData Management")
    st.markdown("Upload a new `Sales.csv` file to the backend for analysis.")
    
    st.subheader("1. Upload New LaligaData")
    sales_file = st.file_uploader("📄 Upload LaligaFile (.csv)", type=["csv"], key="sales_csv")
    
    if st.button("⬆️ Upload LaligaCSV to Backend"):
        if sales_file:
            with st.spinner(f"Uploading {sales_file.name}..."):
                
                # Prepare file for upload
                files = {
                    'file': (sales_file.name, sales_file.getbuffer(), 'text/csv'),
                }
                
                try:
                    upload_response = requests.post(f"{BACKEND_URL}/upload_csv", files=files)
                    
                    if upload_response.status_code == 200:
                        st.success(f"✅ Upload successful: {upload_response.json().get('detail')}")
                    else:
                        st.error(f"❌ Upload failed: {upload_response.json().get('detail', 'Unknown error')}")
                        
                except requests.exceptions.RequestException as e:
                    st.error(f"❌ Connection Error: {e}")
        else:
            st.warning("Please select a CSV file to upload.")


    st.subheader("2. Analyze Data")
    
    # --- Summary Button Logic (Improved UI) ---
    if st.button("Get Data Summary"):
        with st.spinner("Fetching summary data from backend..."):
            try:
                summary_response = requests.get(f"{BACKEND_URL}/summary")
                
                if summary_response.status_code == 200:
                    st.success("Summary data retrieved successfully.")
                    
                    # Convert JSON response (list of objects) into a DataFrame for display
                    summary_json = summary_response.json()
                    df_summary = pd.DataFrame.from_records(summary_json)
                    
                    # Set the 'index' column (variable name) as the actual DataFrame index
                    df_summary = df_summary.rename(columns={'index': 'Variable'}).set_index('Variable')
                    
                    st.dataframe(df_summary.style.format('{:.2f}'), width=True)
                    
                else:
                    st.error(f"❌ Failed to retrieve summary: {summary_response.json().get('detail', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Connection Error: {e}")
            except Exception as e:
                 st.error(f"❌ Data Parsing Error: Failed to process summary JSON. Ensure CSV headers are correct. Error: {e}")

    # --- KPI Retrieval Section (Improved UI) ---
    st.markdown("---")
    country_query = st.text_input("Enter Country for KPI Analysis (Optional)", key="kpi_country")
    if st.button("Get KPIs"):
        with st.spinner(f"Fetching KPIs for {country_query if country_query else 'all markets'}..."):
            params = {}
            if country_query:
                params['country'] = country_query
                
            try:
                kpi_response = requests.get(f"{BACKEND_URL}/kpis", params=params)
                
                if kpi_response.status_code == 200:
                    st.success("KPIs retrieved successfully.")
                    kpi_data = kpi_response.json()
                    
                    # Display KPIs using st.metric in three columns
                    kpi_cols = st.columns(4)
                    
                    # Use a dictionary mapping keys to display names and icons
                    kpi_map = {
                        "total_revenue": ("Total Revenue", "💰"),
                        "total_profit": ("Total Profit", "✅"),
                        "total_cost": ("Total Cost", "📉"),
                        "number_of_purchases": ("Transactions", "🛒")
                    }
                    
                    for i, (key, (label, icon)) in enumerate(kpi_map.items()):
                        value_str = kpi_data.get(key, 'N/A')
                        # Format number string for better display, removing str() needed for backend
                        try:
                            value = float(value_str)
                            # Simple formatting for currency (assuming US-style currency)
                            formatted_value = f"{icon} {value:,.0f}" if 'revenue' in key or 'profit' in key or 'cost' in key else f"{icon} {value:,.0f}"
                        except ValueError:
                            formatted_value = value_str

                        with kpi_cols[i]:
                            st.metric(label=label, value=formatted_value)
                            
                else:
                    st.error(f"❌ Failed to retrieve KPIs: {kpi_response.json().get('detail', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Connection Error: {e}")

# -----------------------------------------------------------
#         🌍 MARKET SPECIFIC CHECKS TAB (MODIFIED)
# -----------------------------------------------------------
with market_checks_tab:
    st.header("🌍 Market Specific Checks & Channel Configuration")
    st.markdown("Upload the **BSR file** and the **F1 Obligation file** here to perform and log manual checks.")

    # --- Dedicated Upload for Manual Checks (MODIFIED) ---
    col_file1, col_file2, col_file3,col_file4 = st.columns(4) # <-- Increase columns to 3
    with col_file1:
        market_check_file = st.file_uploader("📥 Upload BSR File for Checks (.xlsx)", type=["xlsx"], key="market_check_file")
    with col_file2:
        obligation_file = st.file_uploader("📄 Upload F1 Obligation File (.xlsx)", type=["xlsx"], key="obligation_file")
    with col_file3: # <-- NEW UPLOADER
        overnight_file = st.file_uploader("📈 Upload Overnight Audience File (.xlsx)", type=["xlsx"], key="overnight_file") # <-- NEW
    with col_file4: # <-- NEW UPLOADER
        macro_file = st.file_uploader("📋 4. BSA Duplicator File (Existence Check)", type=["xlsm", "xlsx"], key="macro_file") # <-- NEW
    
    st.write("---")

    # Initialize check states in session_state if not present
    for key in all_market_check_keys.keys():
        if key not in st.session_state:
            st.session_state[key] = False

    # --- Checkbox UI generation (unchanged) ---
    with st.expander("1. Channel and Territory Review", expanded=True):
        st.subheader("General Market Checks")
        st.checkbox(all_market_check_keys["check_latam_espn"], key="check_latam_espn")
        st.checkbox(all_market_check_keys["check_italy_mexico"], key="check_italy_mexico")
        
        st.subheader("Specific Channel Checks (against uploaded file)")
        st.checkbox(all_market_check_keys["check_channel4plus1"], key="check_channel4plus1")
        st.checkbox(all_market_check_keys["check_espn4_bsa"], key="check_espn4_bsa")
        st.checkbox(all_market_check_keys["check_f1_obligations"], key="check_f1_obligations") # <--- F1 Check
        st.checkbox(all_market_check_keys["apply_duplication_weights"], key="apply_duplication_weights") # <--- F1 Check
        st.checkbox(all_market_check_keys["check_session_completeness"], key="check_session_completeness")
        st.checkbox(all_market_check_keys["impute_program_type"], key="impute_program_type")
        st.checkbox(all_market_check_keys["duration_limits"], key="duration_limits")
        st.checkbox(all_market_check_keys["live_date_integrity"], key="live_date_integrity")
        st.checkbox(all_market_check_keys["update_audience_from_overnight"], key="update_audience_from_overnight") # <-- NEW
        
        st.checkbox(all_market_check_keys["dup_channel_existence"], key="dup_channel_existence") # <-- NEW CHECKBOX

    # ... (rest of the checkboxes remain here) ...
    with st.expander("2. Broadcaster/Platform Coverage (BROADCASTER/GLOBAL)"):
        st.subheader("Global/Platform Adds")
        st.checkbox(all_market_check_keys["check_youtube_global"], key="check_youtube_global")
        
        st.subheader("Individual Broadcaster Confirmations")
        st.checkbox(all_market_check_keys["check_pan_mena"], key="check_pan_mena")
        st.checkbox(all_market_check_keys["check_china_tencent"], key="check_china_tencent")
        st.checkbox(all_market_check_keys["check_czech_slovakia"], key="check_czech_slovakia")
        st.checkbox(all_market_check_keys["check_ant1_greece"], key="check_ant1_greece")
        st.checkbox(all_market_check_keys["check_india"], key="check_india")
        st.checkbox(all_market_check_keys["check_usa_espn"], key="check_usa_espn")
        st.checkbox(all_market_check_keys["check_dazn_japan"], key="check_dazn_japan")
        st.checkbox(all_market_check_keys["check_aztv"], key="check_aztv")
        st.checkbox(all_market_check_keys["check_rush_caribbean"], key="check_rush_caribbean")


    with st.expander("3. Removals and Recreations"):
        st.subheader("Removals (Ensure these are absent)")
        st.checkbox(all_market_check_keys["remove_andorra"], key="remove_andorra")
        st.checkbox(all_market_check_keys["remove_serbia"], key="remove_serbia")
        st.checkbox(all_market_check_keys["remove_montenegro"], key="remove_montenegro")
        st.checkbox(all_market_check_keys["remove_brazil_espn_fox"], key="remove_brazil_espn_fox")
        st.checkbox(all_market_check_keys["remove_switz_canal"], key="remove_switz_canal")
        st.checkbox(all_market_check_keys["remove_viaplay_baltics"], key="remove_viaplay_baltics")

        st.subheader("Recreations (Check for full market coverage)")
        st.checkbox(all_market_check_keys["recreate_viaplay"], key="recreate_viaplay")
        st.checkbox(all_market_check_keys["recreate_disney_latam"], key="recreate_disney_latam")
        
    st.write("---")


    # --- Run Processing Button (MODIFIED LOGIC) ---
    if st.button("⚙️ Apply Selected Checks"):
        
        active_checks = [key for key in all_market_check_keys.keys() if st.session_state[key]]
        
        # Check mandatory files
        if market_check_file is None:
            st.error("⚠️ Please upload a BSR file before applying checks.")
        elif "check_f1_obligations" in active_checks and obligation_file is None:
            st.error("⚠️ **F1 Obligation Check Selected:** Please upload the F1 Obligation File.")
        elif "update_audience_from_overnight" in active_checks and overnight_file is None: # <-- NEW CHECK
            st.error("⚠️ Audience Upscale Check Selected: Please upload the Overnight Audience File.") # <-- NEW ERROR MESSAGE
        elif "dup_channel_existence" in active_checks and macro_file is None: # <-- NEW DEPENDENCY CHECK
            st.error("⚠️ Duplication Channel Existence Check Selected: Please upload the BSA Macro Duplicator File.")
        else:
            with st.spinner(f"Applying {len(active_checks)} checks on the backend..."):
                
                # 2. Prepare files for backend
                files = {
                    'bsr_file': (market_check_file.name, market_check_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                }
                
                # CONDITIONAL ADDITION OF OBLIGATION FILE
                if obligation_file:
                    files['obligation_file'] = (obligation_file.name, obligation_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                # CONDITIONAL ADDITION OF OVERNIGHT FILE <--- NEW LOGIC
                if overnight_file:
                    files['overnight_file'] = (overnight_file.name, overnight_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                if macro_file: # <-- ADD NEW FILE TO REQUEST
                    files['macro_file'] = (macro_file.name, macro_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                # Send active checks as form data
                data = {'checks': active_checks} 

                try:
                    # 3. Call the backend endpoint
                    response = requests.post(
                        f"{BACKEND_URL}/market_check_and_process", 
                        files=files, 
                        data=data,
                        timeout=600
                    )

                    if response.status_code == 200:
                        # 4. Success: Handle the JSON response (unchanged)
                        try:
                            result_json = response.json()
                            summaries = result_json.get("summaries", [])
                            download_url_suffix = result_json.get("download_url")
                            message = result_json.get("message", "Processing complete.")
                            
                            # Construct the full download URL using the base URL
                            full_download_url = f"http://localhost:8000{download_url_suffix}"

                            st.success(f"✅ Checks completed successfully! {message}")
                            
                            # --- Display Summaries ---
                            st.subheader("Processing Summary")
                            if summaries:
                                # Convert the list of dicts into a DataFrame for clean display
                                df_summary = pd.DataFrame(summaries)
                                
                                df_summary_display = df_summary.copy()

                                # Extract key details from the 'details' column 
                                if 'details' in df_summary.columns:
                                    
                                    # Use 'market_affected' if present, otherwise use 'markets_context'
                                    df_summary_display['Market'] = df_summary['details'].apply(
                                        lambda d: d.get('market_affected', d.get('markets_context', 'Global/N/A'))
                                    )
                                    
                                    # --- Create a unified 'Change Count' column ---
                                    def get_change_count(d):
                                        if 'rows_removed' in d: return d['rows_removed']
                                        if 'total_issues_flagged' in d: return d['total_issues_flagged']
                                        if 'rows_added' in d: return d['rows_added']
                                        
                                        # NEW: Check for the 'broadcasters_missing' count from F1 check
                                        if 'broadcasters_missing' in d: return d['broadcasters_missing'] 
                                        
                                        return 0
                                        
                                    # Apply the unified logic
                                    df_summary_display['Change Count'] = df_summary['details'].apply(get_change_count)
                                    
                                    df_summary_display = df_summary_display.rename(columns={
                                        "description": "Operation", 
                                        "status": "Status"
                                    })
                                    
                                    # Final column selection
                                    df_summary_display = df_summary_display[[
                                        'Status', 
                                        'Operation', 
                                        'Market', 
                                        'Change Count', 
                                        'check_key'
                                    ]].set_index('check_key')
                                else:
                                    # Fallback if no details column exists 
                                    df_summary_display = df_summary_display.rename(columns={
                                        "description": "Operation", 
                                        "status": "Status"
                                    })
                                    if 'check_key' in df_summary_display.columns:
                                            df_summary_display = df_summary_display[['Status', 'Operation', 'check_key']].set_index('check_key')
                                            
                                st.dataframe(df_summary_display, use_container_width=True)
                                
                                # --- Display Duplicates Dataframe (UNCHANGED) ---
                                dupe_summary = next((s for s in summaries if s.get('check_key') == 'check_italy_mexico' and s['details'].get('duplicate_data')), None)
                                
                                if dupe_summary and dupe_summary['details']['duplicate_data']:
                                    duplicate_data = dupe_summary['details']['duplicate_data']
                                    st.subheader("⚠️ Duplicate Rows Found and Consolidated (Italy/Mexico)")
                                    
                                    duplicates_df = pd.DataFrame(duplicate_data)
                                    st.dataframe(duplicates_df, use_container_width=True)
                                    st.caption(
                                        f"The table above shows {len(duplicates_df)} rows involved in the duplicate sets (including the one kept). "
                                        f"**{dupe_summary['details'].get('rows_removed', 0)}** rows were removed."
                                    )

                            else:
                                st.info("No specific operational summaries were returned.")

                            # --- Provide Download Button (UNCHANGED) ---
                            if download_url_suffix:
                                st.markdown("---")
                                st.markdown(
                                    f'### 📥 Download Processed File <a href="{full_download_url}" download>Click Here to Download</a>',
                                    unsafe_allow_html=True
                                )
                            else:
                                st.warning("Processed file download link was not generated. Check backend logs.")

                        except (requests.JSONDecodeError, KeyError) as e:
                            st.error(f"❌ Failed to parse JSON response from backend. Error: {e}")
                        
                    else:
                        # 5. Handle Backend Error
                        try:
                            error_detail = response.json().get("detail", "Unknown error occurred during check execution.")
                        except requests.JSONDecodeError:
                            error_detail = response.text
                        st.error(f"❌ Backend Processing Error ({response.status_code}): {error_detail}")

                except requests.exceptions.RequestException as e:
                    st.error(f"❌ Connection Error: Could not reach the backend. Error: {e}")