import streamlit as st
import pandas as pd
import os
import time
import shutil
import json
from typing import Optional, List

# --- Import ALL QC functions from ALL your files ---

# Your colleague's original F1/QC functions
try:
    from qc_checks import (
        detect_period_from_rosco as rosco_detect_orig, # Alias to avoid conflict
        load_bsr as load_bsr_orig,
        period_check as period_check_orig,
        completeness_check as completeness_check_orig,
        overlap_duplicate_daybreak_check as overlap_orig,
        program_category_check as program_cat_orig,
        duration_check as duration_orig,
        check_event_matchday_competition as event_matchday_orig,
        market_channel_program_duration_check as market_channel_orig,
        domestic_market_coverage_check as domestic_orig,
        rates_and_ratings_check as rates_orig,
        duplicated_markets_check as duplicated_orig,
        country_channel_id_check as country_id_orig,
        client_lstv_ott_check as client_lstv_orig,
        color_excel as color_excel_orig,
        generate_summary_sheet as summary_orig,
    )
    from C_data_processing_f1 import BSRValidator
except ImportError as e:
    st.error(f"Failed to import colleague's files (qc_checks.py, C_data_processing_f1.py): {e}")
    st.stop()


# Your 11-check QC functions
try:
    import qc_checks_1 as qc_general
except ImportError as e:
    st.error(f"Failed to import your QC file (qc_checks_1.py): {e}")
    st.stop()


# -------------------- ⚙️ Folder setup --------------------
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------- 🧠 Config Loader --------------------
# Helper function to load the config.json file
@st.cache_data
def load_config():
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        return config
    except Exception as e:
        st.error(f"FATAL ERROR: Could not load config.json. {e}")
        return None

config = load_config()
if config is None:
    st.stop()

# -------------------- 🌐 Streamlit UI --------------------
st.set_page_config(page_title="Data Processing App", layout="wide")
st.title("📊 Data Processing and QC Automation")

st.markdown("""
This application runs all QC checks directly. You can run all three processes simultaneously.
""")

# --- Use Tabs for Clear Separation ---
main_qc_tab, laliga_qc_tab, f1_tab = st.tabs([
    "✅ Main QC Automation", 
    "⚽ Laliga Specific QC", 
    "🏎️ F1 Market Specific Checks"
])

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
#        ✅ MAIN QC AUTOMATION TAB (YOUR 9 CHECKS)
# -----------------------------------------------------------

with main_qc_tab:
    st.header("QC File Uploader")
    st.markdown("Upload your **Rosco** and **BSR** files below. This will run the 9 general QC checks.")

    col1, col2 = st.columns(2)
    with col1:
        main_rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="main_rosco")
    with col2:
        main_bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx"], key="main_bsr")
    
    st.write("---")

    if st.button("🚀 Run General QC Checks"):
        if not main_rosco_file or not main_bsr_file or not config:
            st.error("⚠️ Please upload both Rosco and BSR files (and ensure config.json is loaded).")
        else:
            with st.spinner("Running General QC checks... Please wait ⏳"):
                try:
                    # Load config
                    col_map = config["column_mappings"]
                    rules = config["qc_rules"]
                    file_rules = config["file_rules"]
                    
                    # Save files temporarily
                    rosco_path = os.path.join(UPLOAD_FOLDER, main_rosco_file.name)
                    bsr_path = os.path.join(UPLOAD_FOLDER, main_bsr_file.name)
                    with open(rosco_path, "wb") as f: f.write(main_rosco_file.getbuffer())
                    with open(bsr_path, "wb") as f: f.write(main_bsr_file.getbuffer())

                    # --- Run YOUR 9 QC Checks Directly ---
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])
                    
                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"])
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"])
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules["client_check"])

                    # --- Generate Output File ---
                    output_file = f"General_QC_Result_{os.path.splitext(main_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="QC Results")

                    qc_general.color_excel(output_path, df)
                    qc_general.generate_summary_sheet(output_path, df, file_rules)
                    
                    st.success("✅ General QC completed successfully!")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download General QC Result",
                            data=f,
                            file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f"❌ An error occurred during General QC: {e}")


# -----------------------------------------------------------
#         ⚽ LALIGA QC TAB (YOUR 11 CHECKS)
# -----------------------------------------------------------

with laliga_qc_tab:
    st.header("⚽ Laliga Specific QC Checks")
    st.markdown("Upload your **Rosco**, **BSR**, and **Macro Duplicator** files. This will run all 11 QC checks.")

    col1, col2, col3 = st.columns(3)
    with col1:
        laliga_rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="laliga_rosco")
    with col2:
        laliga_bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx"], key="laliga_bsr")
    with col3:
        laliga_macro_file = st.file_uploader("📒 Upload Macro Duplicator File", type=["xlsx","xls","xlsm","xlsb"], key="laliga_macro")
    
    st.write("---")

    if st.button("⚙️ Run Laliga QC Checks"):
        if not laliga_rosco_file or not laliga_bsr_file or not laliga_macro_file or not config:
            st.error("⚠️ Please upload all three files (and ensure config.json is loaded).")
        else:
            with st.spinner("Running all 11 Laliga QC checks..."):
                try:
                    # Load config
                    col_map = config["column_mappings"]
                    rules = config["qc_rules"]
                    project = config["project_rules"]
                    file_rules = config["file_rules"]
                    
                    # Save files temporarily
                    rosco_path = os.path.join(UPLOAD_FOLDER, laliga_rosco_file.name)
                    bsr_path = os.path.join(UPLOAD_FOLDER, laliga_bsr_file.name)
                    macro_path = os.path.join(UPLOAD_FOLDER, laliga_macro_file.name)
                    with open(rosco_path, "wb") as f: f.write(laliga_rosco_file.getbuffer())
                    with open(bsr_path, "wb") as f: f.write(laliga_bsr_file.getbuffer())
                    with open(macro_path, "wb") as f: f.write(laliga_macro_file.getbuffer())
                    
                    # --- Run YOUR 11 QC Checks Directly ---
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    # Run the 9 General Checks
                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"])
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"])
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules["client_check"])
                    
                    # Run the 2 Laliga-Specific Checks
                    df = qc_general.domestic_market_check(df, project, col_map["bsr"], debug=True)
                    df = qc_general.duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=True)

                    # --- Generate Output File ---
                    output_file = f"Laliga_QC_Result_{os.path.splitext(laliga_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="Laliga QC Results")

                    qc_general.color_excel(output_path, df)
                    qc_general.generate_summary_sheet(output_path, df, file_rules)
                    
                    st.success("✅ Laliga QC completed successfully!")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Laliga QC Result",
                            data=f,
                            file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f"❌ An error occurred during Laliga QC: {e}")

# -----------------------------------------------------------
#         🏎️ F1 MARKET SPECIFIC CHECKS TAB (COLLEAGUE'S LOGIC)
# -----------------------------------------------------------
with f1_tab:
    st.header("🌍 Market Specific Checks & Channel Configuration")
    st.markdown("Upload the **BSR file** and the **F1 Obligation file** here to perform and log manual checks.")

    col_file1, col_file2, col_file3,col_file4 = st.columns(4)
    with col_file1:
        f1_bsr_file = st.file_uploader("📥 Upload BSR File for Checks (.xlsx)", type=["xlsx"], key="market_check_file")
    with col_file2:
        f1_obligation_file = st.file_uploader("📄 Upload F1 Obligation File (.xlsx)", type=["xlsx"], key="obligation_file")
    with col_file3:
        f1_overnight_file = st.file_uploader("📈 Upload Overnight Audience File (.xlsx)", type=["xlsx"], key="overnight_file")
    with col_file4:
        f1_macro_file = st.file_uploader("📋 4. BSA Duplicator File (Existence Check)", type=["xlsm", "xlsx"], key="macro_file")
    
    st.write("---")

    for key in all_market_check_keys.keys():
        if key not in st.session_state:
            st.session_state[key] = False

    with st.expander("1. Channel and Territory Review", expanded=True):
        st.subheader("General Market Checks")
        st.checkbox(all_market_check_keys["check_latam_espn"], key="check_latam_espn")
        st.checkbox(all_market_check_keys["check_italy_mexico"], key="check_italy_mexico")
        st.subheader("Specific Channel Checks (against uploaded file)")
        st.checkbox(all_market_check_keys["check_channel4plus1"], key="check_channel4plus1")
        st.checkbox(all_market_check_keys["check_espn4_bsa"], key="check_espn4_bsa")
        st.checkbox(all_market_check_keys["check_f1_obligations"], key="check_f1_obligations") 
        st.checkbox(all_market_check_keys["apply_duplication_weights"], key="apply_duplication_weights") 
        st.checkbox(all_market_check_keys["check_session_completeness"], key="check_session_completeness")
        st.checkbox(all_market_check_keys["impute_program_type"], key="impute_program_type")
        st.checkbox(all_market_check_keys["duration_limits"], key="duration_limits")
        st.checkbox(all_market_check_keys["live_date_integrity"], key="live_date_integrity")
        st.checkbox(all_market_check_keys["update_audience_from_overnight"], key="update_audience_from_overnight") 
        st.checkbox(all_market_check_keys["dup_channel_existence"], key="dup_channel_existence")

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

    if st.button("⚙️ Apply Selected Checks"):
        
        active_checks = [key for key in all_market_check_keys.keys() if st.session_state[key]]
        
        if f1_bsr_file is None:
            st.error("⚠️ Please upload a BSR file before applying checks.")
        elif "check_f1_obligations" in active_checks and f1_obligation_file is None:
            st.error("⚠️ **F1 Obligation Check Selected:** Please upload the F1 Obligation File.")
        elif "update_audience_from_overnight" in active_checks and f1_overnight_file is None:
            st.error("⚠️ Audience Upscale Check Selected: Please upload the Overnight Audience File.")
        elif "dup_channel_existence" in active_checks and f1_macro_file is None:
            st.error("⚠️ Duplication Channel Existence Check Selected: Please upload the BSA Macro Duplicator File.")
        else:
            with st.spinner(f"Applying {len(active_checks)} checks..."):
                # --- THIS IS THE START OF THE TRY BLOCK ---
                try:
                    # --- Save files temporarily ---
                    bsr_file_path = os.path.join(UPLOAD_FOLDER, f1_bsr_file.name)
                    with open(bsr_file_path, "wb") as f: f.write(f1_bsr_file.getbuffer())
                    
                    obligation_path = None
                    if f1_obligation_file:
                        obligation_path = os.path.join(UPLOAD_FOLDER, f1_obligation_file.name)
                        with open(obligation_path, "wb") as f: f.write(f1_obligation_file.getbuffer())
                    
                    overnight_path = None
                    if f1_overnight_file:
                        overnight_path = os.path.join(UPLOAD_FOLDER, f1_overnight_file.name)
                        with open(overnight_path, "wb") as f: f.write(f1_overnight_file.getbuffer())
                    
                    macro_path = None
                    if f1_macro_file:
                        macro_path = os.path.join(UPLOAD_FOLDER, f1_macro_file.name)
                        with open(macro_path, "wb") as f: f.write(f1_macro_file.getbuffer())

                    # --- Run F1 Logic Directly ---
                    validator = BSRValidator(
                        bsr_path=bsr_file_path, 
                        obligation_path=obligation_path, 
                        overnight_path=overnight_path, 
                        macro_path=macro_path
                    ) 
                    
                    status_summaries = validator.market_check_processor(checks)
                    df_processed = validator.df
                    
                    # --- Generate Output File ---
                    output_filename = f"Processed_BSR_{os.path.splitext(f1_bsr_file.name)[0]}_{int(time.time())}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                    
                    df_processed.to_excel(output_path, index=False)
                    
                    st.success(f"✅ F1 checks completed successfully!")
                    
                    # --- Display Summaries ---
                    st.subheader("Processing Summary")
                    if status_summaries:
                        df_summary = pd.DataFrame(status_summaries)
                        st.dataframe(df_summary, use_container_width=True) # Simplified display
                    else:
                        st.info("No specific operational summaries were returned.")

                    # --- Provide Download Button ---
                    st.markdown("---")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Processed F1 File",
                            data=f,
                            file_name=output_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
                # --- THIS IS THE CORRECTED 'except' BLOCK ---
                except Exception as e:
                    st.error(f"❌ An error occurred during F1 checks: {e}")