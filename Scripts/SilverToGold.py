from pathlib import Path
import re
import pandas as pd

SILVER_PATH_Safercar_data = Path("Silver/Safercar_data.parquet")
SILVER_PATH_light_duty_vehicles = Path("Silver/light-duty-vehicles-2026-02-08.parquet")
SILVER_PATH_vehicles_fuel_consumption = Path("Silver/vehicles.parquet")
GOLD_FOLDER = Path("Gold")
GOLD_SAFETY_SUMMARY = GOLD_FOLDER / "safety_summary.parquet"
GOLD_LIGHT_VEHICLE_DETAIL = GOLD_FOLDER / "light_vehicle_detail.parquet"
GOLD_FUEL_SUMMARY = GOLD_FOLDER / "fuel_summary.parquet"

#if the folder doesn't already exist, it creates it
GOLD_FOLDER.mkdir(parents=True, exist_ok=True)

if not SILVER_PATH_Safercar_data.exists():
    raise FileNotFoundError(f"Silver file not found at {SILVER_PATH_Safercar_data}.")
if not SILVER_PATH_light_duty_vehicles.exists():
    raise FileNotFoundError(f"Silver file not found at {SILVER_PATH_light_duty_vehicles}.")
if not SILVER_PATH_vehicles_fuel_consumption.exists():
    raise FileNotFoundError(f"Silver file not found at {SILVER_PATH_vehicles_fuel_consumption}.")

df_safe = pd.read_parquet(SILVER_PATH_Safercar_data)
df_vehicle = pd.read_parquet(SILVER_PATH_light_duty_vehicles)
df_fuel = pd.read_parquet(SILVER_PATH_vehicles_fuel_consumption)

join_keys = ["MANUFACTURER", "MODEL", "MODEL_YEAR"]

# Delete rows with nulls in join keys
df_safe = df_safe.dropna(subset=join_keys)
df_vehicle = df_vehicle.dropna(subset=join_keys)
df_fuel = df_fuel.dropna(subset=join_keys)

##TABLE 1 - Safety summary by Manufacturer

safety_summary_cols = ["MIN_GROSS_WEIGHT", "MAX_GROSS_WEIGHT", "OVERALL_STARS", "FRNT_DRIV_STARS",
            "FRNT_PASS_STARS", "OVERALL_FRNT_STARS", "CURB_WEIGHT", "HIC15_DRIV", "CHEST_DEFL_DRIV",
            "LEFT_FEMUR_DRIV", "RIGHT_FEMUR_DRIV", "NIJ_DRIV", "NECK_TENS_DRIV", "NET_COMP_DRIV",
            "HIC15_PASS", "CHEST_DEFL_PASS", "LEFT_FEMUR_PASS", "RIGHT_FEMUR_PASS", "NIJ_PASS",
            "NECK_TENS_PASS", "NET_COMP_PASS", "SIDE_DRIV_STARS", "SIDE_PASS_STARS", "SIDE_BARRIER_STAR",
            "COMB_FRNT_STAR", "COMB_REAR_STAR", "OVERALL_SIDE_STARS", "SIDE_HIC_36_DRIV", "RIB_DEFLECTION_DRIV",
            "ABDOMEN_FORCE_DRIV", "SYMPHYSIS_FORCE_DRIV", "SIDE_HIC_36_PASS", "PELVIC_FORCE_PASS", "SIDE_POLE_STARS",
            "POLE_HIC_36_DRIV", "PELVIC_FORCE", "ROLLOVER_POSSIBILITY", "STATIC_STABI_FACTOR", "ROLLOVER_STARS"]

#This table took the mean of all safety variables for each combination of Manufacturer, Model, and Model Year
df_safety_summary = (
    df_safe
    .groupby(join_keys, as_index=False)[safety_summary_cols]
    .mean()
)

#Values rounded to 0 decimals
df_safety_summary[safety_summary_cols] = df_safety_summary[safety_summary_cols].round(0)

# Write Gold (Parquet)
df_safety_summary.to_parquet(GOLD_SAFETY_SUMMARY, index=False)

## TABLE 2 - Light Vehicles Details

#This table is just the light-duty-vehicles silver table, only the rows with null join keys values were removed
#This approach was followed, as this table has the detail per light vehicle and doesn't need more transforamtions at this stage

# Write Gold (Parquet)
df_vehicle.to_parquet(GOLD_LIGHT_VEHICLE_DETAIL, index=False)

##TABLE 3 - Fuel summary by Manufacturer

fuel_summary_cols = ["BARRELS08", "BARRELS_A08", "CHARGE120", "CHARGE240", "CITY08", "CITY08_U", "CITY_A08",
                     "CITY_A08_U", "CITY_CD", "CITY_E", "CITY_UF", "CO2", "CO2_A", "CO2_TAILPIPE_AGPM", "CO2_TAILPIPE_GPM",
                     "COMB08", "COMB08_U", "COMB_A08", "COMB_A08_U", "COMB_E", "COMBINED_CD", "COMBINED_UF", "CYLINDERS",
                     "DISPL", "FE_SCORE", "FUEL_COST08", "FUEL_COST_A08", "GHG_SCORE", "GHG_SCORE_A", "HIGHWAY08", "HIGHWAY08_U",
                     "HIGHWAY_A08", "HIGHWAY_A08_U", "HIGHWAY_CD", "HIGHWAY_E", "HIGHWAY_UF", "HLV", "HPV", "LV2", "LV4", "PV2",
                     "PV4", "RANGE", "RANGE_CITY", "RANGE_CITY_A", "RANGE_HWY", "RANGE_HWY_A", "UCITY", "UCITY_A", "UHIGHWAY",
                     "UHIGHWAY_A", "YOU_SAVE_SPEND", "CHARGE240B", "PHEV_CITY", "PHEV_HWY", "PHEV_COMB"]

#This table took the mean of all fuel variables for each combination of Manufacturer, Model, and Model Year

df_fuel_summary = (
    df_fuel
    .groupby(join_keys, as_index=False)[fuel_summary_cols]
    .mean()
)

#Values rounded to 0 decimals
df_fuel_summary[fuel_summary_cols] = df_fuel_summary[fuel_summary_cols].round(0)

# Write Gold (Parquet)
df_fuel_summary.to_parquet(GOLD_FUEL_SUMMARY, index=False)



