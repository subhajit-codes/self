import os


# ---------------------------------------------------
# VEHICLE PROFILE PIPELINE CONFIG
# ---------------------------------------------------
PIPELINE_NAME = os.getenv("ETL_PIPELINE_NAME", "vehicle_profile_cdc")

CH_PROFILE_TABLE = "vehicle_profile"
CH_CHECKPOINT_TABLE = "etl_checkpoints_SC"

TABLE_TS = [
    "entities",
    "client_details",
    "sim_details",
    "service_providers",
    "device_details",
    "device_models",
    "fuel_calibration_detail",
]
