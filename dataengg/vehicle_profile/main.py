import os
import logging
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import mysql.connector
import pandas as pd
import psycopg2
from clickhouse_connect import get_client

from includes.etl_pipelines.config import (
    get_clickhouse_config,
    get_mysql_config,
    get_postgres_config,
)


from includes.etl_pipelines.vehicle_profile.config import (
    CH_CHECKPOINT_TABLE,
    CH_PROFILE_TABLE,
    PIPELINE_NAME,
    TABLE_TS,
)


logging.basicConfig(level=os.getenv("ETL_LOG_LEVEL", "INFO"))
log = logging.getLogger(__name__)


# ---------------------------------------------------
# DB CONNECTORS
# ---------------------------------------------------
def mysql_conn():
    mysql_config = get_mysql_config()

    return mysql.connector.connect(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        autocommit=True,
    )


def click_conn():
    clickhouse_config = get_clickhouse_config()

    return get_client(
        host=clickhouse_config["host"],
        port=clickhouse_config["port"],
        username=clickhouse_config["username"],
        password=clickhouse_config["password"],
        database=clickhouse_config["database"],
    )
    
def postgres_conn():
    postgres_config = get_postgres_config()

    return psycopg2.connect(
        host=postgres_config["host"],
        port=postgres_config["port"],
        user=postgres_config["user"],
        password=postgres_config["password"],
        database=postgres_config["database"],
    )


# ---------------------------------------------------
# SOURCE QUERIES
# ---------------------------------------------------
query ={
      "entities": """ SELECT entityId,vehicleNumber,engineNumber,chassisNumber,speedSensorType,speedLimit,vehicleType,vehicleModel,vehicleModelId,
                             vehicleIdentificationNumber,vehicleColour,averageMileage,vehicleMake,vehicleMakeId,vehicleCapacity,idlingSpeed,	
                             CONVERT_TZ(FROM_UNIXTIME(createdAt), '+00:00', '+00:00') AS createdAt,
                             CONVERT_TZ(FROM_UNIXTIME(updatedAt), '+00:00', '+00:00') AS updatedAt,
                             CONVERT_TZ(FROM_UNIXTIME(activatedAt), '+00:00', '+00:00') AS activatedAt,
                             CONVERT_TZ(FROM_UNIXTIME(deactivatedAt), '+00:00', '+00:00') AS deactivatedAt,
                             dateOfActivation,status,clientLoginId,deviceUniqueId_fk,simId,uniqueAccessoryId,certificatePath,approveStatus,ref_x,
                             ref_y,ref_z,generatorCapacity,averageConsumption,tankCapacity,invoice_number,equipment_serial_number,commissioning_date,
                             department,vts_serial_number,equipmentName,contractorName,operatorName,operatorContactNumber,simId2,vehicle_registration_number,
                             vehicle_nick_name,sale_date,variant_id,codp_model,codp_sim_provider,minAvgRpm,maxAvgRpm,engineTorque,engineLoad,fuelType,torquePower,
                             engineSpecification,emissionRateType,fuelSensorModel,isFuelSensor,grossVehicleWeight,manufacturingYear,vehicleSegment,equipmentTypeClass,
                             vehicleWheeler,application,documentUpload,baselineDataForFE,fuelTankCapacity,adblueCapacity,baselineDistance,immobilize,solutionType,
                             isDualEngine,	zonalManager,	keyAccountManager,	channelPartnerName,	baseLineDataForFELPH,	baselineCompleted,	tempActDeactVehTracking,
                             isDuplicated,	portedVehicle,	isTestingVehicle,	hasArm,	engineOnHours,	odometerDistance,	videoTelematicsId,	videoTelematicsType,
                             CONVERT_TZ(FROM_UNIXTIME(invoiceDate), '+00:00', '+00:00') AS invoiceDate,
                             kvaRating,	bhp,	phase,	engineModel,	gensetController,	actualActivationDate,	dgSetType,	constructionVehicleType,
                             vehicleRegistrationNumber,	registrationDate,	registrationValidUpto,	stateCode,	rtoCode,	vehicleCategoryCode,
                             vehicleCategoryDescription,	vehicleClassDescription,	makerDescription,	makerModel,	bodyTypeDescription,	emissionNormsDescription,
                             fitmentUpto,	nationalPermitUpto,	taxUpto,	insurerName,	insuranceUpto,	manufacturingMonthYear,	unladenWeight,	registeredAt,puccUpto,
                             permitNumber,	permitIssueDate,	permitValidFrom,	permitValidUpto,	permitType,	permitCode,	numberOfAxles,	fuelCode,	vehicleClass,
                             noOfGears,	gearRatio,	baselineDataType,	baselineDataHour,	haulageType,	clientName,	custCode,	custType,	noOfWheels,	baselineHours,
                             isIndividualViewOrCombinedView,	vehicleCustomizedBodyType,	emissionRating,	renewalDate,	renewalPeriod,	lastExpiryDate,	vehicleNickName,
                             passengervehicletype,	transportationVehicleType,	offset_cummulative_distance,	offset_cummulative_engine_on_hours,	
                             offset_cummulative_primary_engine_on_hours,	offset_cummulative_secondary_engine_on_hours,	multiplicationFactor,	inputParameter,	eon_voltage,
                             eoff_voltage,	tensionerConductorLength,	isRotationSensor,	verifiedDocs,	sitedetails,	vehicleIdSite,	isServiceInstallationVeh,	isShiftDsr,
                             videoTelematicsId, videoTelematicsType
                        FROM `{table}`
                        """,
    "client_details": """ SELECT id, clientName, email, contactPerson, contactNumber, aadharNumber, address, city,
                           pincode, panNumber, gst, clientType, timezone, lat, `long`, weights,
                           CONVERT_TZ(FROM_UNIXTIME(createdAt), '+00:00', '+00:00') AS createdAt,
                           CONVERT_TZ(FROM_UNIXTIME(updatedAt), '+00:00', '+00:00') AS updatedAt,
                           status, countryId, stateId, partnerLoginId, resellerLoginId, loginId as client_loginid,
                           planId, licenseTypeId, type, expiryDate, isERP, solution, billingMode,
                           salesPersonId, packetThreshold, wallet, simWallet, registrationType,
                           secondaryContactName, secondaryContactNumber, secondaryEmail,
                           additionalContactNumbers, shippingAddress, shippingCity, shippingPincode,
                           minimumTrackedDays, prorata, addressEffectiveDate, shippingCountry,
                           shippingState, clientSOCThreshold, offlineSeconds, fuel_unit, zonalManager,
                           keyAccountManager, channelPartnerName, clientAccess, clientDataProcessing,
                           litemode, is_sim_tracking, videoTelematicsType, totalPotentialVehicles,
                           landlineNumber, customerAcquisitionType, directCustomer, directCustomerType,
                           directCustomerSAPCode, indirectCustomerUniqueId, secondaryLandlineNumber,
                           zone, serviceManager, customerSuccessManager, serviceType,
                           channelPartnerSAPCode, indirectCustomerType, indirectCustomerName,
                           customerLocation, customerNameToBeDisplayed, isSecondaryContact,
                           channelPartnerType, passwordLoggedOut
                        FROM `{table}`
                        WHERE `partnerLoginId` = 8148 
                        """,
      "sim_details": """ SELECT id,	phoneNumber,	simNumber,	monthlyCharges,	
                                CONVERT_TZ(FROM_UNIXTIME(createdAt), '+00:00', '+00:00') AS createdAt,
                                CONVERT_TZ(FROM_UNIXTIME(updatedAt), '+00:00', '+00:00') AS updatedAt,	status,	serviceProviderId,	ownerLoginId
                         FROM `{table}`
                         """,
      
      "service_providers": """ SELECT id,	name as service_provider_name,	
                                      CONVERT_TZ(FROM_UNIXTIME(createdAt), '+00:00', '+00:00') AS createdAt,
                                      CONVERT_TZ(FROM_UNIXTIME(updatedAt), '+00:00', '+00:00') AS updatedAt,	status
                               FROM `{table}` 
                               """,

      "device_details": """ SELECT serial_num,	imei_num,	uniqueDeviceId,
                                   CONVERT_TZ(FROM_UNIXTIME(createdAt), '+00:00', '+00:00') AS createdAt,
                                   CONVERT_TZ(FROM_UNIXTIME(updatedAt), '+00:00', '+00:00') AS updatedAt,
                                   status,	deviceModelId,	manufacturerId,	ownerLoginId,	is_panic_enabled
                            FROM `{table}`
                            """,
      "device_models": """ SELECT id,	model_name,	version,devicetype,	description,	maxPrice,	manufacturerId,
                                  CONVERT_TZ(FROM_UNIXTIME(createdAt), '+00:00', '+00:00') AS createdAt,
                                  CONVERT_TZ(FROM_UNIXTIME(updatedAt), '+00:00', '+00:00') AS updatedAt,
                                  status
                            FROM `{table}`
                            """,
      "fuel_calibration_detail": """ SELECT id,	vehicleId,	clientLoginId,	polynomialEquation,	fuelSensorType,	minTankCapacity,`minValue`,
                                            maxTankCapacity,`maxValue`,
                                            CONVERT_TZ(FROM_UNIXTIME(createdAt), '+00:00', '+00:00') AS createdAt,
                                            CONVERT_TZ(FROM_UNIXTIME(updatedAt), '+00:00', '+00:00') AS updatedAt,
                                            status,	fuelUserType,	refuelThreshold,	drainThreshold,	medianFilter,	pfPerMin,pilferageTolerance,
                                            minimumPilferageDuration,calibration_filename,totalFuelTank,hasSecondaryTank,secondaryTankMaxValue,
                                            secondaryPolynomialEquation,secondary_tank_calibration_filename,secondaryTankFuelSensorType,secondaryTankMinCapacity,
                                            secondaryTankMaxCapacity,secondaryTankTotalFuel,secondaryTankMinValue,useTaabiAlgo,
                                            fuelSensorAlgo,	obdAlgo,preferedDataType,availableDataTypes,hasInSameTank,fuelSensorNames,
                                            isDualSensor,fuelSensorIMEIs,deviceUniqueId,tanks
                                    FROM `{table}` 
                                    """
    
}


def fetch_data(mysql, table):
    cur = mysql.cursor(dictionary=True)
    q = query[table].format(table=table)
    #print(q)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

# %%
def clean_maker_description(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return None
    name = name.lower().strip()
    name = re.sub(r'[^a-z0-9\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    noise_words = {
        "motors","motor","ltd","limited","pvt","private","india",
        "automotive","vehicles","vehicle","group","co","company",
        "industries","corp","corporation","inc"
    }
    tokens = [t for t in name.split() if t not in noise_words]
    if not tokens:
        return None
    combined = " ".join(tokens)
    # Merge known variants
    if any(k in combined for k in ["maruti","suzuki"]): return "maruti suzuki"
    if any(k in combined for k in ["tata","tat"]): return "tata motors"
    if any(k in combined for k in ["mahindra","m&m"]): return "mahindra"
    if "ashok" in combined and "leyland" in combined: return "ashok leyland"
    if "bharat" in combined and "benz" in combined: return "bharat benz"
    if "hyundai" in combined or "hundai" in combined: return "hyundai"
    if "toyota" in combined or "kirloskar" in combined: return "toyota"
    if "volvo" in combined: return "volvo"
    if "honda" in combined: return "honda"
    if "isuzu" in combined: return "isuzu"
    if "force" in combined: return "force motors"
    if "eicher" in combined: return "eicher"
    return tokens[0]

# %%
# ----------------- main run -----------------
def run_once():
    mysql = mysql_conn()
    ch = click_conn()
    pg = postgres_conn()
    try:
        # 1) fetch per-table data
        fetched = {}
        for t in TABLE_TS:
            rows = fetch_data(mysql, t)
            log.info("Fetched %d rows from %s", len(rows), t)
            fetched[t] = rows

        
        # 2) build data frames
        df_entity= pd.DataFrame(fetched["entities"]) if fetched["entities"] else pd.DataFrame()
        df_client = pd.DataFrame(fetched["client_details"]) if fetched["client_details"] else pd.DataFrame()
        
        df_users = pd.read_sql("""SELECT id, username FROM users""", pg)

        df_algo = pd.read_sql("""SELECT uniqueid, (algo_params::json ->> 'overallThreshold')::float AS "overallThreshold" FROM ( SELECT *, ROW_NUMBER() 
        OVER ( PARTITION BY uniqueid ORDER BY updated_at DESC NULLS LAST, created_at ASC NULLS LAST) AS rn FROM public.vehicle_algo_details ) t WHERE rn = 1 """, pg)
        
        df_sim= pd.DataFrame(fetched["sim_details"]) if fetched["sim_details"] else pd.DataFrame()
        df_service_provider = pd.DataFrame(fetched["service_providers"]) if fetched["service_providers"] else pd.DataFrame()
        df_device_details= pd.DataFrame(fetched["device_details"]) if fetched["device_details"] else pd.DataFrame()
        df_device_models = pd.DataFrame(fetched["device_models"]) if fetched["device_models"] else pd.DataFrame()
        df_fuel = pd.DataFrame(fetched["fuel_calibration_detail"]) if fetched["fuel_calibration_detail"] else pd.DataFrame()

        # 3) Renaming the column
        df_client = df_client.rename(columns={'id': 'clientId'})
        df_sim = df_sim.rename(columns={'id': 'simId'})
        df_service_provider = df_service_provider.rename(columns={'id': 'serviceProviderId'})
        df_device_models = df_device_models.rename(columns={'id': 'deviceModelId'})
        df_fuel = df_fuel.rename(columns={'id': 'fuelCalibrationId'})

        df=df_entity.copy()

        # 4) Merging the tables
        df=df.merge(df_client,left_on="clientLoginId",right_on="client_loginid",how="inner",suffixes=("","_client"))
        # ---------------- Zonal Manager ----------------
        df = df.merge(df_users.rename(columns={"id": "zonalManager_client", "username": "zonalManager_username"}),on="zonalManager_client", how="left")
        
        df["zonalManager_client"] = df["zonalManager_username"]
        df.drop(columns=["zonalManager_username"], inplace=True)
        
        # ---------------- KAM ----------------
        df = df.merge(df_users.rename(columns={"id": "keyAccountManager_client","username": "keyAccountManager_username"}), on="keyAccountManager_client", how="left")
        
        df["keyAccountManager_client"] = df["keyAccountManager_username"]
        df.drop(columns=["keyAccountManager_username"], inplace=True)
        
        df=df.merge(df_sim,left_on="simId",right_on="simId",how="left",suffixes=("","_sim"))
        df=df.merge(df_service_provider,left_on="serviceProviderId",right_on="serviceProviderId",how="left",suffixes=("","_sp"))
        df=df.merge(df_fuel,left_on="entityId",right_on="vehicleId",how="left",suffixes=("","_fuel"))

        device_data=df_device_details.merge(df_device_models,left_on="deviceModelId",right_on="deviceModelId",how="left",suffixes=("","_model"))
        df = df.merge(device_data,left_on="deviceUniqueId_fk",right_on="uniqueDeviceId",how="left",suffixes=("","_device"))
        df = df.merge( df_algo, left_on="deviceUniqueId_fk", right_on="uniqueid", how="left")
        
        # Default overallThreshold to 12 if not found
        df["overallThreshold"] = df["overallThreshold"].fillna(12)

        df.drop(columns=["uniqueid"], inplace=True)
    
        df["calibration_file_encoded"] = df["calibration_filename"].apply(
                                                                            lambda x: 1 if str(x).endswith(".xlsx") else 0
                                                                          )

        # 5) Enriching the maker description column
        df["makerDescription"] = df["makerDescription"].apply(clean_maker_description)
        
        # 6) clean all types of null by converting it into clickhouse accepted null i.e. converting to None
        NULL_STRINGS = {
        'null', 'NULL', 'Null',
        'nan', 'NaN', 'NAN',
        'none', 'None', 'NONE',
        '', ' ', '\t'
        }

        cleaned = df.copy()

        for col in cleaned.columns:
            # First convert NaN, NaT, <NA> → None
            cleaned[col] = cleaned[col].where(pd.notnull(cleaned[col]), None)
        
            # If column is object/string-like, clean string-null values
            if cleaned[col].dtype == "object":
                cleaned[col] = cleaned[col].apply( lambda x: None if isinstance(x, str) and x.strip() in NULL_STRINGS else x )
        

        # 7) Rename columns as per clickhouse table
        cleaned = cleaned.rename(columns={'createdAt': 'createdAt_entity'})
        cleaned = cleaned.rename(columns={'updatedAt': 'updatedAt_entity'})

        final_cols=["entityId",
            "vehicleNumber",
            "engineNumber",
            "speedSensorType",
            "vehicleType",
            "vehicleModel",
            "vehicleMake",
            "createdAt_entity",
            "updatedAt_entity",
            "activatedAt",
            "deactivatedAt",
            "dateOfActivation",
            "invoiceDate",
            "status",
            "clientLoginId",
            "deviceUniqueId_fk",
            "fuelType",
            "engineSpecification",
            "grossVehicleWeight",
            "manufacturingYear",
            "vehicleSegment",
            "equipmentTypeClass",
            "vehicleWheeler",
            "application",
            "adblueCapacity",
            "solutionType",
            "isDualEngine",
            "portedVehicle",
            "dgSetType",
            "constructionVehicleType",
            "vehicleRegistrationNumber",
            "registrationDate",
            "vehicleCategoryCode",
            "vehicleCategoryDescription",
            "vehicleClassDescription",
            "makerDescription",
            "makerModel",
            "bodyTypeDescription",
            "emissionNormsDescription",
            "numberOfAxles",
            "haulageType",
            "noOfWheels",
            "passengervehicletype",
            "transportationVehicleType",
            "eon_voltage",
            "eoff_voltage",
            "clientName_client",
            "pincode",
            "createdAt_client",
            "updatedAt_client",
            "partnerLoginId",
            "zone",
            "serviceType",
            "channelPartnerType",
            "service_provider_name",
            "totalPotentialVehicles",
            "zonalManager_client",
            "keyAccountManager_client",
            "fuelSensorType",
            "minTankCapacity",
            "minValue",
            "maxTankCapacity",
            "maxValue",
            "calibration_filename",
            "calibration_file_encoded",
            "hasSecondaryTank",
            "secondaryTankMaxValue",
            "secondary_tank_calibration_filename",
            "secondaryTankFuelSensorType",
            "secondaryTankMinCapacity",
            "secondaryTankMaxCapacity",
            "useTaabiAlgo",
            "overallThreshold",
            "fuelSensorAlgo",
            "obdAlgo",
            "preferedDataType",
            "availableDataTypes",
            "hasInSameTank",
            "fuelSensorNames",
            "isDualSensor",
            "model_name",
            "devicetype",
            "videoTelematicsId",
            "videoTelematicsType",
            "is_bowser_vehicle"]

        # =====================================================
        # BUSINESS ENRICHMENTS
        # =====================================================
        
        # 1. Bowser Vehicle Flag
        bowser_ids = {
            "it_352592575625025",
            "it_352592570947226",
            "it_352016709727671",
            "it_352592574949558",
            "it_353691843504407",
            "it_353742371881632",
            "it_353691845506871",
            "it_353201359371140",
            "it_352592578076770",
            "it_353201354560127"
        }
        
        cleaned["is_bowser_vehicle"] = cleaned["deviceUniqueId_fk"].isin(bowser_ids)
        
        # 2. Status Mapping
        status_map = {
            1: "Activated",
            0: "Deactivated",
            4: "UAT"
        }
        
        cleaned["status"] = cleaned["status"].map(status_map)
        
        # =====================================================
        # END OF BUSINESS ENRICHMENTS
        # =====================================================

        # 8) filtering columns
        cleaned = cleaned[final_cols]

        # 8.1) Convert source UTC date-time columns to IST
        
        IST = "Asia/Kolkata"
        
        datetime_cols = [
            "createdAt_entity",
            "updatedAt_entity",
            "activatedAt",
            "deactivatedAt",
            "invoiceDate",
            "createdAt_client",
            "updatedAt_client",
        ]
        
        for col in datetime_cols:
            cleaned[col] = (
                pd.to_datetime(cleaned[col], errors="coerce", utc=True)
                .dt.tz_convert(IST)
            )
            

        # 9) compute created_at, updated_at
        now = datetime.now(ZoneInfo("Asia/Kolkata"))        
        cleaned["created_at"] = now
        cleaned["updated_at"] = now

        
        # 10) convert col in int64
        column=['dateOfActivation','clientLoginId','manufacturingYear','isDualEngine','portedVehicle','noOfWheels','useTaabiAlgo','hasInSameTank']
        for col in column:
            cleaned[col] = cleaned[col].astype("Int64")


        # 11) Truncate the clickhouse table
        log.info("Truncating ClickHouse table %s", CH_PROFILE_TABLE)
        q = f"Truncate table {CH_PROFILE_TABLE} " 
        ch.command(q)

        # 12) insert into ClickHouse (single-shot)
        log.info("Upserting %d rows into ClickHouse table %s", len(cleaned), CH_PROFILE_TABLE)
        ch.insert_df(CH_PROFILE_TABLE, cleaned)

        # 13) update entities table checkpoints to max(updatedAt) of fetched rows table
        rows = fetched[TABLE_TS[0]]
    
        if rows:
            # compute max ts
            max_ts = max([r["updatedAt"] for r in rows])
            if isinstance(max_ts, str):
                max_ts = datetime.fromisoformat(max_ts)
            if max_ts.tzinfo is None:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            
            now2 = datetime.now(timezone.utc)
            data={"pipeline_name":PIPELINE_NAME,
                  "table_name" : TABLE_TS[0],
                  "last_sync_ts" : max_ts,
                  "updatedAt" : now2
                 }
            check=pd.DataFrame([data])
            ch.insert_df(CH_CHECKPOINT_TABLE, check)
            log.info("Updated checkpoint for %s to %s", TABLE_TS[0], max_ts.isoformat())
                
    finally:
                 
        try: mysql.close()
        except: pass
        try: ch.close()
        except: pass
        try: pg.close()
        except:pass

if __name__ == "__main__":
    run_once()