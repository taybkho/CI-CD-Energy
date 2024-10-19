from solarnetwork_python.client import Client
import json
import sys
import argparse
from datetime import datetime, timedelta
import mysql.connector 

def validate_date(date_str: str):
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Expected format: YYYY-MM-DDTHH:MM:SS")

def extract_data(node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret):
    client = Client(token, secret)

    if aggregate == "None":
        param_str = f"endDate={enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={sourceids}&startDate={startdate}"
    else:
        param_str = f"aggregation={aggregate}&endDate={enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={sourceids}&startDate={startdate}"

    response = client.extract(param_str)

    # Transformation Phase: Strip hidden chars and null values, split by commas, check for jumps in irradianceHours
    print('Created,localDate,localTime,nodeId,sourceId,irradiance,irradianceHours,new_meter,last_meter,new_meter_date,last_meter_date,reset_date')


def transform_data(response): 

    last_irradiance_hours = None  # To track the last valid irradiance hours value
    last_irradiance_date = None   # To track the last valid date

    for element in response.get('results', []):
        # Strip hidden characters and null values
        created = element.get('created', '').strip()
        local_date = element.get('localDate', '').strip()
        local_time = element.get('localTime', '').strip()
        node_id = element.get('nodeId', '').strip()
        source_id = element.get('sourceId', '').strip()
        irradiance = element.get('irradiance', '').strip()
        irradiance_hours = element.get('irradianceHours', '').strip()

        # Handle null or empty values
        if not irradiance_hours:
            continue

        # Parse the irradianceHours as a float for numerical comparison
        try:
            current_irradiance_hours = float(irradiance_hours)
        except ValueError:
            print(f"Invalid irradianceHours value: {irradiance_hours}")
            continue

        # Check for sudden jumps in irradianceHours
        if last_irradiance_hours is not None:
            # Compare the current irradianceHours with the previous one
            if current_irradiance_hours != last_irradiance_hours:
                # If there's a jump, capture the new and last values
                new_meter = current_irradiance_hours
                last_meter = last_irradiance_hours
                new_meter_date = local_date
                last_meter_date = last_irradiance_date

                # Calculate reset_date: 5 minutes after last_meter_date
                last_meter_datetime = datetime.strptime(last_meter_date, '%Y-%m-%d')  # Assuming localDate is in 'YYYY-MM-DD' format
                reset_datetime = last_meter_datetime + timedelta(minutes=5)
                reset_date = reset_datetime.strftime("%b %d, %Y %H:%M:%S")

                # Print the result with the jump and reset date
                print(f"{created},{local_date},{local_time},{node_id},{source_id},{irradiance},{irradiance_hours},{new_meter},{last_meter},{new_meter_date},{last_meter_date},{reset_date}")

        # Update the last known irradiance hours and date
        last_irradiance_hours = current_irradiance_hours
        last_irradiance_date = local_date

# def load(): 
#     # ecosuite api needed 

def main():
    parser = argparse.ArgumentParser(description="Solar query!")

    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Comma-separated list of source IDs")
    parser.add_argument("--startdate", required=True, type=validate_date, help="Start date in format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--enddate", required=True, type=validate_date, help="End date in format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--aggregate", required=True, help="Aggregation method")
    parser.add_argument("--maxoutput", required=True, help="Maximum output limit")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")

    args = parser.parse_args()

    extract_data(args.node, args.sourceids, args.startdate.isoformat(), args.enddate.isoformat(),
                args.aggregate, args.maxoutput, args.token, args.secret)

if __name__ == "__main__":
    main()
