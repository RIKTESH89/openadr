import asyncio
import aiohttp
import json
from datetime import timedelta
from openleadr import OpenADRClient, enable_default_logging
from flask import Flask, jsonify
from flask_cors import CORS
import threading
import logging
from typing import List, Dict, Optional, Tuple

# Import your singleton Pg pool
from your_db_module import pool  # Replace with your actual import

enable_default_logging()

# Logging mechanism
logs = []
logger = logging.getLogger(__name__)

# Flask app for serving logs
# app = Flask(__name__)
# CORS(app)

# @app.route('/logs', methods=['GET'])
# def get_logs():
#     return jsonify(logs[-100:])  # Serve the last 100 logs

async def get_installed_apps_from_db() -> List[Tuple[str, str]]:
    """
    Fetch all installedAppId and location_id pairs from the database.
    
    Returns:
        List of tuples containing (installedAppId, location_id)
    """
    query = """
    SELECT installedAppId, location_id 
    FROM your_table_name 
    WHERE installedAppId IS NOT NULL 
    AND location_id IS NOT NULL
    """
    
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            result = [(row['installedappid'], row['location_id']) for row in rows]
            logs.append(f"Fetched {len(result)} installed apps from database")
            return result
    except Exception as e:
        error_msg = f"Database error while fetching installed apps: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        return []

async def get_auth_token(location_id: str, installed_app_id: str) -> Optional[str]:
    """
    Get authentication token for the given location_id and installedAppId.
    
    Args:
        location_id: The location identifier
        installed_app_id: The installed app identifier
        
    Returns:
        Authentication token or None if failed
    """
    # Replace this with your actual authentication logic
    # This is a placeholder implementation
    try:
        # Example: Call your authentication service/API
        auth_url = f"https://your-auth-service.com/auth"  # Replace with actual URL
        
        async with aiohttp.ClientSession() as session:
            auth_payload = {
                "location_id": location_id,
                "installed_app_id": installed_app_id
            }
            
            async with session.post(auth_url, json=auth_payload) as response:
                if response.status == 200:
                    auth_data = await response.json()
                    token = auth_data.get('access_token')  # Adjust based on your API response
                    logs.append(f"Successfully obtained auth token for app {installed_app_id}")
                    return token
                else:
                    logs.append(f"Failed to get auth token for app {installed_app_id}: {response.status}")
                    return None
                    
    except Exception as e:
        error_msg = f"Error getting auth token for app {installed_app_id}: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        return None

async def fetch_report_data(installed_app_id: str, auth_token: str) -> Optional[Dict]:
    """
    Fetch report data for a specific installed app using the auth token.
    
    Args:
        installed_app_id: The installed app identifier
        auth_token: Authentication token
        
    Returns:
        API response data or None if failed
    """
    try:
        # Replace with your actual API base URL
        api_url = f"https://your-api-base-url.com/installedapps/{installed_app_id}/execute"
        
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
        request_body = {
            "parameters": {
                "apiType": "OPENADR",
                "method": "GET",
                "uri": "/openadr/report",
                "installedappid": installed_app_id
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(api_url, json=request_body, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    logs.append(f"Successfully fetched report data for app {installed_app_id}")
                    return data
                else:
                    logs.append(f"Failed to fetch report data for app {installed_app_id}: {response.status}")
                    return None
                    
    except Exception as e:
        error_msg = f"Error fetching report data for app {installed_app_id}: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        return None

def extract_delta_energy_from_response(response_data: Dict) -> float:
    """
    Extract and sum all deltaEnergy values from the API response.
    
    Args:
        response_data: The API response data
        
    Returns:
        Sum of all deltaEnergy values
    """
    total_delta_energy = 0.0
    
    try:
        if (response_data and 
            response_data.get('statusCode') == 200 and 
            'executeData' in response_data):
            
            execute_data = response_data['executeData']
            if (execute_data.get('statusCode') == 200 and 
                'message' in execute_data and 
                'items' in execute_data['message']):
                
                items = execute_data['message']['items']
                for item in items:
                    if 'deltaEnergy' in item:
                        try:
                            delta_energy = float(item['deltaEnergy'])
                            total_delta_energy += delta_energy
                        except (ValueError, TypeError):
                            logs.append(f"Invalid deltaEnergy value: {item.get('deltaEnergy')}")
                            continue
                
                logs.append(f"Extracted total deltaEnergy: {total_delta_energy} from {len(items)} devices")
                
    except Exception as e:
        error_msg = f"Error extracting deltaEnergy from response: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
    
    return total_delta_energy

async def collect_report_value():
    """
    Main callback function that collects report values from all installed apps.
    
    Returns:
        Aggregated delta energy value across all apps and devices
    """
    logs.append("Starting report collection process...")
    
    try:
        # Step 1: Get all installed apps from database
        installed_apps = await get_installed_apps_from_db()
        
        if not installed_apps:
            logs.append("No installed apps found in database")
            return 0.0
        
        total_aggregated_energy = 0.0
        successful_calls = 0
        
        # Step 2: Process each installed app
        for installed_app_id, location_id in installed_apps:
            try:
                # Step 3: Get auth token
                auth_token = await get_auth_token(location_id, installed_app_id)
                
                if not auth_token:
                    logs.append(f"Skipping app {installed_app_id} due to auth failure")
                    continue
                
                # Step 4: Fetch report data
                report_data = await fetch_report_data(installed_app_id, auth_token)
                
                if not report_data:
                    logs.append(f"Skipping app {installed_app_id} due to API failure")
                    continue
                
                # Step 5: Extract and aggregate delta energy
                app_delta_energy = extract_delta_energy_from_response(report_data)
                total_aggregated_energy += app_delta_energy
                successful_calls += 1
                
                logs.append(f"App {installed_app_id} contributed {app_delta_energy} to total")
                
            except Exception as e:
                error_msg = f"Error processing app {installed_app_id}: {str(e)}"
                logs.append(error_msg)
                logger.error(error_msg)
                continue
        
        # Final logging
        logs.append(f"Report collection completed: {successful_calls}/{len(installed_apps)} apps processed")
        logs.append(f"Total aggregated deltaEnergy: {total_aggregated_energy}")
        
        return total_aggregated_energy
        
    except Exception as e:
        error_msg = f"Fatal error in collect_report_value: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        return 0.0

async def handle_event(event):
    """Handle incoming OpenADR events."""
    logs.append(f"Event ID: {event['event_descriptor']['event_id']}")
    logs.append(f"Event Status: {event['event_descriptor']['event_status']}")
    logs.append(f"Created Date: {event['event_descriptor']['created_date_time']}")
    logs.append(f"Start Time: {event['active_period']['dtstart']}")
    logs.append(f"Duration: {event['active_period']['duration']}")
    logs.append(f"Signal Name: {event['event_signals'][0]['signal_name']}")
    logs.append(f"Signal Payload: {event['event_signals'][0]['intervals'][0]['signal_payload']}")
    logs.append(f"Target VEN ID: {event['targets'][0]['ven_id']}")
    logs.append(f"Response Required: {event['response_required']}")
    return 'optIn'

# Initialize OpenADR client
client = OpenADRClient(ven_name='ven123',
                       vtn_url='http://localhost:8080/OpenADR2/Simple/2.0b')

# Add report with the enhanced callback
client.add_report(callback=collect_report_value,
                  resource_id='device001',
                  report_type='deltaUsage',
                  measurement='voltage',
                  sampling_rate=timedelta(seconds=50))

# Add event handler
client.add_handler('on_event', handle_event)

# Start the client
loop = asyncio.get_event_loop()
loop.create_task(client.run())

# Run Flask in a separate thread if needed
# threading.Thread(target=start_flask, daemon=True).start()

loop.run_forever()
