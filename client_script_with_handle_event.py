import asyncio
import aiohttp
import json
from datetime import timedelta, datetime
from openleadr import OpenADRClient, enable_default_logging
from flask import Flask, jsonify
from flask_cors import CORS
import threading
import logging
from typing import List, Dict, Optional, Tuple
import uuid

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

async def store_event_in_db(event: Dict) -> Optional[str]:
    """
    Store event details in the database.
    
    Args:
        event: The OpenADR event data
        
    Returns:
        Event ID if successful, None if failed
    """
    try:
        # Extract event details
        event_id = event['event_descriptor']['event_id']
        event_status = event['event_descriptor']['event_status']
        created_date_time = event['event_descriptor']['created_date_time']
        start_time = event['active_period']['dtstart']
        duration = event['active_period']['duration']
        
        # Extract signal information
        signal_name = event['event_signals'][0]['signal_name'] if event['event_signals'] else None
        signal_payload = None
        if event['event_signals'] and event['event_signals'][0]['intervals']:
            signal_payload = event['event_signals'][0]['intervals'][0]['signal_payload']
        
        # Extract target information
        target_ven_id = event['targets'][0]['ven_id'] if event['targets'] else None
        response_required = event.get('response_required', False)
        
        # Convert duration to seconds if it's a timedelta
        if isinstance(duration, timedelta):
            duration_seconds = int(duration.total_seconds())
        else:
            duration_seconds = duration
        
        # Insert query
        insert_query = """
        INSERT INTO events (
            event_id, event_status, created_date_time, start_time, 
            duration_seconds, signal_name, signal_payload, target_ven_id, 
            response_required, raw_event_data, processed_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        ON CONFLICT (event_id) DO UPDATE SET
            event_status = EXCLUDED.event_status,
            processed_at = EXCLUDED.processed_at,
            raw_event_data = EXCLUDED.raw_event_data
        """
        
        async with pool.acquire() as conn:
            await conn.execute(
                insert_query,
                event_id,
                event_status,
                created_date_time,
                start_time,
                duration_seconds,
                signal_name,
                json.dumps(signal_payload) if signal_payload else None,
                target_ven_id,
                response_required,
                json.dumps(event),
                datetime.utcnow()
            )
            
        logs.append(f"Successfully stored event {event_id} in database")
        return event_id
        
    except Exception as e:
        error_msg = f"Error storing event in database: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        return None

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
    try:
        # Replace this with your actual authentication logic
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

def format_event_for_api(event: Dict) -> Dict:
    """
    Format the OpenADR event data for the API request.
    
    Args:
        event: The OpenADR event data
        
    Returns:
        Formatted event data for API
    """
    try:
        # Extract basic event information
        event_id = event['event_descriptor']['event_id']
        event_status = event['event_descriptor']['event_status']
        start_time = event['active_period']['dtstart']
        duration = event['active_period']['duration']
        response_required = event.get('response_required', False)
        
        # Convert duration to seconds if it's a timedelta
        if isinstance(duration, timedelta):
            duration_seconds = int(duration.total_seconds())
        else:
            duration_seconds = duration
        
        # Format start time as ISO string if it's a datetime object
        if isinstance(start_time, datetime):
            start_time_str = start_time.isoformat() + 'Z'
        else:
            start_time_str = str(start_time)
        
        # Extract signal information and convert to actions
        actions = []
        if event.get('event_signals') and event['event_signals']:
            for signal in event['event_signals']:
                if signal.get('intervals'):
                    for interval in signal['intervals']:
                        payload = interval.get('signal_payload', 0)
                        # Convert signal payload to action format
                        action = {
                            'type': 'incr_setpoint',  # Default action type
                            'value': str(payload),
                            'unit': 'C'  # Default unit
                        }
                        actions.append(action)
        
        # If no actions extracted, add a default action
        if not actions:
            actions = [{
                'type': 'incr_setpoint',
                'value': '3',
                'unit': 'C'
            }]
        
        # Create the formatted event
        formatted_event = {
            'eventId': event_id,
            'status': event_status,
            'startTime': start_time_str,
            'duration': duration_seconds,
            'actions': actions,
            'responseRequired': response_required
        }
        
        return formatted_event
        
    except Exception as e:
        error_msg = f"Error formatting event for API: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        # Return a basic formatted event as fallback
        return {
            'eventId': event.get('event_descriptor', {}).get('event_id', 'unknown'),
            'status': 'active',
            'startTime': datetime.utcnow().isoformat() + 'Z',
            'duration': 7200,
            'actions': [{'type': 'incr_setpoint', 'value': '3', 'unit': 'C'}],
            'responseRequired': True
        }

async def send_event_to_app(installed_app_id: str, auth_token: str, event_data: Dict) -> bool:
    """
    Send event data to a specific installed app.
    
    Args:
        installed_app_id: The installed app identifier
        auth_token: Authentication token
        event_data: Formatted event data
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Replace with your actual API base URL
        api_url = f"https://your-api-base-url.com/installedapps/{installed_app_id}/execute"
        
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
        # Create the request body as per your specification
        request_body = {
            "parameters": {
                "apiType": "OPENADR",
                "methods": "POST",  # Note: you had "methods" in your spec
                "uri": "/openadr/event",
                "body": json.dumps(event_data)
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(api_url, json=request_body, headers=headers) as response:
                if response.status == 200:
                    response_data = await response.json()
                    if response_data.get('statusCode') == 200:
                        logs.append(f"Successfully sent event to app {installed_app_id}")
                        return True
                    else:
                        logs.append(f"API returned non-200 status for app {installed_app_id}: {response_data.get('statusCode')}")
                        return False
                else:
                    logs.append(f"Failed to send event to app {installed_app_id}: HTTP {response.status}")
                    return False
                    
    except Exception as e:
        error_msg = f"Error sending event to app {installed_app_id}: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        return False

async def update_event_distribution_status(event_id: str, total_apps: int, successful_apps: int, failed_apps: int):
    """
    Update the event distribution status in the database.
    
    Args:
        event_id: The event identifier
        total_apps: Total number of apps
        successful_apps: Number of successful distributions
        failed_apps: Number of failed distributions
    """
    try:
        update_query = """
        UPDATE events 
        SET 
            distribution_total = $1,
            distribution_successful = $2,
            distribution_failed = $3,
            distribution_completed_at = $4
        WHERE event_id = $5
        """
        
        async with pool.acquire() as conn:
            await conn.execute(
                update_query,
                total_apps,
                successful_apps,
                failed_apps,
                datetime.utcnow(),
                event_id
            )
            
        logs.append(f"Updated distribution status for event {event_id}: {successful_apps}/{total_apps} successful")
        
    except Exception as e:
        error_msg = f"Error updating event distribution status: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)

async def handle_event(event):
    """
    Enhanced handle_event function that stores event details in DB and distributes to all apps.
    
    Args:
        event: The OpenADR event data
        
    Returns:
        Event response ('optIn', 'optOut', etc.)
    """
    logs.append(f"Received new event: {event['event_descriptor']['event_id']}")
    
    try:
        # Step 1: Store event in database
        event_id = await store_event_in_db(event)
        
        if not event_id:
            logs.append("Failed to store event in database, aborting event handling")
            return 'optOut'
        
        # Step 2: Get all installed apps from database
        installed_apps = await get_installed_apps_from_db()
        
        if not installed_apps:
            logs.append("No installed apps found, event stored but not distributed")
            return 'optIn'
        
        # Step 3: Format event data for API
        formatted_event = format_event_for_api(event)
        
        # Step 4: Distribute event to all installed apps
        successful_distributions = 0
        failed_distributions = 0
        
        # Process apps concurrently for better performance
        async def process_app(installed_app_id: str, location_id: str) -> bool:
            try:
                # Get auth token
                auth_token = await get_auth_token(location_id, installed_app_id)
                
                if not auth_token:
                    logs.append(f"Failed to get auth token for app {installed_app_id}")
                    return False
                
                # Send event to app
                success = await send_event_to_app(installed_app_id, auth_token, formatted_event)
                return success
                
            except Exception as e:
                error_msg = f"Error processing app {installed_app_id}: {str(e)}"
                logs.append(error_msg)
                logger.error(error_msg)
                return False
        
        # Create tasks for all apps
        tasks = [process_app(app_id, loc_id) for app_id, loc_id in installed_apps]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successful and failed distributions
        for result in results:
            if isinstance(result, bool):
                if result:
                    successful_distributions += 1
                else:
                    failed_distributions += 1
            else:
                # Exception occurred
                failed_distributions += 1
        
        # Step 5: Update event distribution status in database
        await update_event_distribution_status(
            event_id, 
            len(installed_apps), 
            successful_distributions, 
            failed_distributions
        )
        
        # Step 6: Log final results
        logs.append(f"Event {event_id} distribution completed:")
        logs.append(f"  Total apps: {len(installed_apps)}")
        logs.append(f"  Successful: {successful_distributions}")
        logs.append(f"  Failed: {failed_distributions}")
        
        # Return appropriate response based on success rate
        if successful_distributions > 0:
            return 'optIn'
        else:
            logs.append("All app distributions failed, opting out")
            return 'optOut'
            
    except Exception as e:
        error_msg = f"Fatal error in handle_event: {str(e)}"
        logs.append(error_msg)
        logger.error(error_msg)
        return 'optOut'

# Rest of your existing code remains the same...

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
