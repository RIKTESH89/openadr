import logging
from openleadr.messaging import create_message, parse_message
from openleadr.utils import get_ven_id

logger = logging.getLogger('openleadr')

class OptService:
    """
    Service for handling EiOpt capabilities from a VEN.
    """
    def __init__(self, vtn_id):
        self.vtn_id = vtn_id
        # In-memory storage for opt schedules, keyed by ven_id
        self.ven_opts = {}

    async def _create_opt(self, payload):
        """
        Handles oadrCreateOpt messages.
        """
        ven_id = get_ven_id(payload)
        request_id = payload['request_id']
        opt_id = payload.get('opt_id') # The VEN can provide its own ID
        opt_type = payload['opt_type']
        opt_reason = payload['opt_reason']
        vavailability = payload.get('vavailability') # This contains the schedules

        # Store the opt schedule for this VEN
        # In a real-world scenario, you would persist this to a database.
        if ven_id not in self.ven_opts:
            self.ven_opts[ven_id] = []
        
        # You would add more robust logic here to handle market contexts, resource_ids, etc.
        opt_schedule = {
            'opt_id': opt_id,
            'opt_type': opt_type,
            'opt_reason': opt_reason,
            'vavailability': vavailability
        }
        self.ven_opts[ven_id].append(opt_schedule)
        logger.info(f"Received and stored oadrCreateOpt from ven_id: {ven_id} with opt_id: {opt_id}")

        # Create a response
        response_payload = {
            'ven_id': ven_id,
            'opt_id': opt_id,
        }
        response = create_message('oadrCreatedOpt',
                                  response_code=200,
                                  response_description="OK",
                                  request_id=request_id,
                                  **response_payload)
        return response

    async def _cancel_opt(self, payload):
        """
        Handles oadrCancelOpt messages.
        """
        ven_id = get_ven_id(payload)
        request_id = payload['request_id']
        opt_id = payload['opt_id']

        # Find and remove the opt schedule
        if ven_id in self.ven_opts:
            schedules = self.ven_opts[ven_id]
            schedules_to_keep = [s for s in schedules if s['opt_id'] != opt_id]
            if len(schedules_to_keep) < len(schedules):
                self.ven_opts[ven_id] = schedules_to_keep
                logger.info(f"Canceled opt_id: {opt_id} for ven_id: {ven_id}")
                response_code = 200
                response_description = "OK"
            else:
                logger.warning(f"Could not find opt_id: {opt_id} to cancel for ven_id: {ven_id}")
                response_code = 404
                response_description = "Not Found"
        else:
            logger.warning(f"Could not find any opt schedules for ven_id: {ven_id} to cancel.")
            response_code = 404
            response_description = "Not Found"

        # Create a response
        response_payload = {
            'ven_id': ven_id,
            'opt_id': opt_id
        }
        response = create_message('oadrCanceledOpt',
                                  response_code=response_code,
                                  response_description=response_description,
                                  request_id=request_id,
                                  **response_payload)
        return response

    async def _invalid_opt(self, payload):
        """
        Handles invalid opt messages.
        """
        ven_id = get_ven_id(payload)
        request_id = payload['request_id']
        response_payload = {
            'ven_id': ven_id,
            'opt_id': "N/A"
        }
        response = create_message('oadrCanceledOpt',
                                  response_code=400,
                                  response_description="Invalid Opt Request",
                                  request_id=request_id,
                                  **response_payload)
        return response
