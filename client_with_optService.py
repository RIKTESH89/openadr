import asyncio
import logging
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from dataclasses import asdict
import ssl

import aiohttp
from lxml import etree

from openleadr.messaging import create_message, parse_message, validate_xml
from openleadr.utils import get_cert_fingerprint, find_by, generate_request_id
from openleadr.enums import CODES, STATUS_CODES, NAMESPACES, CERT_FORMATS
from openleadr.errors import NotRegisteredError, DeploymentError
from openleadr.objects import Event, Opt, Report

logger = logging.getLogger('openleadr')

class OpenADRClient:
    """
    Client for interacting with a VTN (Server).
    """
    def __init__(self, ven_name, vtn_url, cert_path=None, key_path=None,
                 passphrase=None, ven_id=None, ca_path=None,
                 vtn_fingerprint=None, show_fingerprint=True,
                 allow_untested_vtn=False, ):
        self.ven_name = ven_name
        self.vtn_url = vtn_url
        self.ven_id = ven_id
        self.registration_id = None
        self.poll_interval = None
        self.vtn_id = None

        self.reports = []
        self.report_callbacks = {}              # A dict of data source names and their callback functions
        self.report_schedulers = {}             # A dict of registered reports and their associated schedulers

        self.event_callbacks = {
            'on_event': self.on_event
        }
        self.responded_events = {}              # A list of events that we have already responded to
        self.pending_events = {}

        # Two-way SSL authentication
        self.cert_path = cert_path
        self.key_path = key_path
        self.passphrase = passphrase
        self.ca_path = ca_path

        # For TLS fingerprint validation
        self.vtn_fingerprint = vtn_fingerprint
        self.show_fingerprint = show_fingerprint
        self.allow_untested_vtn = allow_untested_vtn

        self.session = None
        self._poll_task = None
        self.running = False

    @property
    def registered(self):
        return self.registration_id is not None

    async def run(self):
        """
        Run the client in polling mode.
        """
        if self.ven_id is None:
            self.ven_id = await self._create_party_registration(self.vtn_fingerprint,
                                                                 self.show_fingerprint,
                                                                 self.allow_untested_vtn)
            if not self.ven_id:
                raise NotRegisteredError

        if self._poll_task:
            self._poll_task.cancel()
        self._poll_task = asyncio.create_task(self._poll())
        self.running = True
        await self._poll_task

    async def stop(self):
        """
        Stop the client.
        """
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self.session and not self.session.closed:
            await self.session.close()
        self.running = False

    async def on_event(self, event):
        """
        Default event handler.
        """
        logger.warning("You should implement your own on_event handler. This handler receives an "
                         "event, and should return the opt-type (optIn, optOut).")
        return 'optIn'

    async def on_report(self, report_request_id, r_id, report, values):
        """
        Default report handler.
        """
        logger.warning("You should implement your own on_report handler. This handler receives a "
                         "report and should return the values for that report.")
        return []

    async def _poll(self):
        """
        The polling loop.
        """
        if self.session is None:
            self.session = self._create_session(self.vtn_fingerprint,
                                                 self.show_fingerprint,
                                                 self.allow_untested_vtn)
        while self.running:
            if self.poll_interval is None:
                if self.registration_id:
                    await self._reregister()
                else:
                    await self._create_party_registration()

            if self.poll_interval:
                message = create_message('oadrPoll', ven_id=self.ven_id)
                response_type, response_payload = await self.send(message)

                if response_type == 'oadrResponse':
                    logger.info("No new messages.")
                else:
                    await self.on_message(response_type, response_payload)

                logger.info(f"Polling again in {self.poll_interval.total_seconds()} seconds.")
                await asyncio.sleep(self.poll_interval.total_seconds())

    async def on_message(self, message_type, message_payload):
        """
        Handle a message from the VTN.
        """
        logger.debug(f"A new message of type {message_type} received: {message_payload}")
        if message_type == 'oadrDistributeEvent':
            for event in message_payload['events']:
                if event['event_descriptor']['event_id'] not in self.responded_events:
                    response_opt = await self.event_callbacks['on_event'](event)
                    event_id = event['event_descriptor']['event_id']
                    modification_number = event['event_descriptor']['modification_number']
                    request_id = event['request_id']
                    self.responded_events[event_id] = response_opt
                    logger.info(f"Responding to event {event_id} with {response_opt}")
                    message = create_message('oadrCreatedEvent',
                                             event_responses=[{'response_code': 200,
                                                               'response_description': 'OK',
                                                               'request_id': request_id,
                                                               'event_id': event_id,
                                                               'modification_number': modification_number,
                                                               'opt_type': response_opt}],
                                             ven_id=self.ven_id)
                    response_type, response_payload = await self.send(message)
                    logger.info(response_payload)

        elif message_type == 'oadrCreateReport':
            logger.info("The VTN asks to create a report")
            for report_request in message_payload['report_requests']:
                callback = self.report_callbacks.get(report_request['report_specifier']['report_name'])
                if callback:
                    await callback(report_request)
                else:
                    logger.warning(f"No callback for a report of type "
                                     f"{report_request['report_specifier']['report_name']} was registered.")

        elif message_type == 'oadrRegisterReport':
            logger.info("The VTN asks to register a report")
            message = create_message('oadrRegisteredReport',
                                     ven_id=self.ven_id,
                                     request_id=message_payload['request_id'])
            await self.send(message)

        elif message_type == 'oadrCancelReport':
            logger.info("The VTN asks to cancel a report")
            await self._cancel_report(message_payload)
            message = create_message('oadrCanceledReport',
                                     request_id=message_payload['request_id'],
                                     ven_id=self.ven_id)
            await self.send(message)

    async def query_registration(self):
        """
        Request the registration status from the VTN.
        """
        message = create_message('oadrQueryRegistration', request_id=generate_request_id())
        response_type, response_payload = await self.send(message)
        self.registration_id = response_payload.get('registration_id')
        self.ven_id = response_payload.get('ven_id')
        self.vtn_id = response_payload.get('vtn_id')
        return response_payload

    async def create_opt(self, availability_schedule, opt_reason="Economic", opt_type="optOut"):
        """
        Communicate your availability to the VTN.
        """
        request_id = generate_request_id()
        opt_id = generate_request_id()
        payload = {
            'request_id': request_id,
            'ven_id': self.ven_id,
            'opt_id': opt_id,
            'opt_type': opt_type,
            'opt_reason': opt_reason,
            'vavailability': {
                'components': [
                    {'properties': {'start_after': schedule['start_after']},
                     'intervals': schedule['intervals']}
                    for schedule in availability_schedule
                ]
            }
        }
        message = create_message('oadrCreateOpt', **payload)
        response_type, response_payload = await self.send(message)
        return response_type, response_payload

    async def cancel_opt(self, opt_id):
        """
        Cancel a previously provided availability schedule.
        """
        payload = {
            'request_id': generate_request_id(),
            'ven_id': self.ven_id,
            'opt_id': opt_id
        }
        message = create_message('oadrCancelOpt', **payload)
        response_type, response_payload = await self.send(message)
        return response_type, response_payload

    async def register_report(self, report):
        """
        Register a report with the VTN.
        """
        if not report.report_request_id:
            report.report_request_id = generate_request_id()

        # Add the report to the list of reports
        self.reports = find_by(self.reports, 'report_id', report.report_id, replacement=report)

        # Create the message
        message = create_message('oadrRegisterReport',
                                 request_id=generate_request_id(),
                                 ven_id=self.ven_id,
                                 reports=[asdict(report)])

        # Send the message
        response_type, response_payload = await self.send(message)
        report.registration_id = self.registration_id
        return response_payload

    async def cancel_report(self, report):
        """
        Cancel a report registration with the VTN.
        """
        report_to_cancel = find_by(self.reports, 'report_id', report.report_id)
        if report_to_cancel:
            self.reports.remove(report_to_cancel)
            if report_to_cancel.report_request_id in self.report_schedulers:
                self.report_schedulers[report_to_cancel.report_request_id].cancel()
                self.report_schedulers.pop(report_to_cancel.report_request_id)
            message = create_message('oadrCancelReport',
                                     ven_id=self.ven_id,
                                     report_request_id=report_to_cancel.report_request_id,
                                     report_to_follow=False)
            response_type, response_payload = await self.send(message)
            return response_payload
        else:
            logger.warning(f"Could not cancel report with report_id {report.report_id}, as it is not known to this client.")
            return None

    def add_handler(self, event, function):
        """
        Add a handler for a specific event.
        """
        if event not in ('on_event', 'on_report'):
            raise NameError(f"Unknown event {event}. Valid events are 'on_event', 'on_report'.")
        self.event_callbacks[event] = function

    def add_report(self, callback, resource_id, measurement,
                   sampling_rate=timedelta(seconds=10),
                   report_duration=timedelta(hours=1),
                   data_collection_mode='incremental'):
        """
        Add a new report that can be collected from this VEN.
        """
        report_id = generate_request_id()
        report_specifier_id = f"report_specifier_{report_id}"
        r_id = f"r_id_{report_id}"

        report = Report(report_id=report_id,
                        report_specifier_id=report_specifier_id,
                        report_name='METADATA_TELEMETRY_USAGE',
                        resource_id=resource_id,
                        measurement=measurement,
                        unit='W',
                        sampling_rate=sampling_rate,
                        report_duration=report_duration,
                        data_collection_mode=data_collection_mode)
        self.reports.append(report)
        return self.register_report(report)

    def add_event(self, callback, event_id, response_required='always'):
        """
        Add a new event with a callback to this VEN.
        """
        event = Event(event_id=event_id,
                      response_required=response_required,
                      callback=callback)
        self.events.append(event)

    def add_opt(self, callback, opt_id):
        """
        Add a new opt with a callback to this VEN.
        """
        opt = Opt(opt_id=opt_id,
                  callback=callback)
        self.opts.append(opt)

    async def send(self, message):
        """
        Send a message to the VTN.
        """
        url = self.vtn_url
        if not url.endswith('/'):
            url += '/'
        service = message['payload']['service']
        url += service

        if self.session is None or self.session.closed:
            self.session = self._create_session(self.vtn_fingerprint,
                                                 self.show_fingerprint,
                                                 self.allow_untested_vtn)
        try:
            async with self.session.post(url, data=message['body']) as resp:
                content = await resp.read()
                logger.debug(f"Request: {message['body']}")
                logger.debug(f"Response: {content.decode('utf-8')}")
                if resp.status != 200:
                    logger.error(f"VTN returned a non-200 status code: {resp.status} with body: {content}")
                    raise aiohttp.ClientResponseError(None, None, status=resp.status, message=content)

                try:
                    validate_xml(content)
                    response_type, response_payload = parse_message(content)
                except Exception as err:
                    logger.error("The VTN returned an invalid message.")
                    logger.error(f"The error was: {err}")
                    logger.error(f"The message was: {content.decode('utf-8')}")
                    raise err
                return response_type, response_payload
        except aiohttp.ClientConnectorError as err:
            logger.error(f"Could not connect to the VTN at {self.vtn_url}. Please check the URL and that the VTN is running.")
            raise err

    async def _create_party_registration(self, vtn_fingerprint=None,
                                         show_fingerprint=True, allow_untested_vtn=False):
        """
        Try to register the VEN with the VTN.
        """
        if self.session is None or self.session.closed:
            self.session = self._create_session(self.vtn_fingerprint,
                                                 self.show_fingerprint,
                                                 self.allow_untested_vtn)
        payload = {'ven_name': self.ven_name,
                   'ven_id': self.ven_id,
                   'http_pull_model': True,
                   'xml_signature': False,
                   'report_only': False,
                   'profile_name': '2.0b'}
        message = create_message('oadrCreatePartyRegistration',
                                 **payload,
                                 request_id=generate_request_id())

        response_type, response_payload = await self.send(message)
        if response_type == 'oadrCreatedPartyRegistration':
            if response_payload['response']['response_code'] == 200:
                self.registration_id = response_payload.get('registration_id')
                self.vtn_id = response_payload.get('vtn_id')
                self.poll_interval = response_payload.get('requested_oadr_poll_freq')
                logger.info("Successfully registered with the VTN. Our registration_id is "
                              f"{self.registration_id} and the poll interval is "
                              f"{self.poll_interval.total_seconds()} seconds.")
                return self.ven_id
        logger.error(f"Failed to register with the VTN. Response: {response_payload}")

    async def _reregister(self):
        """
        Renew the registration with the VTN.
        """
        payload = {'ven_name': self.ven_name,
                   'ven_id': self.ven_id,
                   'registration_id': self.registration_id,
                   'http_pull_model': True,
                   'xml_signature': False,
                   'report_only': False,
                   'profile_name': '2.0b'}
        message = create_message('oadrCreatePartyRegistration',
                                 **payload,
                                 request_id=generate_request_id())
        response_type, response_payload = await self.send(message)
        if response_payload['response']['response_code'] == 200:
            self.poll_interval = response_payload.get('requested_oadr_poll_freq')
            logger.info(f"Successfully re-registered with the VTN. The poll interval is "
                          f"{self.poll_interval.total_seconds()} seconds.")
            return True
        else:
            logger.error("Failed to re-register with the VTN. The VTN will probably drop our"
                           " session. We will attempt to register again from scratch.")
            self.registration_id = None
            self.ven_id = None
            self.poll_interval = None
            return False

    def _create_session(self, vtn_fingerprint=None, show_fingerprint=True, allow_untested_vtn=False):
        """
        Create the aiohttp session.
        """
        if self.cert_path:
            if show_fingerprint:
                with open(self.cert_path) as file:
                    cert_fingerprint = get_cert_fingerprint(file.read())
                logger.info(f"Your VEN's fingerprint is {cert_fingerprint}")
            if vtn_fingerprint is None and not allow_untested_vtn:
                raise DeploymentError("You must provide the VTN's fingerprint in order to use TLS client authentication.")
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=self.ca_path)
            ssl_context.load_cert_chain(self.cert_path, self.key_path, self.passphrase)
            tcp_connector = aiohttp.TCPConnector(ssl=ssl_context,
                                                 verify_ssl=True)
            session = aiohttp.ClientSession(connector=tcp_connector)
        else:
            session = aiohttp.ClientSession()
        return session

    async def _cancel_report(self, message):
        report_request_id = message['report_requests'][0]['report_request_id']
        if report_request_id in self.report_schedulers:
            self.report_schedulers[report_request_id].cancel()
            self.report_schedulers.pop(report_request_id)

    async def _create_report(self, message):
        """
        Respond to a request from the VTN to create a report.
        """
        report_request = message['report_requests'][0]
        report_request_id = report_request['report_request_id']
        report_specifier_id = report_request['report_specifier']['report_specifier_id']
        report_name = report_request['report_specifier']['report_name']
        report_interval = report_request['report_specifier']['granularity']

        callback = self.on_report

        # Create a message queue for this report
        queue = asyncio.Queue()
        self.report_schedulers[report_request_id] = asyncio.create_task(self._report_scheduler(queue, report_interval))

        # Create a report task
        asyncio.create_task(self._report_task(callback, queue, report_request_id, report_specifier_id, report_name))

        # Send a oadrCreatedReport message to the VTN
        payload = {'request_id': message['request_id'],
                   'report_request_id': report_request_id,
                   'report_specifier_id': report_specifier_id}
        response = create_message('oadrCreatedReport',
                                  **payload,
                                  ven_id=self.ven_id)
        return 'oadrCreatedReport', response

    async def _report_scheduler(self, queue, interval):
        """
        Puts a message on the queue at each interval.
        """
        while True:
            await queue.put(True)
            await asyncio.sleep(interval.total_seconds())

    async def _report_task(self, callback, queue, report_request_id, report_specifier_id, report_name):
        """
        Collects and sends out a report to the VTN.
        """
        while True:
            await queue.get()
            report_data = await callback(report_request_id, report_specifier_id, report_name)
            message = create_message('oadrUpdateReport',
                                     ven_id=self.ven_id,
                                     request_id=generate_request_id(),
                                     reports=[report_data])
            await self.send(message)
