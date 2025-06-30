import asyncio
import inspect
import logging
from datetime import timedelta
from functools import partial

from aiohttp import web

from .. import enums
from ..messaging import create_message, parse_message, render_message
from ..utils import (find_by, get_cert_fingerprint, get_ven_id,
                     validate_ven_id)
from .event_service import EventService
from .poll_service import PollService
from .registration_service import RegistrationService
from .report_service import ReportService
from .opt_service import OptService

logger = logging.getLogger('openleadr')


class OpenADRServer:
    """
    The OpenADR Server (Virtual Top Node).
    """

    def __init__(self, vtn_id, cert=None, key=None, cert_depth=3, http_port=8080,
                 http_host='127.0.0.1', http_cert=None, http_key=None,
                 http_ca_file=None, http_cert_path=None, http_key_path=None,
                 ven_lookup=None, fingerprint_lookup=None, show_fingerprint=True,
                 http_path_prefix='/OpenADR2/Simple/2.0b',
                 requested_poll_freq=timedelta(seconds=10),
                 on_created_event=None,
                 on_request_event=None):
        """
        Initializes a new OpenADRServer object.

        :param str vtn_id: The ID for this VTN.
        :param str cert: The path to a PEM-formatted certificate file.
        :param str key: The path to a PEM-formatted private key file.
        :param int cert_depth: The depth of the certificate chain to verify.
        :param int http_port: The port to run the HTTP server on.
        :param str http_host: The host to run the HTTP server on.
        :param str http_cert: The path to the certificate for the HTTP server.
        :param str http_key: The path to the private key for the HTTP server.
        :param callable ven_lookup: A callable that will receive a ven_id and should return a
                                    dict with the ven's certificate's fingerprint and its
                                    registration_id.
        :param callable fingerprint_lookup: A callable that will receive a ven_id and should
                                            return a dict with the ven's certificate's
                                            fingerprint.
        :param bool show_fingerprint: Whether to show the fingerprint of a connecting VEN.
                                      Defaults to True.
        :param timedelta requested_poll_freq: The poll frequency that is requested from the VEN.
        """
        self.vtn_id = vtn_id
        self.app = None
        self.http_port = http_port
        self.http_host = http_host
        self.http_cert_path = http_cert
        self.http_key_path = http_key
        self.http_ca_file = http_ca_file
        self.http_path_prefix = http_path_prefix
        self.cert_path = cert
        self.key_path = key
        self.cert_depth = cert_depth
        self.show_fingerprint = show_fingerprint
        self.requested_poll_freq = requested_poll_freq

        self.services = {'event_service': EventService(vtn_id),
                         'poll_service': PollService(vtn_id),
                         'registration_service': RegistrationService(vtn_id),
                         'report_service': ReportService(vtn_id),
                         'opt_service': OptService(vtn_id)}

        self.ven_lookup = ven_lookup or self.services['registration_service'].ven_lookup
        self.fingerprint_lookup = fingerprint_lookup or self.ven_lookup

        # Set up the message queues
        self.queues = self.services['poll_service'].queues

        # Set up the service map
        self.service_map = {
            'oadrQueryRegistration': self.on_query_registration,
            'oadrCreatePartyRegistration': self.on_create_party_registration,
            'oadrCancelPartyRegistration': self.on_cancel_party_registration,
            'oadrRequestReregistration': self.on_reregister_party_registration,
            'oadrCanceledPartyRegistration': self.on_canceled_party_registration,

            'oadrPoll': self.on_poll,

            'oadrRequestEvent': self.on_request_event,
            'oadrCreatedEvent': self.on_created_event,

            'oadrRegisterReport': self.on_register_report,
            'oadrRegisteredReport': self.on_registered_report,
            'oadrCreateReport': self.on_create_report,
            'oadrCreatedReport': self.on_created_report,
            'oadrUpdateReport': self.on_update_report,
            'oadrUpdatedReport': self.on_updated_report,
            'oadrCancelReport': self.on_cancel_report,
            'oadrCanceledReport': self.on_canceled_report,

            'oadrCreateOpt': self.on_create_opt,
            'oadrCancelOpt': self.on_cancel_opt,
        }

        # Handlers for push-based services
        if on_created_event:
            self.add_handler('on_created_event', on_created_event)

        if on_request_event:
            self.add_handler('on_request_event', on_request_event)

    def run(self):
        """
        Run the server in a blocking fashion.
        """
        loop = asyncio.get_event_loop()
        loop.create_task(self.start())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            loop.run_until_complete(self.stop())

    async def start(self):
        """
        Run the server in a non-blocking fashion.
        """
        await self.setup()
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.http_host, self.http_port)
        await site.start()
        logger.info(f"Server running on {self.http_host}:{self.http_port}")

    async def stop(self):
        if self.app:
            await self.app.shutdown()
            await self.app.cleanup()
        logger.info("Server stopped.")

    async def setup(self):
        """
        Sets up the server and routes.
        """
        self.app = web.Application()
        self.app.add_routes([web.post(f'{self.http_path_prefix}/{{service}}', self.handle_request)])

    def add_handler(self, name, func):
        """
        Add a handler for a specific message type.

        :param str name: The name for the handler. Can be on_create_party_registration,
                         on_cancel_party_registration, on_register_report, on_create_report,
                         on_update_report, on_poll, on_request_event.
        :param callable func: A function or coroutine that will handle this message.
        """
        if name in self.service_map:
            self.service_map[name] = partial(func, self.service_map[name])
        elif name.startswith('on_'):
            self.services['event_service'].add_handler(name, func)
            self.services['report_service'].add_handler(name, func)
            self.services['registration_service'].add_handler(name, func)
        else:
            raise NameError(f"Unknown handler {name}. Valid handlers are on_create_party_registration, "
                            "on_cancel_party_registration, on_register_report, on_create_report, "
                            "on_update_report, on_poll, on_request_event.")

    async def handle_request(self, request):
        """
        This is the main handler for all incoming HTTP requests.
        """
        payload_str = await request.text()
        logger.debug(f"Incoming request: {payload_str}")

        try:
            # We don't know the VEN ID yet, so we can't validate it against a registration.
            # We will therefore only check if the message is formally correct.
            message_type, message_payload = parse_message(payload_str)
        except Exception as err:
            logger.warning(f"Could not parse message: {err}")
            response_payload = {'response_code': 400,
                                'response_description': f"Could not parse message: {err}"}
            response_type = 'oadrResponse'
            response_xml = render_message(response_type, **response_payload)
            return web.Response(text=response_xml,
                                content_type='application/xml')

        handler = self.service_map.get(message_type)
        if handler:
            # We found a handler for this message type, so we can handle it.
            # The VEN ID is now available, so we can look up its fingerprint
            # and validate future messages from this VEN.
            ven_id = get_ven_id(message_payload)

            if self.show_fingerprint:
                peer_cert = request.transport.get_extra_info('peercert')
                if peer_cert:
                    fingerprint = get_cert_fingerprint(peer_cert)
                    logger.info(f"Receiving message {message_type} from ven_id:"
                                f"{ven_id} with fingerprint: {fingerprint}")

            if self.fingerprint_lookup:
                await validate_ven_id(ven_id, request, self.fingerprint_lookup)

            if inspect.iscoroutinefunction(handler):
                response_type, response_payload = await handler(message_payload)
            else:
                response_type, response_payload = handler(message_payload)
            response_xml = render_message(response_type, **response_payload)
            logger.debug(f"Returning response: {response_xml}")
            return web.Response(text=response_xml,
                                content_type='application/xml')
        else:
            # We don't have a handler for this message type, so we return an error.
            logger.warning(f"No handler for message type {message_type} found.")
            response_payload = {'response_code': 400,
                                'response_description': f"No handler for message type {message_type} found."}
            response_type = 'oadrResponse'
            response_xml = render_message(response_type, **response_payload)
            return web.Response(text=response_xml,
                                content_type='application/xml')

    def add_event(self, ven_id, signal_name, signal_type, intervals, **kwargs):
        """
        Add a new event to the queue for a specific VEN.
        """
        return self.services['event_service'].add_event(ven_id, signal_name, signal_type, intervals, **kwargs)

    def cancel_event(self, ven_id, event_id):
        """
        Mark a specific event as 'cancelled'.
        """
        return self.services['event_service'].cancel_event(ven_id, event_id)

    async def on_query_registration(self, payload):
        """
        The VEN is asking for its registration details.
        """
        return await self.services['registration_service']._on_query_registration(payload)

    async def on_create_party_registration(self, payload):
        """
        The VEN is trying to register.
        """
        payload['requested_poll_freq'] = self.requested_poll_freq
        return await self.services['registration_service']._on_create_party_registration(payload)

    async def on_cancel_party_registration(self, payload):
        """
        The VEN is trying to cancel its registration.
        """
        return await self.services['registration_service']._on_cancel_party_registration(payload)

    async def on_reregister_party_registration(self, payload):
        """
        The VEN is trying to re-register.
        """
        return await self.services['registration_service']._on_reregister_party_registration(payload)

    async def on_canceled_party_registration(self, payload):
        """
        The VEN is informing us that it has canceled its registration.
        """
        logger.info("VEN has canceled its registration.")
        return 'oadrResponse', {}

    async def on_poll(self, payload):
        """
        The VEN is polling for new messages.
        """
        return await self.services['poll_service']._on_poll(payload)

    async def on_request_event(self, payload):
        """
        The VEN is requesting events.
        """
        ven_id = payload['ven_id']
        return await self.services['event_service']._on_request_event(payload)

    async def on_created_event(self, payload):
        """
        The VEN has responded to an event.
        """
        return await self.services['event_service']._on_created_event(payload)

    async def on_register_report(self, payload):
        """
        The VEN is registering a report.
        """
        return await self.services['report_service']._on_register_report(payload)

    async def on_registered_report(self, payload):
        """
        The VEN has confirmed a report registration.
        """
        return await self.services['report_service']._on_registered_report(payload)

    async def on_create_report(self, payload):
        """
        A VEN is pushing a report to us.
        """
        return await self.services['report_service']._on_create_report(payload)

    async def on_created_report(self, payload):
        """
        The VEN is acknowledging a report request from us.
        """
        return await self.services['report_service']._on_created_report(payload)

    async def on_update_report(self, payload):
        """
        A VEN is pushing a report to us.
        """
        return await self.services['report_service']._on_update_report(payload)

    async def on_updated_report(self, payload):
        """
        The VEN is acknowledging a report request from us.
        """
        return await self.services['report_service']._on_updated_report(payload)

    async def on_cancel_report(self, payload):
        return await self.services['report_service']._on_cancel_report(payload)

    async def on_canceled_report(self, payload):
        return await self.services['report_service']._on_canceled_report(payload)

    async def on_create_opt(self, payload):
        """
        Call the OptService to handle oadrCreateOpt.
        """
        return await self.services['opt_service']._create_opt(payload)

    async def on_cancel_opt(self, payload):
        """
        Call the OptService to handle oadrCancelOpt.
        """
        return await self.services['opt_service']._cancel_opt(payload)

    @property
    def registered_vens(self):
        return self.services['registration_service'].registered_vens

