import json
import os
from datetime import datetime
from datetime import timezone
from functools import partial
from functools import wraps
import json

import traceback
import time
import logging
import logging.handlers
import sys
import uuid
import yaml

try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote

import dateutil.parser

from tornado.gen import coroutine, multi
from tornado.locks import Semaphore
# from tornado.log import app_log
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.options import define, options, parse_command_line

# consts
LOG_NAME = 'Cull-Service'
LOG_FORMAT = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s:%(funcName)s - [%(levelname)s] %(message)s'

CONFIG_PATH = './config.yaml'

with open(CONFIG_PATH) as config_file:
    config_str = config_file.read()
    configs = yaml.load(config_str)

    LOG_LEVEL = configs['log_level']

    hub_url = configs['jupyterhub_url']
    hub_api_prefix = configs['jupyterhub_api_prefix']
    hub_api_token = configs['jupyterhub_api_token']

    hub_api_url = '{}{}'.format(hub_url, hub_api_prefix)

    es_service_url = configs['es_service_url']
    tenant = configs['tenant']

    timeout = configs['timeout']
    cull_interval = configs['cull_interval']
    max_age = configs['max_age']
    cull_users = configs['cull_users']
    concurrency = configs['concurrency']

def setup_logger(level):
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)

    logger = logging.getLogger(LOG_NAME)
    logger.addHandler(handler)
    logger.setLevel(level)

    return logger

logger = setup_logger(int(LOG_LEVEL))
logger.info('\n*** Cull-Service ***\n\nGot configs:\nhub_api_url: {}\ntimeout: {}\ncull_interval: {}\nmax_age: {}\ncull_users: {}\nconcurrency: {}\n'.format(
    hub_api_url,
    timeout,
    cull_interval,
    max_age,
    cull_users,
    concurrency
))

def parse_date(date_string):
    """Parse a timestamp

    If it doesn't have a timezone, assume utc

    Returned datetime object will always be timezone-aware
    """
    dt = dateutil.parser.parse(date_string)
    if not dt.tzinfo:
        # assume naïve timestamps are UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def format_td(td):
    """
    Nicely format a timedelta object

    as HH:MM:SS
    """
    if td is None:
        return "unknown"
    if isinstance(td, str):
        return td
    seconds = int(td.total_seconds())
    h = seconds // 3600
    seconds = seconds % 3600
    m = seconds // 60
    seconds = seconds % 60
    return "{h:02}:{m:02}:{seconds:02}".format(h=h, m=m, seconds=seconds)


@coroutine
def cull_idle(
    url, api_token, inactive_limit, cull_users=False, max_age=0, concurrency=10
):
    """Shutdown idle single-user servers

    If cull_users, inactive *users* will be deleted as well.
    """
    auth_header = {'Authorization': 'token %s' % api_token}
    req = HTTPRequest(url=url + '/users', headers=auth_header)
    now = datetime.now(timezone.utc)
    client = AsyncHTTPClient()

    if concurrency:
        semaphore = Semaphore(concurrency)

        @coroutine
        def fetch(req):
            """client.fetch wrapped in a semaphore to limit concurrency"""
            yield semaphore.acquire()
            try:
                return (yield client.fetch(req))
            finally:
                yield semaphore.release()

    else:
        fetch = client.fetch

    resp = yield fetch(req)
    users = json.loads(resp.body.decode('utf8', 'replace'))
    futures = []

    @coroutine
    def handle_server(user, server_name, server):
        """Handle (maybe) culling a single server

        Returns True if server is now stopped (user removable),
        False otherwise.
        """
        log_name = user['name']
        if server_name:
            log_name = '%s/%s' % (user['name'], server_name)
        if server.get('pending'):
            '''
            app_log.warning(
                "Not culling server %s with pending %s", log_name, server['pending']
            )
            '''
            logger.warning('Not culling server {} with pending {}'.format(
                log_name,
                server['pending']
            ))
            return False

        # jupyterhub < 0.9 defined 'server.url' once the server was ready
        # as an *implicit* signal that the server was ready.
        # 0.9 adds a dedicated, explicit 'ready' field.
        # By current (0.9) definitions, servers that have no pending
        # events and are not ready shouldn't be in the model,
        # but let's check just to be safe.

        if not server.get('ready', bool(server['url'])):
            '''
            app_log.warning(
                "Not culling not-ready not-pending server %s: %s", log_name, server
            )
            '''
            logger.warning('Not culling not-ready not-pending server {}: {}'.format(
                log_name,
                server
            ))
            return False

        if server.get('started'):
            age = now - parse_date(server['started'])
        else:
            # started may be undefined on jupyterhub < 0.9
            age = None

        # check last activity
        # last_activity can be None in 0.9
        if server['last_activity']:
            inactive = now - parse_date(server['last_activity'])
        else:
            # no activity yet, use start date
            # last_activity may be None with jupyterhub 0.9,
            # which introduces the 'started' field which is never None
            # for running servers
            inactive = age

        should_cull = (
            inactive is not None and inactive.total_seconds() >= inactive_limit
        )
        if should_cull:
            '''
            app_log.info(
                "Culling server %s (inactive for %s)", log_name, format_td(inactive)
            )
            '''
            logger.info('Culling server {} (inactive for {})'.format(
                log_name,
                format_td(inactive)
            ))

        if max_age and not should_cull:
            # only check started if max_age is specified
            # so that we can still be compatible with jupyterhub 0.8
            # which doesn't define the 'started' field
            if age is not None and age.total_seconds() >= max_age:
                '''
                app_log.info(
                    "Culling server %s (age: %s, inactive for %s)",
                    log_name,
                    format_td(age),
                    format_td(inactive),
                )
                '''
                logger.info('Culling server {} (age: {}, inactive for {})'.format(
                    log_name,
                    format_td(age),
                    format_td(inactive)
                ))
                should_cull = True

        if not should_cull:
            '''
            app_log.debug(
                "Not culling server %s (age: %s, inactive for %s)",
                log_name,
                format_td(age),
                format_td(inactive),
            )
            '''
            logger.info('Not culling server {} (age: {}, inactive for {})'.format(
                log_name,
                format_td(age),
                format_td(inactive)
            ))
            return False

        if server_name:
            # culling a named server
            delete_url = url + "/users/%s/servers/%s" % (
                quote(user['name']),
                quote(server['name']),
            )
        else:
            delete_url = url + '/users/%s/server' % quote(user['name'])

        # TODO : call es_service to record stopping event
        body = json.dump({
            'user_name': user['name'],
            'end': now,
            'last_activity': server['last_activity'],
            'tenant_id': tenant
        })
        print(body)
        '''
        req = HTTPRequest(
            url='{}/notify/end'.format(es_service_url),
            method='POST',
            body=body
        )
        resp = yield fetch(req)
        logger.debug('es_service req: {}\nresp: {}'.format(body, resp.code))
        '''
        
        req = HTTPRequest(url=delete_url, method='DELETE', headers=auth_header)
        resp = yield fetch(req)
        if resp.code == 202:
            # app_log.warning("Server %s is slow to stop", log_name)
            logger.warning('Server {} is slow to stop'.format(log_name))
            # return False to prevent culling user with pending shutdowns
            return False
        return True

    @coroutine
    def handle_user(user):
        """Handle one user.

        Create a list of their servers, and async exec them.  Wait for
        that to be done, and if all servers are stopped, possibly cull
        the user.
        """
        # shutdown servers first.
        # Hub doesn't allow deleting users with running servers.
        # jupyterhub 0.9 always provides a 'servers' model.
        # 0.8 only does this when named servers are enabled.
        if 'servers' in user:
            servers = user['servers']
        else:
            # jupyterhub < 0.9 without named servers enabled.
            # create servers dict with one entry for the default server
            # from the user model.
            # only if the server is running.
            servers = {}
            if user['server']:
                servers[''] = {
                    'last_activity': user['last_activity'],
                    'pending': user['pending'],
                    'url': user['server'],
                }
        server_futures = [
            handle_server(user, server_name, server)
            for server_name, server in servers.items()
        ]
        results = yield multi(server_futures)
        if not cull_users:
            return
        # some servers are still running, cannot cull users
        still_alive = len(results) - sum(results)
        if still_alive:
            '''
            app_log.debug(
                "Not culling user %s with %i servers still alive",
                user['name'],
                still_alive,
            )
            '''
            logger.debug('Not culling user {} with {} servers still alive'.format(
                user['name'],
                still_alive
            ))
            return False

        should_cull = False
        if user.get('created'):
            age = now - parse_date(user['created'])
        else:
            # created may be undefined on jupyterhub < 0.9
            age = None

        # check last activity
        # last_activity can be None in 0.9
        if user['last_activity']:
            inactive = now - parse_date(user['last_activity'])
        else:
            # no activity yet, use start date
            # last_activity may be None with jupyterhub 0.9,
            # which introduces the 'created' field which is never None
            inactive = age

        should_cull = (
            inactive is not None and inactive.total_seconds() >= inactive_limit
        )
        if should_cull:
            # app_log.info("Culling user %s (inactive for %s)", user['name'], inactive)
            logger.debug('Culling user {} (inactive for {})'.format(
                user['name'],
                inactive
            ))

        if max_age and not should_cull:
            # only check created if max_age is specified
            # so that we can still be compatible with jupyterhub 0.8
            # which doesn't define the 'started' field
            if age is not None and age.total_seconds() >= max_age:
                '''
                app_log.info(
                    "Culling user %s (age: %s, inactive for %s)",
                    user['name'],
                    format_td(age),
                    format_td(inactive),
                )
                '''
                logger.info('Culling user {} (age: {}, inactive for {})'.format(
                    user['name'],
                    format_td(age),
                    format_td(inactive)
                ))
                should_cull = True

        if not should_cull:
            '''
            app_log.debug(
                "Not culling user %s (created: %s, last active: %s)",
                user['name'],
                format_td(age),
                format_td(inactive),
            )
            '''
            logger.info('Not culling user {} (age: {}, inactive for {})'.format(
                user['name'],
                format_td(age),
                format_td(inactive)
            ))
            return False

        req = HTTPRequest(
            url=url + '/users/%s' % user['name'], method='DELETE', headers=auth_header
        )
        yield fetch(req)
        return True

    for user in users:
        futures.append((user['name'], handle_user(user)))

    for (name, f) in futures:
        try:
            result = yield f
        except Exception:
            # app_log.exception("Error processing %s", name)
            logger.error('Error processing {}'.format(name))
        else:
            if result:
                # app_log.debug("Finished culling %s", name)
                logger.debug('Finish culling {}'.format(name))

if __name__ == '__main__':
    try:
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
    except ImportError as e:
        '''
        app_log.warning(
            "Could not load pycurl: %s\n"
            "pycurl is recommended if you have a large number of users.",
            e,
        )
        '''
        logger.warning(
            'Could not load pycurl: {}\npycurl is recommended if you have a large number of users.'.format(e)
        )

    loop = IOLoop.current()
    cull = partial(
        cull_idle,
        url=hub_api_url,
        api_token=hub_api_token,
        inactive_limit=timeout,
        cull_users=cull_users,
        max_age=max_age,
        concurrency=concurrency,
    )

    # schedule first cull immediately
    # because PeriodicCallback doesn't start until the end of the first interval
    loop.add_callback(cull)
    # schedule periodic cull
    pc = PeriodicCallback(cull, 1e3 * cull_interval)
    pc.start()
    try:
        loop.start()
    except KeyboardInterrupt:
        pass
