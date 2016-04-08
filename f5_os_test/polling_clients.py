# Copyright 2016 F5 Networks Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
'''This module contains a set of OS Client-specific Polling Managers.

   These managers are intended to map 1-1 to OS clients, and provide event
drive polling monitors for their methods.  Because clients and their methods
are idiosyncratic in OS there's little scope for a generalized (cross-client)
manager, and I'm cautious about excessively abstract method polling, e.g.
making each manager simply implement a decorator for all methods.

IF a client has methods that provide a uniform means of observing state changes
then we could probably effectively used such a decorator, but I'm not yet
familiar enough with OS to make that leap.
'''
from f5.bigip import BigIP
from neutronclient.common.exceptions import NotFound
from neutronclient.common.exceptions import StateInvalidClient
from pprint import pprint as pp
import pytest
import time


# flake8 hack
pp(BigIP)


class MaximumNumberOfAttemptsExceeded(Exception):
    pass


class PollingMixin(object):
    '''Use this mixin to poll for resource entering 'target' from other.'''
    def poll(self, observer, resource_id,
             status_reader, target_status='ACTIVE'):
        current_state = observer(resource_id)
        current_status = status_reader(current_state)
        attempts = 0
        while current_status != target_status:
            time.sleep(self.interval)
            current_state = observer(resource_id)
            current_status = status_reader(current_state)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return current_state


class NeutronClientPollingManager(PollingMixin):
    '''Invokes Neutronclient methods and polls for target expected states.'''
    def __init__(self, neutronclient, **kwargs):
        pp("got here in the constructor")
        self.interval = kwargs.pop('interval', .4)
        self.max_attempts = kwargs.pop('max_attempts', 12)
        if kwargs:
            raise TypeError("Unexpected **kwargs: %r" % kwargs)
        self.client = neutronclient

    def create_loadbalancer(self, lbconf):
        print("Entered manager create_loadbalancer.")
        init_lb = self.client.create_loadbalancer(lbconf)
        lbid = init_lb['loadbalancer']['id']

        def lb_reader(loadbalancer):
            return loadbalancer['loadbalancer']['provisioning_status']
        print("About to start polling creation.")
        return self.poll(self.client.show_loadbalancer, lbid, lb_reader)

    def _lb_delete_helper(self, lbid):
        try:
            self.client.delete_loadbalancer(lbid)
        except NotFound:
            return True
        return False

    def delete_loadbalancer(self, lbid):
        attempts = 0
        while not self._lb_delete_helper(lbid):
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def delete_all_loadbalancers(self):
        for lb in self.client.list_loadbalancers()['loadbalancers']:
            self.client.delete_loadbalancer(lb['id'])
        balancers = self.client.list_loadbalancers()['loadbalancers']
        attempts = 0
        while balancers:
            time.sleep(self.interval)
            balancers = self.client.list_loadbalancers()['loadbalancers']
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def delete_all_listeners(self):
        for listener in self.client.list_listeners()['listeners']:
            self.client.delete_listener(listener['id'])
        attempts = 0
        while self.client.list_listeners()['listeners']:
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def create_listener(self, listener_conf):
        init_listener = self.client.create_listener(listener_conf)
        # The dict returned by show listener doesn't have a status.
        lids = [l['id'] for l in self.client.list_listeners()['listeners']]
        attempts = 0
        while init_listener['listener']['id'] not in lids:
            time.sleep(self.interval)
            lids = [l['id'] for l in self.client.list_listeners()['listeners']]
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return init_listener

    def delete_listener(self, listener_id):
        self.client.delete_listener(listener_id)
        lids = [l['id'] for l in self.client.list_listeners()['listeners']]
        attempts = 0
        while listener_id in lids:
            time.sleep(self.interval)
            lids = [l['id'] for l in self.client.list_listeners()['listeners']]
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def delete_all_lbaas_pools(self):
        for pool in self.client.list_lbaas_pools()['pools']:
            try:
                self.delete_lbaas_pool(pool['id'])
            except NotFound:
                continue
        attempts = 0
        pp(self.client.list_lbaas_pools()['pools'])
        while self.client.list_lbaas_pools()['pools']:
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def _poll_call_with_exceptions(self, exceptional, call, *args, **kwargs):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                print(args)
                retval = call(*args)
            except exceptional as e:
                pp(type(e))
                pp(e.message)
                time.sleep(self.interval)
                attempts = attempts + 1
                if attempts > self.max_attempts:
                    raise MaximumNumberOfAttemptsExceeded
                continue
            break
        return retval

    def delete_lbaas_pool(self, pool_id):
        self.delete_all_lbaas_pool_members(pool_id)
        self._poll_call_with_exceptions(
            StateInvalidClient,
            self.client.delete_lbaas_pool,
            pool_id)
        attempts = 0
        while pool_id in\
                [p['id'] for p in self.client.list_lbaas_pools()['pools']]:
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def create_lbaas_pool(self, pool_config):
        pool = self._poll_call_with_exceptions(
            StateInvalidClient,
            self.client.create_lbaas_pool,
            pool_config)
        attempts = 0
        pool_id = pool['pool']['id']
        while pool_id not in\
                [p['id'] for p in self.client.list_lbaas_pools()['pools']]:
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return pool

    def create_lbaas_member(self, pool_id, member_config):
        member = self._poll_call_with_exceptions(
            StateInvalidClient,
            self.client.create_lbaas_member,
            pool_id, member_config)
        attempts = 0
        member_id = member['member']['id']
        while member_id not in [
                m['id'] for m in
                self.client.list_lbaas_members(pool_id)['members']
                ]:
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return member

    def delete_lbaas_member(self, member_id, pool_id):
        self._poll_call_with_exceptions(
            StateInvalidClient,
            self.client.delete_lbaas_member,
            member_id, pool_id)
        attempts = 0
        while member_id in [
                m['id'] for m in
                self.client.list_lbaas_members(pool_id)['members']
                ]:
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def delete_all_lbaas_pool_members(self, pool_id):
        for member in self.client.list_lbaas_members(pool_id)['members']:
            try:
                self.delete_lbaas_member(member['id'], pool_id)
            except NotFound:
                continue
        attempts = 0
        pp(self.client.list_lbaas_members(pool_id)['members'])
        while self.client.list_lbaas_members(pool_id)['members']:
            time.sleep(self.interval)
            attempts = attempts + 1
            if attempts > self.max_attempts:
                raise MaximumNumberOfAttemptsExceeded
        return True

    def __getattr__(self, name):
        if hasattr(self.client, name):
            return getattr(self.client, name)


class HeatClientPollingManager(PollingMixin):
    '''Utilizes heat client to create/delete heat stacks.'''

    default_stack_config = {
        'files': {},
        'disable_rollback': True,
        'environment': {},
        'tags': None,
        'environment_files': None
    }

    def __init__(self, heatclient, **kwargs):
        self.client = heatclient
        self.interval = kwargs.pop('interval', 2)
        self.attempts = kwargs.pop('attempts', 20)

        if kwargs:
            raise TypeError('Unexpected **kwargs: {}'.format(kwargs))

    def stack_status(self, stack):
        return stack.stack_status

    def create_stack(self, configuration):
        configuration.update(self.default_stack_config)
        stack = self.client.stacks.create(**configuration)
        return self.poll(self.client.stacks.get, stack.id, self.stack_status)

    def delete_stack(self, stack_id):
        self.client.stacks.delete(stack_id)
        try:
            self.poll(self.client.stacks.get, stack_id, self.stack_status)
        except Exception as ex:
            if 'could not be found' not in ex:
                raise


@pytest.fixture
def polling_neutronclient():
    '''Invokes Neutronclient methods and polls for target expected states.'''
    return NeutronClientPollingManager


@pytest.fixture
def polling_heatclient():
    '''Access to HeatClient polling for create/delete/modify of heat stack.'''
    return HeatClientPollingManager
