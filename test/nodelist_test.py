import unittest
import time
import logging

from phonon import TTL
from phonon.nodelist import Nodelist
import phonon.connections

logging.disable(logging.CRITICAL)


def s_to_ms(s):
    return int(1000. * s)


class NodelistTest(unittest.TestCase):

    def setUp(self):
        self.conn = phonon.connections.connect(hosts=['localhost'])
        if hasattr(self.conn, "client"):
            self.conn.client.flushdb()

    def test_create_node_list(self):
        nodelist = Nodelist("key")
        self.assertEqual(nodelist.nodelist_key, "phonon_key.nodelist")
        self.assertNotEqual(self.conn.client.hgetall(nodelist.nodelist_key), {})

    def test_refresh_session_refreshes_time(self):
        nodelist = Nodelist("key")
        now = int(time.time() * 1000.)
        self.conn.client.hset(nodelist.nodelist_key, self.conn.id, now)
        time.sleep(0.01)
        nodelist.refresh_session()
        updated_now = nodelist.get_last_updated(self.conn.id)
        self.assertIsInstance(updated_now, int)
        self.assertNotEqual(updated_now, now)

    def test_find_expired_nodes(self):
        now = int(time.time() * 1000.)
        expired = now - s_to_ms(2 * TTL + 1)

        nodelist = Nodelist("key")

        self.conn.client.hset(nodelist.nodelist_key, '1', now)
        self.conn.client.hset(nodelist.nodelist_key, '2', expired)

        target = nodelist.find_expired_nodes()
        self.assertIn(b'2', target)
        self.assertNotIn(b'1', target)

    def test_remove_expired_nodes(self):
        now = int(time.time() * 1000.)
        expired = now - s_to_ms(2 * TTL + 1)

        nodelist = Nodelist("key")

        self.conn.client.hset(nodelist.nodelist_key, '1', expired)
        self.conn.client.hset(nodelist.nodelist_key, '2', expired)

        nodes = nodelist.get_all_nodes()
        self.assertIn(b'1', nodes)
        self.assertIn(b'2', nodes)

        nodelist.remove_expired_nodes()
        nodes = nodelist.get_all_nodes()
        self.assertNotIn(b'1', nodes)
        self.assertNotIn(b'2', nodes)

    def test_refreshed_node_not_deleted(self):
        now = int(time.time() * 1000.)
        expired = now - s_to_ms(2 * TTL + 1)

        nodelist = Nodelist('key')

        self.conn.client.hset(nodelist.nodelist_key, '1', expired)
        self.conn.client.hset(nodelist.nodelist_key, '2', expired)

        expired = nodelist.find_expired_nodes()
        self.assertIn(b'2', expired)
        self.assertIn(b'1', expired)
        self.conn.client.hset(nodelist.nodelist_key, '1', now)

        nodelist.refresh_session('1')
        nodelist.remove_expired_nodes(expired)

        self.assertNotEqual(nodelist.get_last_updated('1'), None)
        self.assertIs(nodelist.get_last_updated('2'), None)

    def test_remove_node(self):
        nodelist = Nodelist('key')
        nodelist.refresh_session('1')

        nodes = nodelist.get_all_nodes()
        self.assertIn(b'1', nodes)

        nodelist.remove_node('1')
        nodes = nodelist.get_all_nodes()
        self.assertNotIn(b'1', nodes)

    def test_clear_nodelist(self):
        nodelist = Nodelist('key')
        nodes = nodelist.clear_nodelist()
        nodes = nodelist.get_all_nodes()
        self.assertEqual(nodes, {})
