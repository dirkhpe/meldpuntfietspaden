"""
This procedure will test the functions of the models_graph module.
"""

import unittest
from lib.datastore import Datastore
from lib import mf_env


# @unittest.skip("Focus on Coverage")
class TestDatastore(unittest.TestCase):

    def setUp(self):
        self.cfg = mf_env.init_env("meldpuntfietspaden", __file__)
        self.ds = Datastore(self.cfg)

    def test_get_uri(self):
        uri = self.ds.get_uri("gemeente", "Oostende")
        self.assertEqual(uri, "")

if __name__ == "__main__":
    unittest.main()
