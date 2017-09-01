"""
This procedure will test the functions of the models_graph module.
"""

import unittest
from lib.datastore import Datastore
from lib import mf_env


# @unittest.skip("Focus on Coverage")
class TestDatastore(unittest.TestCase):

    def setUp(self):
        # projectdir = "C:\\ProjectsWorkspace\\Vo\\MOW_Dataroom\\meldpuntfietspaden\\"
        # projectfile = projectdir + "test_datastore.py"
        self.cfg = mf_env.init_env("meldpuntfietspaden", __file__)
        self.ds = Datastore(self.cfg)

    def test_get_uri(self):
        uri = self.ds.get_uri("gemeente", "Oostende")
        self.assertEqual(uri, "http://id.fedstats.be/nis/35013#id", msg="Valid input, valid output")
        uri = self.ds.get_uri("gemeente", "Aarschot")
        self.assertEqual(uri, "http://id.fedstats.be/nis/24001#id", msg="Valid input, valid output")
        uri = self.ds.get_uri("gemeenteX", "Oostende")
        self.assertFalse(uri, msg="Invalid dimensie")
        uri = self.ds.get_uri("gemeente", "OostendeX")
        self.assertFalse(uri, msg="Invalid element")
        uri = self.ds.get_uri("hoofdvervoerswijze", "trein")
        self.assertEqual(uri, "", "Valid query, no URI")

    def test_get_waarde_from_uri(self):
        waarde = self.ds.get_waarde_from_uri("http://id.fedstats.be/nis/30000#id")
        self.assertEqual(waarde, "West-Vlaanderen", "Valid input, valid output")
        waarde = self.ds.get_waarde_from_uri("http://id.fedstats.be/nis/3000X#id")
        self.assertFalse(waarde, "Invalid URI")

if __name__ == "__main__":
    unittest.main()
