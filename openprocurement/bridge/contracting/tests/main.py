# -*- coding: utf-8 -*-
import unittest

from openprocurement.bridge.contracting.tests import databridge, utils


def suite():
    suite = unittest.TestSuite()
    suite.addTest(databridge.suite())
    suite.addTest(utils.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
