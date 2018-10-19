# -*- coding: utf-8 -*-
import unittest

from openprocurement.bridge.contracting.tests import utils, handlers


def suite():
    suite = unittest.TestSuite()
    suite.addTest(utils.suite())
    suite.addTest(handlers.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
