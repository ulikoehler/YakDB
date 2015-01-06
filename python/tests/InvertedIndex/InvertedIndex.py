#!/usr/bin/env python3
from YakDB.InvertedIndex.InvertedIndex import *
import unittest

class TestInvertedIndex(unittest.TestCase):
    def assertDictEqual(self, d1, d2):
        "Utility to check if two given dicts with set values are equal"
        self.assertEqual(d1.keys(), d2.keys())
        for k1 in d1.keys():
            self.assertTrue(k1 in d2)
            self.assertEqual(d1[k1], d2[k1])

    def testProcessReadResult(self):
        testset = [('L1','x\x00y'),('X','a\x00y')]
        result = InvertedIndex._processReadResult(testset)
        self.assertDictEqual(result, {'L1': {'x', 'y'}, 'X':{'a', 'y'}})
        #Test 2
        testset = [('L2','x\x00y'),('L3','a\x00y'),('L4','z\x00y')]
        result = InvertedIndex._processReadResult(testset)
        self.assertDictEqual(result, {'L2': {'x', 'y'}, 'L3': {'a', 'y'}, 'L4': {'z', 'y'}})

    def testProcessScanResult(self):
        testset = [('L1\x1Ef','x\x00y'),('X\x1Eg','y\x00a\x00y'),('L1\x1Efoo','z\x00y')]
        result = InvertedIndex._processScanResult(testset)
        self.assertEqual(result, [{"x", "y"}, {"y", "a"}, {"z", "y"}])
        #Test 2
        testset = [('L2\x1Ef','x\x00y'),('L3\x1Eg','a\x00y'),('L4\x1Efoo','z\x00y')]
        result = InvertedIndex._processScanResult(testset)
        self.assertEqual(result, [{"x", "y"}, {"a", "y"}, {"z", "y"}])


if __name__ == '__main__':
    unittest.main()
