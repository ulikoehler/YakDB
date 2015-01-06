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
        testset = [(b'L1','x\x00y'),(b'X','a\x00y')]
        result = InvertedIndex._processReadResult(testset)
        self.assertDictEqual(result, {b'L1': {b'x', b'y'}, b'X': {b'a', b'y'}})
        #Test 2
        testset = [(b'L2', b'x\x00y'),(b'L3', b'a\x00y'),(b'L4',b'z\x00y')]
        result = InvertedIndex._processReadResult(testset)
        self.assertDictEqual(result, {b'L2': {b'x', b'y'}, b'L3': {b'a', b'y'}, b'L4': {b'z', b'y'}})

    def testProcessScanResult(self):
        testset = [('L1\x1Ef','x\x00y'),('X\x1Eg','y\x00a\x00y'),('L1\x1Efoo','z\x00y')]
        result = InvertedIndex._processScanResult(testset)
        self.assertEqual(result, [{b"x", b"y"}, {b"y", b"a"}, {b"z", b"y"}])
        #Test 2
        testset = [('L2\x1Ef','x\x00y'),('L3\x1Eg','a\x00y'),('L4\x1Efoo','z\x00y')]
        result = InvertedIndex._processScanResult(testset)
        self.assertEqual(result, [{b"x", b"y"}, {b"a", b"y"}, {b"z", b"y"}])

    def testSplitValues(self):
        #Test 1: empty
        res = InvertedIndex.splitValues(b"")
        self.assertEqual(res, set())
        #Test 2: Simple set
        res = InvertedIndex.splitValues(b"a\x00b\x00cd\x00ef")
        self.assertEqual(res, {b"a", b"b", b"cd", b"ef"})
        #Test 3: Set with duplicates and empty elements
        res = InvertedIndex.splitValues(b"a\x00b\x00cd\x00a\x00\x00ef\x00")
        self.assertEqual(res, {b"a", b"b", b"cd", b"ef", b""})


if __name__ == '__main__':
    unittest.main()
