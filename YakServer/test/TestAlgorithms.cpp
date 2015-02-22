#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <set>
#include <iostream>
#include <string>
#include <cstring>
#include "MergeAlgorithms.hpp"

using namespace std;

BOOST_AUTO_TEST_SUITE(MergeAlgorithms)


//Utility to avoid 4-argument BOOST_CHECK_EQUAL_COLLECTIONS
#define BOOST_CHECK_EQUAL_COLLECTIONS_SIMPLE(a, b) \
    BOOST_CHECK_EQUAL_COLLECTIONS(expected.begin(), expected.end(), result.begin(), result.end());

BOOST_AUTO_TEST_CASE(TestSplitByNUL) {
    //Test empty
    std::set<std::string> expected;
    std::set<std::string> result;
    splitByNUL(result, nullptr, 0);
    BOOST_CHECK_EQUAL_COLLECTIONS_SIMPLE(expected, result);
    result.clear();
    expected.clear();
    //Test simple
    expected = {"a", "b", "c"};
    splitByNUL(result, "a\0b\0c", 5);
    BOOST_CHECK_EQUAL_COLLECTIONS_SIMPLE(expected, result);
    result.clear();
    expected.clear();
    //Test with longer strings
    expected = {"ab", "bc", "def"};
    splitByNUL(result, "ab\0bc\0def", 9);
    BOOST_CHECK_EQUAL_COLLECTIONS_SIMPLE(expected, result);
    result.clear();
    expected.clear();
    //Test with empty last string
    expected = {"a", "b", ""};
    splitByNUL(result, "a\0b\0", 4);
    BOOST_CHECK_EQUAL_COLLECTIONS_SIMPLE(expected, result);
    result.clear();
    expected.clear();
    //Test with empty first string
    expected = {"", "b", "c"};
    splitByNUL(result, "\0b\0c", 4);
    BOOST_CHECK_EQUAL_COLLECTIONS_SIMPLE(expected, result);
    result.clear();
    expected.clear();
    //Test with duplicates
    expected = {"a", "b", "c"};
    splitByNUL(result, "a\0b\0b\0c\0a\0c", 11);
    BOOST_CHECK_EQUAL_COLLECTIONS_SIMPLE(expected, result);
    result.clear();
    expected.clear();
}

BOOST_AUTO_TEST_SUITE_END()
