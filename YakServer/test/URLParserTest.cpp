#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include 
#include <iostream>
using namespace std;

BOOST_AUTO_TEST_SUITE(URLParser)

BOOST_AUTO_TEST_CASE(TestHexToChar) {
    BOOST_CHECK_EQUAL(' ', hexToChar('2','0'));
    BOOST_CHECK_EQUAL('\0', hexToChar('0','0'));
}

BOOST_AUTO_TEST_SUITE_END()