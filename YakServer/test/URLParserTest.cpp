#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include "http/URLParser.hpp"
#include <iostream>
using namespace std;

BOOST_AUTO_TEST_SUITE(URLParser)

BOOST_AUTO_TEST_CASE(TestHexToChar) {
    BOOST_CHECK_EQUAL(' ', hexToChar('2','0'));
    BOOST_CHECK_EQUAL('\0', hexToChar('0','0'));
    BOOST_CHECK_EQUAL('A', hexToChar('4','1'));
}

BOOST_AUTO_TEST_CASE(TestDecodeURLEntities) {
    string out;
    BOOST_CHECK(decodeURLEntities("a%20b%41cDEF%67",out));
    BOOST_CHECK_EQUAL("a bAcDEFg", out);
    BOOST_CHECK(decodeURLEntities("",out));
    BOOST_CHECK_EQUAL("", out);
    BOOST_CHECK(decodeURLEntities("/",out));
    BOOST_CHECK_EQUAL("/", out);
}


BOOST_AUTO_TEST_CASE(TestParseQueryPart) {
    map<string, string> result;
    parseQueryPart("?a=b&c=d&Foo=bar", result);
    BOOST_CHECK_EQUAL(3, result.size());
    BOOST_CHECK_EQUAL("b", result["a"]);
    BOOST_CHECK_EQUAL("d", result["c"]);
    BOOST_CHECK_EQUAL("bar", result["Foo"]);
    //Test with encoded components
    result.clear();
    parseQueryPart("?a=%20b&c%48=d&Foo=b%70ar", result);
    BOOST_CHECK_EQUAL(3, result.size());
    BOOST_CHECK_EQUAL(" b", result["a"]);
    BOOST_CHECK_EQUAL("d", result["cH"]);
    BOOST_CHECK_EQUAL("bpar", result["Foo"]);
}

BOOST_AUTO_TEST_SUITE_END()