#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <map>
#include <iostream>
#include <string>
#include <cstring>
#include "yakclient/Graph/Serialize.hpp"

using namespace std;

BOOST_AUTO_TEST_SUITE(Graph)

BOOST_AUTO_TEST_CASE(TestBasicAttributeSerialization) {
    map<string, string> myMap;
    myMap["key1"] = "value1";
    myMap["mykey2"] = "mv2";
    myMap["k3"] = "myvalue3";
    const char expected[] = "k3\x1Fmyvalue3\x1Ekey1\x1Fvalue1\x1Emykey2\x1Fmv2\x1E";
    size_t actualSize;
    char* actual = serializeBasicAttributes(myMap, &actualSize);
    //For debugging. Replace NUL by 'X'.
    /*
    string str(actual, actualSize);
    for(int i = 0; i < str.size(); i++) {
        if(str.data()[i] == 0) {((char*)str.data())[i] = 'X';}
    }
    cout << str << endl;*/
    BOOST_CHECK_EQUAL(actualSize, sizeof(expected) - 1);
    BOOST_CHECK(memcmp(expected, actual, actualSize) == 0);
    delete[] actual;
}

BOOST_AUTO_TEST_CASE(TestOneKeyBasicAttributeSerialization) {
    const char expected[] = "k3\x1Fmyvalue3\x1E";
    size_t actualSize;
    char* actual = serializeBasicAttributes("k3", "myvalue3", &actualSize);
    BOOST_CHECK_EQUAL(actualSize, sizeof(expected) - 1);
    BOOST_CHECK(memcmp(expected, actual, actualSize) == 0);
    delete[] actual;
}

//Tests the extattr key serialization
BOOST_AUTO_TEST_CASE(TestExtendedAttributeSerialization) {
    const char expected[] = "myEntityId\x1Dthekey";
    size_t actualSize;
    char* actual = serializeExtAttrId("myEntityId", "thekey", &actualSize);
    BOOST_CHECK_EQUAL(actualSize, sizeof(expected) - 1);
    BOOST_CHECK(memcmp(expected, actual, actualSize) == 0);
    delete[] actual;
}


BOOST_AUTO_TEST_CASE(TestEdgeSerialization) {
    const unsigned char expectedFwd[] = "etype" "\x1F" "firstNode" "\x0E" "secondNode";
    const unsigned char expectedBwd[] = "etype" "\x1F" "secondNode" "\x0F" "firstNode";
    BOOST_CHECK_EQUAL(sizeof(expectedFwd), sizeof(expectedBwd)); //selfcheck
    size_t actualSize;
    char* actualFwd;
    char* actualBwd;
    serializeEdgeId("firstNode","secondNode","etype", &actualSize, &actualFwd, &actualBwd);
    //For debugging. Replace \x1F by 'X', \x0E by 'Y' and \x0F by 'Z'
    /*std::string str(actualFwd, actualSize);
    for(int i = 0; i < str.size(); i++) {
        if(str.data()[i] == '\x1F') {((char*)str.data())[i] = 'X';}
        if(str.data()[i] == '\x0E') {((char*)str.data())[i] = 'Y';}
        if(str.data()[i] == '\x0F') {((char*)str.data())[i] = 'Z';}
    }
    cout << str << endl;*/
    //cout <<  << endl;
    BOOST_CHECK_EQUAL(actualSize, sizeof(expectedFwd) - 1);
    BOOST_CHECK(memcmp(expectedFwd, actualFwd, actualSize) == 0);
    BOOST_CHECK(memcmp(expectedBwd, actualBwd, actualSize) == 0);
    delete[] actualFwd;
    delete[] actualBwd;
}

BOOST_AUTO_TEST_SUITE_END()
