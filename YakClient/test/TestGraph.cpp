#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <map>
#include <iostream>
#include <string>
#include <cstring>
#include "Graph/Serialize.hpp"

using namespace std;

BOOST_AUTO_TEST_SUITE(Graph)

BOOST_AUTO_TEST_CASE(TestBasicAttributeSerialization) {
    map<string, string> myMap;
    myMap["key1"] = "value1";
    myMap["mykey2"] = "mv2";
    myMap["k3"] = "myvalue3";
    const char expected[] = "k3\x00myvalue3\x00key1\x00value1\x00mykey2\x00mv2\x00";
    size_t actualSize;
    char* actual = serializeBasicAttributes(myMap, actualSize);
    string str(actual, actualSize);
    //For debugging. Replace NUL by 'X'.
    /*for(int i = 0; i < str.size(); i++) {
        if(str.data()[i] == 0) {((char*)str.data())[i] = 'X';}
    }
    cout << str << endl;*/
    BOOST_CHECK_EQUAL(actualSize, sizeof(expected) - 1);
    BOOST_CHECK(memcmp(expected, actual, actualSize) == 0);
}


BOOST_AUTO_TEST_SUITE_END()