#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <iostream>
using namespace std;

/**
 * This suite tests the low-level update-request-delete facility.
 * Currently, a server is expected to be started in the background.
 */
BOOST_AUTO_TEST_SUITE(DBConsistency)

BOOST_AUTO_TEST_CASE(TestWriteReadDelete) {
    //Connect to the database at localhost
    
}

BOOST_AUTO_TEST_SUITE_END()