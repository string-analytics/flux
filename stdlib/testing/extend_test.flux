package testing_test


import "testing"
import "array"

testcase test_option_extension extends "flux/testing/testing_test.test_option" {
    // Set irrelevant option before the test
    // to ensure we can without causing an error
    option before = 1

    testing.do()

    // Set irrelevant option after the test
    // to ensure we can without causing an error
    option after = 1
}
