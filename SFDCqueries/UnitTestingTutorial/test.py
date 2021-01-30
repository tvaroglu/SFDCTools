import unittest
from my_sum import sum

#target = __import__("my_sum.py")#
#sum = target.sum#


class TestSum(unittest.TestCase):

    def test_list_int(self):
        """
        Test that it can sum a list of integers
        """
        data = [1, 2, 3]
        result = sum(data)
        self.assertEqual(result, 6)

    def test_sum(self):
        self.assertEqual(sum([1, 2, 3]), 6, "Should be 6")

    def test_sum_tuple(self):
        self.assertEqual(sum((1, 2, 2)), 6, "Should be 6")

if __name__ == '__main__':
    unittest.main()


# def test_sum():
#     assert sum([1, 2, 3]) == 6, "Should be 6"

# def test_sum_tuple():
#     assert sum((1, 2, 2)) == 6, "Should be 6"

# if __name__ == "__main__":
#     test_sum()
#     test_sum_tuple()
#     print("Everything passed")