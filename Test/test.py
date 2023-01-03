from Code._utils_code_py import *
import unittest

class PysparkUnittest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
        cls.spark = spark

    def test_joining_table(self):
        print("inside test count")
        df1 = trasactiondf(self.spark)
        df2 = userdf(self.spark)

        df_res=joining_trans_user(df1, df2)
        count = countDistinctLocation(df_res, "location ")
        count1=count.collect()[0][0]
        print("Exit of Testcase")
        self.assertEqual(count1, 3)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()