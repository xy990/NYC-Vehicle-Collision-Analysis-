'''
Created on Dec 16, 2016

@author: twff
'''
from geomap import *
from overallplot import *
import unittest
from Date import *
from correlation import correlation

weather = pd.read_csv("weather.csv")
data = pd.read_csv("clean_data.csv")
dayplot = pd.read_csv("dayplot.csv")
geomap = pd.read_csv('geomap.csv')

class testClass(unittest.TestCase):
    def testSetup(self):
        pass
    
    def testClass(self):
        correlation(rangen=10, label='da')
        correlation(rangen=5, label='A')
        correlation(rangen=5)
        correlation(label='A')
        correlation()
        
    def testEq(self):
        corr = correlation(rangen=10)
        self.assertEqual(corr.rangen, corr.rangen)
        self.assertEqual(len(corr.label), 4)
    
    def testinit(self):
        corr = correlation(rangen=5, label='A')
        self.assertEqual(corr.rangen, 5)
        self.assertEqual(corr.label, 'A')
        
    
    def test_check_zipcode(self):
        self.assertEqual(check_zipcode(10001),True)
        self.assertEqual(check_zipcode(11216),True)
        self.assertEqual(check_zipcode(1), False)
        self.assertEqual(check_zipcode(12), False)
        self.assertEqual(check_zipcode(123),False)
        self.assertEqual(check_zipcode(2344), False)
        self.assertEqual(check_zipcode(23461),False)
        self.assertEqual(check_zipcode(80000),False)
        self.assertEqual(check_zipcode(100000),False)
        self.assertEqual(check_zipcode([10001]),False)
        self.assertEqual(check_zipcode('10001'),False)
        self.assertEqual(check_zipcode(None),False)
        self.assertEqual(check_zipcode('qqlsd'),False)
    
    def testMerge(self):
        corr = correlation(rangen = 13)
        self.assertEqual(len(weather), len(corr.weather_collision_merge(weather, data)))
    
    def test_turn(self):
        self.assertEqual(len(turn_day_to_int('01/01/2016')), 8)
        self.assertEqual(len(turn_date_to_int('01/01/2016')),6)
        self.assertEqual(turn_day_to_int('01/01/2016'), '20160101')
        self.assertEqual(turn_date_to_int('01/01/2016'), '201601')
        self.assertEqual(turn_date_to_int('01/01/2016'), turn_date_to_int('01/02/2016'))
    

    def testData(self):
        self.assertEqual(len(weather), 366)
        self.assertEqual(len(data), 366)
        self.assertEqual(len(dayplot), 245164)

        
    def testDate(self):
        with self.assertRaises(ValueError):
            check('6/10/2016')
        with self.assertRaises(ValueError):
            check('07/16/2017')
        with self.assertRaises(ValueError):
            check('01/10/2015')
        with self.assertRaises(ValueError):
            check('02/30/2016')
        with self.assertRaises(ValueError):
            check('-1/10/2015')
        with self.assertRaises(ValueError):
            check('a/b/c')
        with self.assertRaises(ValueError):
            check('123456')
        with self.assertRaises(ValueError):
            check('13/2015')
        with self.assertRaises(ValueError):
            check('12/32/2015')
            
    def testWeatherOutput(self):
        output = str(weatheroutput(weather, date='01/01/2016'))
        length = len(output)
        self.assertEqual(length, 4)
        self.assertEqual(output, 'None')
        
    
if __name__ == '__main__':
    unittest.main()
