using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StarGroupByJoin
{
    class SGBJoin
    {
        // Dimensions are represented by Di where i = 1...n
        // Each columns of dimension are represented by Cj where j = 1..m
        #region Initialisations
        static int noOfCustomerDimRows = 30001;
        static List<int> customerKey = new List<int>(noOfCustomerDimRows);
        static List<string> customerRegion = new List<string>(noOfCustomerDimRows);
        static List<string> customerNation = new List<string>(noOfCustomerDimRows);

        static int noOfSupplierDimRows = 3001;
        static List<int> supplierKey = new List<int>(noOfSupplierDimRows);
        static List<string> supplierRegion = new List<string>(noOfSupplierDimRows);
        static List<string> supplierNation = new List<string>(noOfSupplierDimRows);

        static int noOfDateDimRows = 4001;
        static List<DateTime> dateKey = new List<DateTime>(noOfDateDimRows);
        static List<int> dateYear = new List<int>(noOfDateDimRows);

        static int factRows = 600001;
        static List<int> loCustomerKey = new List<int>(factRows);
        static List<int> loSupplierKey = new List<int>(factRows);
        static List<DateTime> loOrderDate = new List<DateTime>(factRows);
        static List<float> loRevenue = new List<float>(factRows);

        static BitArray customerBitMap = new BitArray(noOfCustomerDimRows);
        static BitArray supplierBitMap = new BitArray(noOfSupplierDimRows);
        static BitArray dateBitMap = new BitArray(noOfDateDimRows);

        static Dictionary<string, HashSet<int>> customerDictionary = new Dictionary<string, HashSet<int>>();
        static Dictionary<string, HashSet<int>> supplierDictionary = new Dictionary<string, HashSet<int>>();
        static Dictionary<int, HashSet<int>> dateDictionary = new Dictionary<int, HashSet<int>>();

        #endregion

        static void Main()
        {
            DataGeneration();
            Query3_1();
            Console.ReadKey();
        }


        #region Data Generation
        private static void DataGeneration()
        {
            List<string> custRegion = new List<string>() { "Asia", "Europe", "Asia", "Asia" };
            customerRegion.AddRange(custRegion);
            List<string> custNation = new List<string>() { "China", "France", "India", "China" };
            customerNation.AddRange(custNation);

            List<string> suppRegion = new List<string>() { "Asia", "Europe", "Asia" };
            supplierRegion.AddRange(suppRegion);
            List<string> suppNation = new List<string>() { "Russia", "Spain", "China" };
            supplierNation.AddRange(suppNation);

            List<DateTime> dKey = new List<DateTime>() { new DateTime(1997, 01, 01), new DateTime(1997, 02, 01), new DateTime(1997, 03, 01) };
            dateKey.AddRange(dKey);
            List<int> year = new List<int>() { 1997, 1997, 1997 };
            dateYear.AddRange(year);

            List<int> custKey = new List<int>() { 3, 3, 2, 1, 2, 1, 3 };
            loCustomerKey.AddRange(custKey);
            List<int> suppKey = new List<int>() { 1, 2, 1, 1, 2, 2, 2 };
            loSupplierKey.AddRange(suppKey);

            List<DateTime> orderDates = new List<DateTime>() {
                new DateTime(1997, 01, 01),
                new DateTime(1997, 01, 01),
                new DateTime(1997, 02, 01),
                new DateTime(1997, 02, 01),
                new DateTime(1997, 02, 01),
                new DateTime(1997, 03, 01),
                new DateTime(1997, 03, 01)};
            loOrderDate.AddRange(orderDates);

            List<float> revenue = new List<float>() { 43256, 33333, 12121, 23233, 45456, 43251, 34325 };
            loRevenue.AddRange(revenue);

        }
        #endregion

        #region Query 3.1
        private static void Query3_1()
        {
            // Step 1
            // Apply region = 'Asia' on Customer Dimension
            int customerIndex = 0;
            foreach (var region in customerRegion)
            {
                if (region.Equals("Asia"))
                {
                    customerBitMap.Set(customerIndex, true);
                }
                customerIndex++;
            }


            // Apply region = ‘Asia’ on Supplier Dimension
            int supplierIndex = 0;
            foreach (var region in supplierRegion)
            {
                if (region.Equals("Asia"))
                {
                    supplierBitMap.Set(supplierIndex, true);
                }
                supplierIndex++;
            }

            // Apply year in [1992, 1997] on Date Dimension
            int dateIndex = 0;
            Dictionary<DateTime, int> dateDimHashTable = new Dictionary<DateTime, int>();
            foreach (var year in dateYear)
            {
                if (year == 1992 || year == 1997)
                {
                    dateDimHashTable.Add(dateKey[dateIndex], year);
                }
                dateIndex++;
            }

            // End Step 1

            // Step 2

            // Customer Dictionary with GroupIDs
            customerIndex = 0;
            foreach (var custKey in loCustomerKey)
            {
                if (customerBitMap[custKey - 1])
                {
                    string custNation = customerNation[custKey - 1];
                    HashSet<int> value = new HashSet<int>();
                    if (customerDictionary.TryGetValue(custNation, out value))
                    {
                        value.Add(customerIndex + 1);
                        customerDictionary[custNation] = value;
                    }
                    else
                    {
                        HashSet<int> value1 = new HashSet<int>();
                        value1.Add(customerIndex + 1);
                        customerDictionary.Add(custNation, value1);
                    }
                }
                customerIndex++;
            }

            // Supplier Dictionary with GroupIDs
            supplierIndex = 0;
            foreach (var suppKey in loSupplierKey)
            {
                if (supplierBitMap[suppKey - 1])
                {
                    string suppNation = supplierNation[suppKey - 1];
                    HashSet<int> value = new HashSet<int>();
                    if (supplierDictionary.TryGetValue(suppNation, out value))
                    {
                        value.Add(supplierIndex + 1);
                        supplierDictionary[suppNation] = value;
                    }
                    else
                    {
                        HashSet<int> value1 = new HashSet<int>();
                        value1.Add(supplierIndex + 1);
                        supplierDictionary.Add(suppNation, value1);
                    }
                }
                supplierIndex++;
            }

            // Date Dictionary with GroupIDs
            dateIndex = 0;
            foreach (var orderDate in loOrderDate)
            {
                int year = 0;
                if(dateDimHashTable.TryGetValue(orderDate, out year))
                {
                    HashSet<int> value = new HashSet<int>();
                    if (dateDictionary.TryGetValue(year, out value))
                    {
                        value.Add(dateIndex + 1);
                        dateDictionary[year] = value;
                    }
                    else
                    {
                        HashSet<int> value1 = new HashSet<int>();
                        value1.Add(dateIndex + 1);
                        dateDictionary.Add(year, value1);
                    }
                }
                dateIndex++;
            }
            // End Step 2

            // Step 3
            List<Tuple<string, string, int, float>> results = new List<Tuple<string, string, int, float>>(100);
            foreach (var custKeyValue in customerDictionary)
            {
                foreach (var suppKeyValue in supplierDictionary)
                {
                    foreach (var dateKeyValue in dateDictionary)
                    {
                        var intersectedResult = custKeyValue.Value.Intersect(suppKeyValue.Value).Intersect(dateKeyValue.Value);
                        if (intersectedResult.Count() > 0)
                        {
                            float sum = 0;
                            foreach (var index in intersectedResult)
                            {
                                sum = sum + loRevenue[index - 1];
                            }
                            results.Add(Tuple.Create(custKeyValue.Key, suppKeyValue.Key, dateKeyValue.Key, sum));
                        }
                    }
                }
            }
            // End Step 3

            // Display the result
            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }
        #endregion

    }
}
