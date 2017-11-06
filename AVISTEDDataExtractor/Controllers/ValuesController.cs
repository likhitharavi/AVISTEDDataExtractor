using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Microsoft.Research.Science.Data;
using MathNet.Numerics.Statistics;
using System.Globalization;
using Newtonsoft.Json;
using AVISTEDDataExtractor.Models;
using HDF5DotNet;
using GeoCoordinatePortable;
using NLog;

namespace AVISTEDDataExtractor.Controllers
{
    public class ValuesController : ApiController
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();      
        // GET api/values for the selected Dataset
        public List<Dictionary<string,string>> Get(string info)
        {
            logger.Log(LogLevel.Info, "Entered AVISTED GET()");
            Extract Datasetinfo = JsonConvert.DeserializeObject<Extract>(info) as Extract;
            List<Dictionary<string, string>> list = new List<Dictionary<string, string>>();
            switch (Datasetinfo.format) {
                case "NetCDF":
                    list = ExtractNetCDF(Datasetinfo);
                    break;
                case "HDF5":
                    list = ExtractHDF(Datasetinfo);
                    break;
                case "CSV":
                    list = ExtractCSV(Datasetinfo);
                    break;
                case "ASCII":
                    list = ExtractASCII(Datasetinfo);
                    break;
                default:
                    list = null;
                    break;

            } 
             
            return list;
        }

        //Extracts data from HDF5 datasets
        public List<Dictionary<string,string>> ExtractHDF(Extract DatasetInfo)
        {
            logger.Log(LogLevel.Info, "Entered AVISTED ExtractHDF()");
            //Get start date and end date
            DateTime start = Convert.ToDateTime(DatasetInfo.startDate);
            DateTime end = Convert.ToDateTime(DatasetInfo.endDate);


            //Get selected parameters
            string[] parameters = DatasetInfo.parameters.Split(',');

            //Get statistics           
            string group = "Grid";
            string dataset_name = "", stat = DatasetInfo.stat;
            float value = 0;
            //initialize variables
            Dictionary<string, string> dict = new Dictionary<string, string>();
            List<Dictionary<string, string>> list = new List<Dictionary<string, string>>();

            try
            {
                //Get the data set path
                String logDirectory = DatasetInfo.path;
                DirectoryInfo dir1 = new DirectoryInfo(logDirectory);
                FileInfo[] DispatchFiles = dir1.GetFiles("*.hdf");
                logger.Log(LogLevel.Info, "ExtractHDF:Successful Dispatch");

                if (DispatchFiles.Length > 0)
                {
                    foreach (FileInfo aFile in DispatchFiles)
                    {
                        string time = aFile.Name.Substring(20, 21).Split('-')[0];
                        DateTime datetime = DateTime.Now;

                        if (DateTime.TryParseExact(time, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out datetime))
                        {
                            int begin = DateTime.Compare(start.Date, datetime);
                            int last = DateTime.Compare(end.Date, datetime);
                            if (begin <= 0 && last >= 0)
                            {
                                string filePath = Path.Combine(dir1.FullName, aFile.Name);
                                dict.Add("date", datetime.ToString("yyyy-MM-dd"));

                                H5.Open();
                                H5FileId h5FileId = H5F.open(filePath, H5F.OpenMode.ACC_RDONLY);
                                float[] lat = h5FileId.Read1DArray<float>("/Grid/lat");
                                float[] lon = h5FileId.Read1DArray<float>("/Grid/lon");
                                logger.Log(LogLevel.Info, "ExtractHDF:Successful lat lon");
                                foreach (string s in parameters)
                                {
                                    dataset_name = "/" + group + "/" + s;
                                    float[,] values = h5FileId.Read2DArray<float>(dataset_name);
                                    //float[,] probliq = h5FileId.Read2DArray<float>("/Grid/probabilityLiquidPrecipitation");
                                    float[,] values_slice = values.Slice(0, 50, 300, 350);
                                    List<float> lst = values_slice.Cast<float>().ToList();
                                    List<double> doubleList = lst.ConvertAll(x => (double)x);
                                    var statistics = new DescriptiveStatistics(doubleList);
                                    // Order Statistics
                                    if (string.Compare(stat, "max") == 0)
                                    {
                                        var largestElement = statistics.Maximum;
                                        value = System.Convert.ToSingle(largestElement);
                                    }
                                    else if (string.Compare(stat, "min") == 0)
                                    {
                                        var smallestElement = statistics.Minimum;
                                        value = System.Convert.ToSingle(smallestElement);
                                    }
                                    else if (string.Compare(stat, "med") == 0)
                                    {
                                        var median = statistics.Mean;
                                        value = System.Convert.ToSingle(median);
                                    }
                                    else if (string.Compare(stat, "mean") == 0)
                                    {
                                        //Central Tendency
                                        var mean = statistics.Mean;
                                        value = System.Convert.ToSingle(mean);
                                    }
                                    else if (string.Compare(stat, "var") == 0)
                                    {// Dispersion
                                        var variance = statistics.Variance;
                                        value = System.Convert.ToSingle(variance);
                                    }
                                    else if (string.Compare(stat, "stD") == 0)
                                    {
                                        var stdDev = statistics.StandardDeviation;
                                        value = System.Convert.ToSingle(stdDev);
                                    }
                                    logger.Log(LogLevel.Info, "ExtractHDF:Successful extraction of {0}",s);
                                    dict.Add(s, value.ToString());
                                }
                                H5.Close();
                                list.Add(new Dictionary<string, string>(dict));
                                dict.Clear();

                            }
                        }
                    }
                }                
            }
            catch (Exception ex)
            {
                logger.Error("ExtractHDF:Failed with exception {0}", ex.Message);
            }
            return list;
        }
        public List<Dictionary<string,string>> ExtractASCII(Extract DatasetInfo)
        {
            logger.Log(LogLevel.Info, "Entered AVISTED ExtractASCII()");

            //Get selected parameters
            string parames = DatasetInfo.parameters + ",Year";
            string[] paramlist = parames.Split(',');
          
            //Get start and end date
            DateTime start = Convert.ToDateTime(DatasetInfo.startDate);
            DateTime end = Convert.ToDateTime(DatasetInfo.endDate);
            DateTime date;

            //initialize variables
            Dictionary<string, string> dict = new Dictionary<string, string>();
            List<Dictionary<string, string>> list = new List<Dictionary<string, string>>();
            Dictionary<string, string> dict1 = new Dictionary<string, string>();
            int index = 0;
            try
            {
                String logDirectory = DatasetInfo.path;
                DirectoryInfo dir1 = new DirectoryInfo(logDirectory);
                FileInfo[] DispatchFiles = dir1.GetFiles();
                //get Data
                if (DispatchFiles.Length > 0)
                {
                    foreach (FileInfo aFile in DispatchFiles)
                    {

                        string filePath = Path.Combine(dir1.FullName, aFile.Name);

                        //Pass the file path and file name to the StreamReader constructor
                        StreamReader sr = new StreamReader(filePath);
                        String line;

                        //Read the first line of text
                        line = sr.ReadLine();
                        line = String.Join(" ", line.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries));
                        line = line.Replace("Year", "date");
                        string[] parameters = line.Split(' ');
                        line = sr.ReadLine();

                        //Continue to read until you reach end of file
                        while (line != null)
                        {

                            line = String.Join(" ", line.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries));
                            string[] values = line.Split(' ');
                            if (DateTime.TryParseExact(values[0] + "-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                            {
                                if (start.Year <= date.Year && end.Year >= date.Year)
                                {
                                    foreach (string s in parameters)
                                    {
                                        if (s.Equals("date"))
                                        {
                                            var month = values[1];
                                            if (month.Length == 1)
                                            {
                                                month = "0" + month;
                                            }
                                            dict.Add(s, values[0] + "-" + month + "-01");
                                        }


                                        if (paramlist.Contains(s))
                                        {
                                            index = Array.IndexOf(parameters, s);
                                            dict.Add(s, values[index]);
                                        }
                                    }
                                    list.Add(new Dictionary<string, string>(dict));
                                }

                                dict.Clear();
                                line = sr.ReadLine();
                            }

                        }
                        sr.Close();
                    }
                }
            }
            catch(Exception ex)
            {
                logger.Error("ExtractASCII:Failed with exception {0}", ex.Message);
            }
            return list;
        }

        public List<Dictionary<string,string>> ExtractCSV(Extract Datasetinfo)
        {
            logger.Log(LogLevel.Info, "Entered AVISTED ExtractCSV()");

           
            //Get selected parameters
            string parameters = Datasetinfo.parameters + ",YYYY-MM-DD";
            string[] paramlist = parameters.Split(',');
            
            //Get start and end date
            DateTime start = Convert.ToDateTime(Datasetinfo.startDate);
            DateTime end = Convert.ToDateTime(Datasetinfo.endDate);

            //Get statistics
            string stat = Datasetinfo.stat; 

            //initialize variables
            Dictionary<string, string> dict = new Dictionary<string, string>();
            List<Dictionary<string, string>> list = new List<Dictionary<string, string>>();
            Dictionary<string, string> dict1 = new Dictionary<string, string>();
            float value = 0;
            string[] strdate;
            try
            {
                //Get the Dataset Path
                String logDirectory = Datasetinfo.path;
                DirectoryInfo dir1 = new DirectoryInfo(logDirectory);
                FileInfo[] DispatchFiles = dir1.GetFiles();
                //get Data
                if (DispatchFiles.Length > 0)
                {
                    foreach (FileInfo aFile in DispatchFiles)
                    {
                        string time = aFile.Name.Split('_')[1] + "-01-01";
                        DateTime datetime = DateTime.Now;

                        if (DateTime.TryParseExact(time, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out datetime))
                        {

                            //Check if the file is with the selected date range
                            if (start.Year <= datetime.Year && end.Year >= datetime.Year)
                            {
                                string filePath = Path.Combine(dir1.FullName, aFile.Name);

                                using (DataSet ds1 = DataSet.Open(filePath))
                                {
                                    //get data for selected variables
                                    foreach (Variable v in ds1.Variables)
                                    {
                                        if (paramlist.Contains(v.Name))
                                        {

                                            if (v.Name.Equals("YYYY-MM-DD"))
                                            {
                                                DateTime[] dt2 = (DateTime[])v.GetData();
                                                strdate = new string[dt2.Length];
                                                int k = 0;
                                                foreach (DateTime dt in dt2)
                                                {
                                                    strdate[k++] = dt.ToString("yyyy-MM-dd");

                                                }
                                                if (dict.ContainsKey("date"))
                                                {
                                                    dict["date"] = dict["date"] + "," + string.Join(",", strdate);
                                                }
                                                else
                                                {
                                                    dict.Add("date", string.Join(",", strdate));
                                                }
                                            }
                                            else
                                            {
                                                double[] x = (double[])v.GetData();
                                                if (dict.ContainsKey(v.Name))
                                                {
                                                    dict[v.Name] = dict[v.Name] + "," + string.Join(",", x);
                                                }
                                                else
                                                {
                                                    dict.Add(v.Name, string.Join(",", x));
                                                }
                                            }

                                        }
                                    }


                                }



                            }
                        }
                    }
                }


                int rows = dict.Values.First().Split(',').Length;
                for (int i = 0; i < rows; i++)
                {
                    foreach (string key in dict.Keys)
                    {
                        dict1.Add(key, dict[key].Split(',')[i]);
                    }
                    list.Add(new Dictionary<string, string>(dict1));
                    dict1.Clear();
                }
            }
            catch(Exception ex)
            {
                logger.Error("ExtractCSV:Failed with exception {0}", ex.Message);
            }
            return list;
        }
       

        //Extract data from NetCDF files
        public List<Dictionary<string,string>> ExtractNetCDF(Extract Datasetinfo)
        {
            logger.Log(LogLevel.Info, "Entered AVISTED ExtractNetCDF()");

            //Get selected parameters
            string[] paramlist = Datasetinfo.parameters.Split(',');
          
            //Get latitude and longitude
            int latmin = 0, latmax = 10, lonmin = 0, lonmax = 10;
           
            //Get start and end date
            DateTime start = Convert.ToDateTime(Datasetinfo.startDate);
            DateTime end = Convert.ToDateTime(Datasetinfo.endDate);

            //Get statistics
            string stat = Datasetinfo.stat;
            
            //initialize variables
            Dictionary<string, string> dict = new Dictionary<string, string>();
            List<Dictionary<string, string>> list = new List<Dictionary<string, string>>();
            float value = 0;
            try
            {
                //Get the Dataset Path
                String logDirectory = Datasetinfo.path;
                DirectoryInfo dir1 = new DirectoryInfo(logDirectory);
                FileInfo[] DispatchFiles = dir1.GetFiles();

                //finding the closest point to the user selection
                string filePathlatlong = Path.Combine(dir1.FullName, "hourly_output_d01_lat.lon.elev.land.nc");
                DataSet ds2 = DataSet.Open(filePathlatlong);
                ReadOnlyDimensionList dimension = ds2.Dimensions;
                float[,] latitude = new float[dimension[0].Length, dimension[1].Length];
                float[,] longitude = new float[dimension[0].Length, dimension[1].Length];
                foreach (Variable v in ds2.Variables)
                {
                    if (v.Name.Equals("latitude"))
                    {
                        latitude = (float[,])v.GetData();
                    }
                    if (v.Name.Equals("longitude"))
                    {
                        longitude = (float[,])v.GetData();
                    }
                }

                //getting the latitude longitude into the array
                GeoCoordinate[] coord = new GeoCoordinate[dimension[0].Length * dimension[1].Length];
                int n = 0;
                for (int l = 0; l < dimension[0].Length; l++)
                {
                    for (int m = 0; m < dimension[1].Length; m++)
                    {

                        coord[n] = new GeoCoordinate(latitude[l, m], longitude[l, m]);
                        n++;

                    }
                }

                //closest point to the user selection 
                var coords = new GeoCoordinate(Datasetinfo.latmin, Datasetinfo.lonmin);
                var nearest = coord.Select(x => new GeoCoordinate(x.Latitude, x.Longitude))
                           .OrderBy(x => x.GetDistanceTo(coords))
                           .First();
                int found = 0;
                for (int l = 0; l < dimension[0].Length; l++)
                {
                    for (int m = 0; m < dimension[1].Length; m++)
                    {
                        if (latitude[l, m] == nearest.Latitude && longitude[l, m] == nearest.Longitude)
                        {
                            found = 1;
                            latmin = l;
                            lonmin = m;
                            break;
                        }

                    }
                    if (found == 1)
                        break;
                }

                //get Data from NetCDF files
                if (DispatchFiles.Length > 0)
                {
                    foreach (FileInfo aFile in DispatchFiles)
                    {
                        //Getting date from file name
                        string dtime = aFile.Name.Substring(18, 10);
                        DateTime datetime = DateTime.Now;

                        if (DateTime.TryParseExact(dtime, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out datetime))
                        {
                            int begin = DateTime.Compare(start.Date, datetime);
                            int last = DateTime.Compare(end.Date, datetime);

                            //check if the date is within the user requested range
                            if (begin <= 0 && last >= 0)
                            {
                                string filePath = Path.Combine(dir1.FullName, aFile.Name);
                                dict.Add("date", dtime.ToString());

                                //open dataset
                                using (DataSet ds1 = DataSet.Open(filePath))
                                {
                                    //for each variable if selected by the user get the values
                                    foreach (Variable v in ds1.Variables)
                                    {
                                        if (Datasetinfo.parameters.Contains(v.Name))
                                        {
                                            float[,] subset = (float[,])v.GetData(new int[] { latmin, lonmin }, new int[] { latmax, lonmax });
                                            List<float> lst = subset.Cast<float>().ToList();
                                            List<double> doubleList = lst.ConvertAll(x => (double)x);

                                            //Apply the selected statistics to the list of values
                                            var statistics = new DescriptiveStatistics(doubleList);
                                            // Order Statistics
                                            if (string.Compare(stat, "max") == 0)
                                            {
                                                var largestElement = statistics.Maximum;
                                                value = System.Convert.ToSingle(largestElement);
                                            }
                                            else if (string.Compare(stat, "min") == 0)
                                            {
                                                var smallestElement = statistics.Minimum;
                                                value = System.Convert.ToSingle(smallestElement);
                                            }
                                            else if (string.Compare(stat, "med") == 0)
                                            {
                                                var median = statistics.Mean;
                                                value = System.Convert.ToSingle(median);
                                            }
                                            else if (string.Compare(stat, "mean") == 0)
                                            {//Central Tendency
                                                var mean = statistics.Mean;
                                                value = System.Convert.ToSingle(mean);
                                            }
                                            else if (string.Compare(stat, "var") == 0)
                                            {// Dispersion
                                                var variance = statistics.Variance;
                                                value = System.Convert.ToSingle(variance);
                                            }
                                            else if (string.Compare(stat, "stD") == 0)
                                            {
                                                var stdDev = statistics.StandardDeviation;
                                                value = System.Convert.ToSingle(stdDev);
                                            }

                                            dict.Add(v.Name, value.ToString());

                                        }
                                    }

                                    list.Add(new Dictionary<string, string>(dict));
                                }

                                dict.Clear();

                            }
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                logger.Error("ExtractNetCDF:Failed with exception {0}", ex.Message);
            }
            return list;
        }
       
        public char[,] StringToChar(char[,] charArray, string[] rows)
        {
            for (int i = 0; i < rows.Length; i++)
                for (int j = 0; j < rows[i].Length; j++)
                {
                    charArray[i, j] = rows[i][j];
                }
            return charArray;
        }
       


        // GET api/values/5
        public string Get(int id)
        {
            return "value";
        }


        // DELETE api/values/5
        public void Delete(int id)
        {
        }

    }



    public static class HdfExtensions
    {

        public static IEnumerable<String> SplitInParts(this String s, Int32 partLength)
        {
            if (s == null)
                throw new ArgumentNullException("s");
            if (partLength <= 0)
                throw new ArgumentException("Part length has to be positive.", "partLength");

            for (var i = 0; i < s.Length; i += partLength)
                yield return s.Substring(i, Math.Min(partLength, s.Length - i));
        }

        public static T[] Read1DArray<T>(this H5FileId fileId, string dataSetName)
        {
            var dataset = H5D.open(fileId, dataSetName);
            var space = H5D.getSpace(dataset);
            var dims = H5S.getSimpleExtentDims(space);
            var dataType = H5D.getType(dataset);
            if (typeof(T) == typeof(string))
            {
                int stringLength = H5T.getSize(dataType);
                byte[] buffer = new byte[dims[0] * stringLength];
                H5D.read(dataset, dataType, new H5Array<byte>(buffer));
                string stuff = System.Text.ASCIIEncoding.ASCII.GetString(buffer);
                return stuff.SplitInParts(stringLength).Select(ss => (T)(object)ss).ToArray();
            }
            T[] dataArray = new T[dims[0]];
            var wrapArray = new H5Array<T>(dataArray);
            H5D.read(dataset, dataType, wrapArray);
            return dataArray;
        }

        public static T[,] Read2DArray<T>(this H5FileId fileId, string dataSetName)
        {
            var dataset = H5D.open(fileId, dataSetName);
            var space = H5D.getSpace(dataset);
            var dims = H5S.getSimpleExtentDims(space);
            var dataType = H5D.getType(dataset);
            if (typeof(T) == typeof(string))
            {
                // this will also need a string hack...
            }
            T[,] dataArray = new T[dims[0], dims[1]];
            var wrapArray = new H5Array<T>(dataArray);
            H5D.read(dataset, dataType, wrapArray);
            return dataArray;
        }
    }


    public static class Ext
    {
        public static T[] Slice<T>(this T[] source, int fromIdx, int toIdx)
        {
            T[] ret = new T[toIdx - fromIdx + 1];
            for (int srcIdx = fromIdx, dstIdx = 0; srcIdx <= toIdx; srcIdx++)
            {
                ret[dstIdx++] = source[srcIdx];
            }
            return ret;
        }
        public static T[,] Slice<T>(this T[,] source, int fromIdxRank0, int toIdxRank0, int fromIdxRank1, int toIdxRank1)
        {
            T[,] ret = new T[toIdxRank0 - fromIdxRank0 + 1, toIdxRank1 - fromIdxRank1 + 1];

            for (int srcIdxRank0 = fromIdxRank0, dstIdxRank0 = 0; srcIdxRank0 <= toIdxRank0; srcIdxRank0++, dstIdxRank0++)
            {
                for (int srcIdxRank1 = fromIdxRank1, dstIdxRank1 = 0; srcIdxRank1 <= toIdxRank1; srcIdxRank1++, dstIdxRank1++)
                {
                    ret[dstIdxRank0, dstIdxRank1] = source[srcIdxRank0, srcIdxRank1];
                }
            }
            return ret;
        }


    }
    
}
