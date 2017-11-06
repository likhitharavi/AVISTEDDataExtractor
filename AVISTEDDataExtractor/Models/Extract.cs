using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace AVISTEDDataExtractor.Models
{
    public class Extract
    {
        public string parameters { get; set; }
        public double latmin { get; set; }
        public double latmax { get; set; }
        public double lonmin { get; set; }
        public double lonmax { get; set; }
        public string stat { get; set; }
        public string path { get; set; }
        public string format { get; set; }
        public DateTime startDate { get; set; }
        public DateTime endDate { get; set; }
        public string outFormat { get; set; }
        public Boolean saveDownload { get; set; }
    }
}