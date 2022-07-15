using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Companyw
    {
        public Int64 mid { get; set; }
        public String code { get; set; }
        public Boolean visible { get; set; }

    }

    public class Oddw
    {
        //public Int64 sid { get; set; }
        public Int64 sid { get; set; }
        public Int64 psid { get; set; }
        public Double odds { get; set; }
        public String otype { get; set; }
        public String oname { get; set; }
        public Double tno { get; set; }
        public Double size { get; set; }
    }

    public class Sectionw
    {
        public Int64 mid { get; set; }
        public Int64 sid { get; set; }
        public Int64 psid { get; set; }
        public Int32 sno { get; set; }
        public Int32 psrno { get; set; }
        public String gstatus { get; set; }
        public String nat { get; set; }
        public Int32 gscode { get; set; }
        public Double max { get; set; }
        public Double min { get; set; }
        public String rem { get; set; } = "";
        public Boolean br { get; set; }
        public String rname { get; set; }
        public String jname { get; set; }
        public String tname { get; set; }
        public Int64 hage { get; set; }
        public String himg { get; set; }
        public Double adfa { get; set; }
        public String rdt { get; set; }
        public List<Oddw> odds { get; set; }

    }

    public class GameDataw
    {
        public Int64 gmid { get; set; }
        public Int64 mid { get; set; }
        public String pmid { get; set; }
        public String mname { get; set; } = "";
        public String rem { get; set; } = "";
        public String gtype { get; set; }
        public String status { get; set; }
        public Int32 rc { get; set; }
        public Boolean visible { get; set; }
        public Int32 pid { get; set; }
        public Int32 gscode { get; set; }
        public Double maxb { get; set; }
        public Double sno { get; set; }
        public Int32 dtype { get; set; }
        public Int32 ocnt { get; set; }
        public Int32 m { get; set; }
        public Double max { get; set; }
        public Double min { get; set; }
        public Boolean biplay { get; set; }
        public Double umaxbof { get; set; }
        public Boolean boplay { get; set; }
        public Boolean iplay { get; set; }
        public Int64 btcnt { get; set; }
        public List<Companyw> company { get; set; }
        public List<Sectionw> section { get; set; }
    }
}
