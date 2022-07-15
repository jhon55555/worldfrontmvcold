using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public class Oddz
    {
        public Double odds { get; set; }
        public String otype { get; set; }
        public String oname { get; set; }
        public Double tno { get; set; }
        public Double size { get; set; }
    }

    public class Sectiona
    {
        public Int64 sid { get; set; }
        public Int32 sno { get; set; }
        public String gstatus { get; set; }
        public Int32 gscode { get; set; }
        public String nat { get; set; }
        public List<Oddz> odds { get; set; }
    }

    public class T11
    {
        public Int64 gmid { get; set; }
        public String ename { get; set; }
        public Int32 etid { get; set; }
        public Int64 cid { get; set; }
        public Boolean iplay { get; set; }
        public String stime { get; set; }
        public Boolean tv { get; set; }
        public Boolean bm { get; set; }
        public Boolean f { get; set; }
        public Boolean f1 { get; set; }
        public Int64 mid { get; set; }
        public String mname { get; set; }
        public String status { get; set; }
        public Int32 rc { get; set; }
        public Int32 gscode { get; set; }
        public Int32 m { get; set; }
        public String gtype { get; set; }
        public Int32 iscc { get; set; }
        public List<Sectiona> section { get; set; }
    }

    public class Odd2z
    {
        public Double odds { get; set; }
        public String oname { get; set; }
        public String otype { get; set; }
        public Int64 sid { get; set; }
        public Double tno { get; set; }
        public Double size { get; set; }
    }

    public class Section2
    {
        public Int64 sid { get; set; }
        public String gstatus { get; set; }
        public String nat { get; set; }
        public Int32 sno { get; set; }
        public Int32 gscode { get; set; }
        public List<Odd2z> odds { get; set; }
    }

    public class T21
    {
        public Int64 gmid { get; set; }
        public Int64 cid { get; set; }
        public String ename { get; set; }
        public Int32 etid { get; set; }
        public Boolean iplay { get; set; }
        public String stime { get; set; }
        public Boolean tv { get; set; }
        public Boolean bm { get; set; }
        public Boolean f { get; set; }
        public Boolean f1 { get; set; }
        public Int64 mid { get; set; }
        public String mname { get; set; }
        public String status { get; set; }
        public Int32 rc { get; set; }
        public Int32 gscode { get; set; }
        public Int32 m { get; set; }
        public String gtype { get; set; }
        public Int32 iscc { get; set; }

        public List<Section2> section { get; set; }
    }

    public class Datassss
    {
        public List<T11> t1 { get; set; }
        public List<T21> t2 { get; set; }
    }

    public class Highlightnew
    {
        public int status { get; set; }
        public String msg { get; set; }
        public Datassss data { get; set; }
    }
}
