using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class Datum1
    {
        public Int64 gmid { get; set; }
        public String ename { get; set; }
        public Int32 etid { get; set; }
        public Int32 m { get; set; }
        public Boolean iplay { get; set; }
        public Int64 cid { get; set; }
        public String cname { get; set; }
        public String gtype { get; set; }
        public Boolean tv { get; set; }
        public Int64 gtv { get; set; }
        public Int32 iscc { get; set; }
        public Int32 scard { get; set; }
        public String stime { get; set; }
        public Boolean f1 { get; set; }
        public Boolean f { get; set; }
        public Boolean bm { get; set; }
        public String oldgmid { get; set; }
    }

    public class GameDetailRes
    {
        public int status { get; set; }
        public String msg { get; set; }
        public List<Datum1> data { get; set; }
    }
}
