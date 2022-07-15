using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class GM
    {
        public Int64 gmid { get; set; }
        public Int32 etid { get; set; }
        public Int32 m { get; set; }
        public Int64 gtv { get; set; }
        public String ename { get; set; }
        public Int64 cid { get; set; }
        public String cname { get; set; }
        public Boolean iplay { get; set; }
        public String gtype { get; set; }
        public Boolean f1 { get; set; }
        public Boolean f { get; set; }
        public Boolean bm { get; set; }
        public Boolean tv { get; set; }
        public Int32 scard { get; set; }
        public Int32 iscc { get; set; }
        public String stime { get; set; }
        public String oldgmid { get; set; }
        public Int64 port { get; set; }
    }

    public class GameMaster
    {
        public List<GM> t1 { get; set; }

    }
}
