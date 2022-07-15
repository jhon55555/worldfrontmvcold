using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    public class MarketAnalysisReport
    {
        public Int64 gmid { get; set; }
        public String mid { get; set; }
        public double amt { get; set; }
        public String gtype { get; set; }
        public String stime { get; set; }
        public String mname { get; set; }
        public String etname { get; set; }
        public Int64 sid { get; set; }
        public String gname { get; set; }
        public Int64 cntsoda { get; set; }
        public Int64 etid { get; set; }
        public Int32 iscc { get; set; }
        public Int32 shlight { get; set; }
        public Boolean tv { get; set; }
    }
}
