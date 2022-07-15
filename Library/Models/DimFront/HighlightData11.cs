using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public class T12
    {
        public Int64 gmid { get; set; }
        public String mid { get; set; }
        public int m { get; set; }
        public String etid { get; set; }
        public DateTime stime { get; set; }
        public Boolean iplay { get; set; }
        public Double oid { get; set; }
    }

    public class HighlightData11
    {
        public IList<T12> t1 { get; set; }
    }
}
