using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public class t11
    {
        public Int32 etid { get; set; }
        public String etype { get; set; }
        public Int32 m { get; set; }
    }

    public class GetMre
    {
        public List<t11> t1 { get; set; }
    }

}
