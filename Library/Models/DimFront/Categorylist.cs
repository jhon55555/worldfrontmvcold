using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models.DimFront
{
    
    public class Categorylist
    {
        public Int32 pid { get; set; }
        //[Range(1, Int16.MaxValue, ErrorMessage = "istest is empty.")]
        public Int16 istest { get; set; }
    }

    public class Slotlist
    {
        public Int64 cid { get; set; }
        //[Range(1, Int16.MaxValue, ErrorMessage = "istest is empty.")]
        public Int16 istest { get; set; }
    }
}
