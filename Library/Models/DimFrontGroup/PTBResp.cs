using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models.DimFrontGroup
{
    
    public class DataPTB
    {
        public string url { get; set; }
    }

    public class PTBResp
    {
        public int status { get; set; }
        public string msg { get; set; }
        public DataPTB data { get; set; }
    }
}
