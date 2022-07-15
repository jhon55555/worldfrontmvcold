using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

   
    public class T1rs
    {
        public string username { get; set; }
        public string winamount { get; set; }
        public string time { get; set; }
    }

    public class UserentryRes
    {
        public List<T1rs> t1 { get; set; }
    }
    public class GetUserentry
    {
        public string domain { get; set; }
    }
}
