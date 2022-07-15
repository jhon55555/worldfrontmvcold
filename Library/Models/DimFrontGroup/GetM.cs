using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFrontGroup
{
    public class GetM
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "eventtypeid is empty.")]
        public Int32 etid { get; set; }
    }
    public class GetMRes
    {
        public int status { get; set; }
        public string msg { get; set; }
        public int data { get; set; }
    }
}
