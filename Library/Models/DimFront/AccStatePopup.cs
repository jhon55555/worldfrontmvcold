using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class AccStatePopup
    {
        public Int32 uid { get; set; }
       // [Range(1, Int64.MaxValue, ErrorMessage = "gameid is empty.")]
        public Int64 gmid { get; set; }
        //[Range(1, Int64.MaxValue, ErrorMessage = "marketid is empty.")]
        public String mid { get; set; }
        public String gtype { get; set; }
        [Required(ErrorMessage = "dtype is empty.")]
        public String dtype { get; set; }
    }
}
