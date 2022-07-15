using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class Fancypopup
    {
        public Int32 uid { get; set; }
        //[Range(1, Int64.MaxValue, ErrorMessage = "gameid is empty.")]
        //public Int64 gmid { get; set; }
        [Required(ErrorMessage = "marketid is empty.")]
        public String mid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "sectionid is empty.")]
        public Int64 sid { get; set; }
    }
}
