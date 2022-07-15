using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public class GetRules
    {
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
    }
    public class Horsedetail
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "gametid is empty.")]
        public Int64 gmid { get; set; }
    }
}
