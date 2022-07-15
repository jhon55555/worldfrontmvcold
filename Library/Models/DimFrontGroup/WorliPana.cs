using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class WorliPana
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "sectionid is empty.")]
        public Int64 sid { get; set; }
        //[Range(0, Double.MaxValue, ErrorMessage = "userrate is empty.")]
        [Required(ErrorMessage = "userrate is empty.")]
        public String urate { get; set; }
    }
}
