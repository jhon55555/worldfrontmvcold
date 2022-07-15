using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class WorliRule
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "sectionid is empty.")]
        public Int64 sid { get; set; }
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
    }
}
