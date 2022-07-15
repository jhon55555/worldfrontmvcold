using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class AccDabaBet
    {
        public Int32 uid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "betid is empty.")]
        public Int64 btid { get; set; }
    }
}
