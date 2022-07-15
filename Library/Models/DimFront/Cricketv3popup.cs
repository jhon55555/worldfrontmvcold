using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class Cricketv3popup
    {
        [Range(0, Int64.MaxValue, ErrorMessage = "marketid is empty.")]
        public Int64 mid { get; set; }
        public Int64 uid { get; set; }
    }
}
