using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class AccountStatement
    {
        public Int32 uid { get; set; }
        [Required(ErrorMessage = "fromdate is empty.")]
        public String fdt { get; set; }
        [Required(ErrorMessage = "todate is empty.")]
        public String tdt { get; set; }
        [Range(1, Int32.MaxValue, ErrorMessage = "type is empty.")]
        public Int32 type { get; set; }
    }
}
