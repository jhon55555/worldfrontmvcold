using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class History
    {
        public Int32 uid { get; set; }
        [Required(ErrorMessage = "fromdate is empty.")]
        public String fdt { get; set; }
        [Required(ErrorMessage = "todate is empty.")]
        public String tdt { get; set; }
        [Required(ErrorMessage = "type is empty.")]
        public String type { get; set; }
    }
}
