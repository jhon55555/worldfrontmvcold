using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public partial class HighlightData
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "eventtypeid is empty.")]
        public Int64 etid { get; set; }
        [Required(ErrorMessage = "action is empty.")]
        public String action { get; set; }
        public Int32 m { get; set; }
    }
    public partial class HighlightDataOpen
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "eventtypeid is empty.")]
        public Int64 etid { get; set; }
        [Required(ErrorMessage = "action is empty.")]
        public String action { get; set; }
        public String tablename { get; set; }
    }
}
