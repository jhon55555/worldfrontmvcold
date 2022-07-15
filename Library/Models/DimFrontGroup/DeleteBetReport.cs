using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public partial class DeleteBetReport
    {
        public Int32 uid { get; set; }
        [Required(ErrorMessage = "date is empty.")]
        public String dt { get; set; }
        [Required(ErrorMessage = "type is empty.")]
        public Int32 type { get; set; }
    }
}
