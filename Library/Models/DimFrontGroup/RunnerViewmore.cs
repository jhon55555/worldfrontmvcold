using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public partial class RunnerViewmore
    {
        [Required(ErrorMessage = "transactionid is empty.")]
        public String txnid { get; set; }
    }
}
