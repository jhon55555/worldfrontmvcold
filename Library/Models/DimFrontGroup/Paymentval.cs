using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class Paymentval
    {
        public String uid { get; set; }
        [Range(1, Double.MaxValue, ErrorMessage = "amount is empty.")]
        public Double amt { get; set; }
    }
}
