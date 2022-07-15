using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFront
{
    public partial class PaymentUrl
    {
        public string uid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "amount is empty")]
        public Int64 amt { get; set; }
        //[Required(ErrorMessage = "webref is empty.")]
        public String webref { get; set; }
        public String token { get; set; }
    }
    //public partial class PaymentUrl
    //{
    //    public string uid { get; set; }
    //    [Range(1, Int64.MaxValue, ErrorMessage = "amount is empty")]
    //    public Int64 amt { get; set; }
    //    //[Required(ErrorMessage = "webref is empty.")]
    //    public String webref { get; set; }
    //    public String token { get; set; }
    //    [Required(ErrorMessage = "type is empty.")]
    //    public String type { get; set; }
    //    public String ptype { get; set; }
    //    public String pname { get; set; }
    //}
    public partial class PaymentUrlnew
    {
        public string uid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "amount is empty")]
        public Int64 amt { get; set; }
        //[Required(ErrorMessage = "webref is empty.")]
        public String webref { get; set; }
        public String token { get; set; }
        [Required(ErrorMessage = "type is empty.")]
        public String type { get; set; }
        public String ptype { get; set; }
        public String pname { get; set; }
    }
}
