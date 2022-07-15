using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Usercreate
    {
        [Required(ErrorMessage = "username is empty.")]
        [StringLength(20, ErrorMessage = "username minimum lenght is 4.", MinimumLength = 4)]
        [RegularExpression("^[a-zA-Z0-9]*$", ErrorMessage = "Only Alphabets and Numbers allowed.")]
        public String uname { get; set; }
        [Required(ErrorMessage = "fullname is empty.")]
        public String fname { get; set; }
        [Required(ErrorMessage = "password is empty.")]
        public String pass { get; set; }
        [Required(ErrorMessage = "city is empty.")]
        public String ct { get; set; }
        [Required(ErrorMessage = "mobileno is empty.")]
        public String mno { get; set; }
        [Required(ErrorMessage = "ipaddress is empty.")]
        public String ip { get; set; }
        [Required(ErrorMessage = "browserdetail is empty.")]
        public String bdetail { get; set; }
        public String refc { get; set; }
        [Required(ErrorMessage = "webdomain is empty.")]
        public String webdom { get; set; }
    }
    public class Wolfpromotionreg
    {
        [Required(ErrorMessage = "name is empty.")]
        //[StringLength(20, ErrorMessage = "username minimum lenght is 4.", MinimumLength = 4)]
        //[RegularExpression("^[a-zA-Z0-9]*$", ErrorMessage = "Only Alphabets and Numbers allowed.")]
        public String name { get; set; }
        [Required(ErrorMessage = "email is empty.")]
        public String email { get; set; }
        //[Required(ErrorMessage = "address is empty.")]
        public String address { get; set; }
        [Required(ErrorMessage = "mobileno is empty.")]
        public String mobileno { get; set; }
    }
    public class Wolfinquiriesreg
    {
        [Required(ErrorMessage = "name is empty.")]
        //[StringLength(20, ErrorMessage = "username minimum lenght is 4.", MinimumLength = 4)]
        //[RegularExpression("^[a-zA-Z0-9]*$", ErrorMessage = "Only Alphabets and Numbers allowed.")]
        public String name { get; set; }
        [Required(ErrorMessage = "city is empty.")]
        public String city { get; set; }
        //[Required(ErrorMessage = "address is empty.")]
        [Required(ErrorMessage = "number is empty.")]
        public String number { get; set; }
    }
    public class Wolfpromotionpage
    {
        //[Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
    }
}
