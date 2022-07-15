using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFrontGroup
{
    public class Login
    {
        //public Guid guid { get; set; }
        //public Guid guid1 { get; set; }
        [Required(ErrorMessage = "username is empty.")]
        public String uname { get; set; }
        [Required(ErrorMessage = "password is empty.")]
        //[StringLength(20, ErrorMessage = "password minimum lenght is 8.", MinimumLength = 8)]
        public String pass { get; set; }
        [Required(ErrorMessage = "webdomain is empty.")]
        [RegularExpression("^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$", ErrorMessage = "invalid domain.")]
        public String webdom { get; set; }
        [Required(ErrorMessage = "ip is empty.")]
        public String ip { get; set; }
        public Boolean vpn { get; set; }
        public Boolean host { get; set; }
        [Required(ErrorMessage = "browserdetail is empty.")]
        public String bdetail { get; set; }
    }
}
