using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Createreq
    {//@ChildUserid,@AccNumber,@AccName,@Ifscode,@BankName,@AccountType,@Remark,@UserName, @gUserId
        [Required(ErrorMessage = "accounttype is empty.")]
        public String atype { get; set; }
        [Required(ErrorMessage = "bankname is empty.")]
        public String bname { get; set; }
        //[Required(ErrorMessage = "Ifsc code is empty.")]
        public String ifsc { get; set; }
        //[Required(ErrorMessage = "accountname is empty.")]
        public String aname { get; set; }
        //[Required(ErrorMessage = "accountnumber is empty.")]
        public String anumber { get; set; }
        public Decimal amt { get; set; }
        public String rem { get; set; }
        //public String uname { get; set; }
        public String guid { get; set; }
        public Int32 exp { get; set; }
    }
}
