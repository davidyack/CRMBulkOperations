using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Client;
using Microsoft.Xrm.Client.Services;
using CTCBulkOpLib;
using Microsoft.Xrm.Sdk.Query;

namespace CTCBulkOpLibTests
{
    [TestClass]
    public class UnitTest2
    {
        [TestMethod]
        public void TestApplyAccount()
        {
            CrmConnection c = new CrmConnection("CRM");
            OrganizationService service = new OrganizationService(c);
            CrmConnection c2 = new CrmConnection("CRM2");
            OrganizationService service2 = new OrganizationService(c2);
            CrmChangeTrackingManager mgr = new CrmChangeTrackingManager(service);

            //mgr.ApplyChanges(service2,"account", new ColumnSet(new string[] {"name"}));
            mgr.ApplyChanges(service2, "account", new ColumnSet(true));
        }
    }
}
