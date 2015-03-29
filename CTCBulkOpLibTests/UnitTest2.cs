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
            CrmConnection connectionSource = new CrmConnection("CRM");
            OrganizationService serviceSource = new OrganizationService(connectionSource);

            CrmConnection connectionTarget = new CrmConnection("CRM2");
            OrganizationService serviceTarget = new OrganizationService(connectionTarget);

            CrmChangeTrackingManager mgr = new CrmChangeTrackingManager(serviceSource);

            mgr.ApplyChanges(serviceTarget, "account", new ColumnSet(true));

        }

        [TestMethod]
        public void TestApplyAccountAndContact()
        {
            CrmConnection connectionSource = new CrmConnection("CRM");
            OrganizationService serviceSource = new OrganizationService(connectionSource);

            CrmConnection connectionTarget = new CrmConnection("CRM2");
            OrganizationService serviceTarget = new OrganizationService(connectionTarget);

            CrmChangeTrackingManager mgr = new CrmChangeTrackingManager(serviceSource);
            
            mgr.ApplyChanges(serviceTarget,new ApplyChangesOptions[] 
                                 { 
                                     new ApplyChangesOptions() { EntityName="account", Columns=new ColumnSet(true)},
                                     new ApplyChangesOptions() { EntityName="contact", Columns=new ColumnSet(true)},
                                 });

        }
    }
}
