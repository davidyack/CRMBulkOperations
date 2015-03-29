using ctccrm.ServerCommon.OrgServiceHelpers;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
// =====================================================================
//  File:		CrmChangeTrackingManager
//  Summary:	Tracks changes to CRM entities
// =====================================================================
// 
//
//  Copyright(C) 2015 Colorado Technology Consultants Inc.  All rights reserved.
//
//
//  THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY
//  KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
//  IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
//  PARTICULAR PURPOSE.
// 
//  Any use or other rights related to this source code, resulting object code or 
//  related artifacts are controlled the prevailing EULA in effect. See the EULA
//  for detail rights. In the event no EULA was provided contact copyright holder
//  for a current copy.
//
// =====================================================================
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CTCBulkOpLib
{
    public class CrmChangeTrackingManager
    {
        private IOrganizationService _Service = null;

        public CrmChangeTrackingManager(IOrganizationService crmService)
        {
            _Service = crmService;
           
           
        }
        public void ApplyChanges(IOrganizationService targetService, string entityName, ColumnSet columns)
        {
            RetrieveEntityChangesRequest changeRequest = new RetrieveEntityChangesRequest();
            changeRequest.EntityName = entityName;
            changeRequest.Columns = columns;
            changeRequest.PageInfo = new PagingInfo() { Count = 100, PageNumber = 1, ReturnTotalRecordCount = false };
            var trackEntity = RetrieveTrackingEntity(targetService, entityName);
            if (trackEntity.Contains("ctccrm_datatoken"))
                changeRequest.DataVersion = trackEntity.GetAttributeValue<string>("ctccrm_datatoken");

            CrmBulkServiceManager bulkMgr = new CrmBulkServiceManager(targetService);
            
            ChangesToToAction(changeRequest, (response) =>
            {                
                List<Entity> entitiesToUpSert = new List<Entity>();
                List<EntityReference> entitiesToDelete = new List<EntityReference>();

                PrepareUpsertDeleteLists(response, entitiesToUpSert, entitiesToDelete);

                //UpsertRequest req = new UpsertRequest() { Target = entitiesToUpSert.FirstOrDefault() };
                //targetService.Execute(req);
                trackEntity["ctccrm_datatoken"] = response.EntityChanges.DataToken;
                entitiesToUpSert.Add(trackEntity);
                bulkMgr.BulkUpsertAndDelete(entitiesToUpSert, entitiesToDelete, 
                    transactionMode: CTCBulkTransactionMode.None);

            });


        }

        private static Entity RetrieveTrackingEntity(IOrganizationService targetService, string entityName)
        {
            try
            {
                RetrieveRequest trackReq = new RetrieveRequest();
                trackReq.Target = new EntityReference("ctccrm_entitychangetracking", new KeyAttributeCollection());
                trackReq.Target.KeyAttributes.Add("ctccrm_name", entityName);
                var trackResponse = targetService.Execute(trackReq) as RetrieveResponse;
                var trackEntity = trackResponse.Entity;
                return trackEntity;
            }
            catch(Exception)
            {
                var newEntity = new Entity("ctccrm_entitychangetracking");
                newEntity["ctccrm_name"] = entityName;
                return newEntity;
            }
        }

        private static void PrepareUpsertDeleteLists(RetrieveEntityChangesResponse response, List<Entity> entitiesToUpSert, List<EntityReference> entitiesToDelete)
        {
            foreach (var change in response.EntityChanges.Changes)
            {
                if (change.Type == ChangeType.NewOrUpdated)
                {
                    var changeditem = (NewOrUpdatedItem)change;
                    Entity changedEntity = changeditem.NewOrUpdatedEntity;
                    TrimFieldsFromChanges(changedEntity);
                    entitiesToUpSert.Add(changedEntity);
                }
                else if (change.Type == ChangeType.RemoveOrDeleted)
                {
                    var deleteditem = (RemovedOrDeletedItem)change;
                    EntityReference deletedEntityReference = deleteditem.RemovedItem;
                    entitiesToDelete.Add(deletedEntityReference);
                }
            }
        }

        private static void TrimFieldsFromChanges(Entity changedEntity)
        {
            if (changedEntity.Contains("ownerid"))
                changedEntity.Attributes.Remove("ownerid");
        }
        private void ChangesToToAction(RetrieveEntityChangesRequest req, Action<RetrieveEntityChangesResponse> action)
        {

            int pageCount = 1;
            int pageSize = req.PageInfo.Count;

            while (true)
            {

                var results = _Service.Execute(req) as RetrieveEntityChangesResponse;

                action(results);

                

                if (results.EntityChanges.MoreRecords)
                {
                    req.DataVersion = results.EntityChanges.DataToken;
                    pageCount++;
                    req.PageInfo = new PagingInfo();
                    req.PageInfo.Count = pageSize;
                    req.PageInfo.PageNumber = pageCount;
                    req.PageInfo.PagingCookie = results.EntityChanges.PagingCookie;
                }
                else
                    break;
            }


        }
    }
}
