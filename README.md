CRM Bulk Operations API - CRM 2015 Spring Update (7.1 )
=================

See Pre71 branch for CRM 2015 and CRM 2015 version of the API

You can install this into a project using the following NuGet Console command
Install-Package CRMBulkOperations -Pre 

NuGet project page https://www.nuget.org/packages/CRMBulkOperations 

The CRM Bulk Operations is a helper class to perform bulk operations with Dynamics CRM.  The library has support for both Execute Multiple and Threads for batching multiple requests and managing sending them to Dynamics CRM.  

The following types of operations are supported
- Bulk Insert
- Bulk Update and Upsert
- Bulk Update from Query 
- Bulk Run Workflow
- Bulk Run Workflow from Query
- Bulk Set State
- Bulk Set State from Query
- Run Multiple Request to handle all other types of Organization Request
