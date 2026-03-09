import com.ser.blueline.*

import de.ser.doxis4.agentserver.AgentExecutionResult
import de.ser.doxis4.agentserver.AgentServerReturnCodes

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import groovy.transform.Field

import com.ser.blueline.metaDataComponents.IArchiveClass;
import com.ser.blueline.IQueryParameter;
import com.ser.blueline.IDocumentHitList;
import com.ser.blueline.resultset.IResultSet;

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ser.blueline.agents.IAgentExecutionObject;
import com.ser.blueline.agents.IAgentExecutionResult;

import com.ser.foldermanager.*

import com.ser.foldermanager.IFolder

import java.util.stream.Collectors;



@Field Logger log = LogManager.getLogger("agent.DeduplicateE-files-ISU-Vertragskontoakte")
@Field ISession s = doxis4Session
@Field IDocumentServer ds = documentServer

try {
 log.info("------------------------------ Agent job Started ------------------------------")

 IQueryOptions options = ds.getClassFactory().createQueryOptions(s, null, null, VersionIdentifier.CURRENT_VERSION, false)

 IResultSet duplicatedFolders = ds.searchAttributes(s, "SELECT VERTRKONT_NUM FROM AKTEN WHERE VERTRKONT_NUM IS NOT NULL AND TYPE='e09be6ea-c7a7-4724-8e2f-53efca4cf3a4' GROUP BY VERTRKONT_NUM HAVING COUNT(*) > 1", options);

 log.info("Rowcount : {}",duplicatedFolders.getRowCount())

 List<String> duplicatedFolders_docsIDs = new ArrayList<>();
 
 IAgentExecutionObject executionObject = null
 String agentDefID = "" //KITOLTENI !!
 
 for (int row = 0; row < duplicatedFolders.getRowCount(); row++) {
 	
  List<IValue> values = duplicatedFolders.getValues(row,0)
  String value = values[0].getStringRepresentation()

  List<String> E1 = ["E1"]
  List<String> E2 = ["E2"]
  List<String> E3 = ["E3"]
  
  List<IFolder> duplicatedFolderObjects = myqueryFolders("TYPE='e09be6ea-c7a7-4724-8e2f-53efca4cf3a4' AND VERTRKONT_NUM='${value}'","AKTEN")


  /*

  List<IDocument> duplicatedFolders_Docs_E1 = myqueryDocuments("TYPE='d88a1b04-b827-49e9-bf18-0f8e1d839bf6' AND SAP_OBJECT = 'PRINTDOC' AND SAP_DOCTYPE = 'ZMOSINVOIC' AND VERTRKONT_NUM='${value}'",E1,true)
  List<IDocument> duplicatedFolders_Docs_E2 = myqueryDocuments("TYPE='4c7f3d54-55a1-443a-b521-619245713c08' AND VERTRKONT_NUM='${value}' AND ((SAP_OBJECT = 'ISUACCOUNT' AND SAP_DOCTYPE = 'ISU_ACCT') OR (SAP_OBJECT = 'ISUPARTNER' AND SAP_DOCTYPE = 'ZMOS_DUN0') OR (SAP_OBJECT = 'ISUACCOUNT' AND SAP_DOCTYPE = 'Z_ISU_ACCT'))",E2,true)
  List<IDocument> duplicatedFolders_Docs_E3 = myqueryDocuments("TYPE='d560c864-48b9-4f63-926d-d0f0e1fba539' AND SAP_OBJECT = 'ISUACCOUNT' AND SAP_DOCTYPE = 'ZMOSMOVEIN' AND VERTRKONT_NUM='${value}'",E3,true)
  
  List<IDocument> duplicatedFolders_Docs = new ArrayList<>();

  if(duplicatedFolders_Docs_E1 != null){
  duplicatedFolders_Docs.addAll(duplicatedFolders_Docs_E1)
  }
  if(duplicatedFolders_Docs_E2 != null){
  duplicatedFolders_Docs.addAll(duplicatedFolders_Docs_E2)
  }
  if(duplicatedFolders_Docs_E3 != null){
  duplicatedFolders_Docs.addAll(duplicatedFolders_Docs_E3)
  }


  //List<IDocument> duplicatedFolders_Docs = myqueryDocuments("(TYPE='d560c864-48b9-4f63-926d-d0f0e1fba539' AND SAP_OBJECT = 'ISUACCOUNT' AND SAP_DOCTYPE = 'ZMOSMOVEIN' AND VERTRKONT_NUM='${value}') OR (TYPE='d88a1b04-b827-49e9-bf18-0f8e1d839bf6' AND SAP_OBJECT = 'PRINTDOC' AND SAP_DOCTYPE = 'ZMOSINVOIC' AND VERTRKONT_NUM='${value}') OR (TYPE='4c7f3d54-55a1-443a-b521-619245713c08' AND VERTRKONT_NUM='${value}' AND ((SAP_OBJECT = 'ISUACCOUNT' AND SAP_DOCTYPE = 'ISU_ACCT') OR (SAP_OBJECT = 'ISUPARTNER' AND SAP_DOCTYPE = 'ZMOS_DUN0') OR (SAP_OBJECT = 'ISUACCOUNT' AND SAP_DOCTYPE = 'Z_ISU_ACCT')))")
*/
  String mainFolderObjectIndex = ""
  String referencingInfoObjectID = ""
  int i = 0
  for(IFolder duplicatedFolderObject : duplicatedFolderObjects){ 
   	
   IInformationObject[] duplicatedFolderObjectReferencingInfoObjects = ds.getReferencingInformationObjects(s, (IInformationObject)duplicatedFolderObject, true, VersionIdentifier.CURRENT_VERSION, IRetentionState.ALL, false, s.getDatabaseByName("AKTEN"))

   if (duplicatedFolderObjectReferencingInfoObjects.length == 1){
   	
    if (referencingInfoObjectID.equals("")){
     referencingInfoObjectID = duplicatedFolderObjectReferencingInfoObjects[0].getId()
    }
    
    if (mainFolderObjectIndex.equals("")){
     mainFolderObjectIndex = String.valueOf(i)
    }
    
       	
    if (referencingInfoObjectID != duplicatedFolderObjectReferencingInfoObjects[0].getId()){
    	log.error("A duplikalt aktak referencingOjectje nem egyezik. AKTA ID: {}, COMMON ATTRIBUTE: {}", duplicatedFolderObject, value )
    	mainFolderObjectIndex = "" 
    	referencingInfoObjectID = ""	
    	break;
    }
   }

   if (duplicatedFolderObjectReferencingInfoObjects.length > 1){
    log.error("Tobb mint 1 referencingObjectje van az adott aktanak ID: {} COMMON ATTRIBUTE: {}", duplicatedFolderObject, value)
    mainFolderObjectIndex= "" 
    referencingInfoObjectID = "" 
    break; 
   }
   i++

   if (i == duplicatedFolderObjects.size() &&  mainFolderObjectIndex.equals("")){
    	mainFolderObjectIndex = String.valueOf(0)
   }

 }



if (!mainFolderObjectIndex.equals("")){

log.info("MAIN FOLDER OBJECT ID: {}", duplicatedFolderObjects.get(Integer.valueOf(mainFolderObjectIndex)).getId())

int j = 0

duplicatedfoldercheck: for(IFolder duplicatedFolderObject : duplicatedFolderObjects){ 



if (j != Integer.valueOf(mainFolderObjectIndex)){

IFolderDescriptors duplicatedFolderObjectDescriptors = duplicatedFolderObject.getDescriptors()

// Deszkriptor osszehasonlitas
for (int d = 0 ; d < duplicatedFolderObjectDescriptors.getCount();d++){
	IFolderDescriptor desc = duplicatedFolderObjectDescriptors.getItem(d)
	String[] descValues = desc.getValues()
	String descID = desc.getID()
    
	log.info("Descriptorok szama az aktan : {}, COMMON ATTR: {}, jelenlegi akta ID : {}, DESCVALUES: {}, DESC SERACHLITERAL:{}", duplicatedFolderObjectDescriptors.getCount(), value, duplicatedFolderObject.getId(), descValues, descID)
    
	IFolder mainFolderObject = duplicatedFolderObjects.get(Integer.valueOf(mainFolderObjectIndex))
	IFolderDescriptor mainDesc = mainFolderObject.getItemByID(descID)
	String[] mainDescValues = mainDesc.getValues()
	
	if (mainDescValues != null && descValues != null && mainDescValues != descValues){
	 log.info("Deszkriptorok nem egyeznek. Desc ID: {}, Common Attribute: {}", descID, value)
	 break duplicatedfoldercheck;
	}

	if (mainDescValues == null && descValues != null){
		mysetDescriptorValueS(mainFolderObject,descID,descValues)
	}
	
}











}

j++

}

}











  
/* kiszedjuk a doksik idjet.


  for (IDocument doc : duplicatedFolders_Docs){

   duplicatedFolders_docsIDs.add(doc.getID())
   







   
  	
  }
  
  log.info("Docs : {}", duplicatedFolders_Docs.size())
  
  log.info("VERTRKONT_NUM= {}, Duplicated E-fie size = {}",value,duplicatedFolderObjects.size())
 
 */
 
 }

 



// DocToFolder hivasa az osszes docra amelynek a COMMONATTR ugyanaz mint a duplikalt akta eseteben
/*
 for (String docID :  duplicatedFolders_docsIDs){

  executionObject = ds.createAgentExecutionObject(s, agentDefID)
  executionObject.setAgentEventObjectClientID(docID)
  int agentExecutionResultReturnCode = executionObject.execute().getReturnCode()
 
  if (agentExecutionResultReturnCode == 0){
   	
     log.info("Agent returnCode={}", agentExecutionResultReturnCode)
     ds.deleteDocument(s, doc)
    
    }
    else{
    log.error("Agent Return code {} for DOC: {}", agentExecutionResultReturnCode, docID)	
      	
    }
	
 }

 */



 log.info("---------------------------- Agent job Ended Successfully ----------------------------")
 
 return new AgentExecutionResult(AgentServerReturnCodes.RETURN_CODE_SUCCESS, null, false, null)
    
} catch (Exception e) {
 log.error("Error: " + e.getMessage())  
 log.error("---------------------------- Agent job Ended with Error----------------------------")
 return new AgentExecutionResult(AgentServerReturnCodes.RETURN_CODE_ERROR, "Error: " + e.getMessage(), false, null)
}



public List<IDocument> myqueryDocuments(String whereClause, List<String> databaseNames, boolean currentVersionOnly) throws BlueLineException {
 IQueryParameter qParam = null
 try {
 qParam = ds.getClassFactory().getQueryParameterInstance(
 s,
 databaseNames.toArray(new String[0]),
 ds.getClassFactory().getExpressionInstance(whereClause),
 null,
 null
 )

 qParam.setCurrentVersionOnly(currentVersionOnly);
            
 IDocumentHitList hitList = ds.query(qParam, s);
 IDocument[] docs = hitList.getDocumentObjects();
            
 if (docs.length == 0) {
  return Collections.emptyList();
 } else {
  return Arrays.asList(docs);
 }

}

 
 catch (Exception e){
  log.error("Error: {}", e.getMessage())
 }
 finally {
  if (qParam != null){
   qParam.close()
  }
 }
}
        
        
        
        
        
        
    
/*

public List<IDocument> myqueryDocuments(String whereClause) throws BlueLineException {
 Pattern p = Pattern.compile("TYPE='([a-f0-9-]{36})'");
 Matcher m = p.matcher(whereClause);
        
 List<String> databaseNames = new ArrayList<>();
 while (m.find()) {
  IArchiveClass archClass = ds.getArchiveClass(m.group(1), s);         
  if (archClass == null) {
   log.error("Invalid Doc Class ID given :{}", archClass);
   continue;
  }
            
  for (String dbId : archClass.getAssignedDatabaseIDs()) {
   IContentRepository contentRepo = ds.getMetaDataConnector(s).getContentRepository(dbId);
   databaseNames.add(contentRepo.getShortName());
  }
 }
 IQueryParameter qParam = null

 try {
  qParam = ds.getClassFactory().getQueryParameterInstance(
  s,
  databaseNames.toArray(new String[0]),
  ds.getClassFactory().getExpressionInstance(whereClause),
  null,
  null
  )

  qParam.setCurrentVersionOnly(true);
            
  IDocumentHitList hitList = ds.query(qParam, s);
  IDocument[] docs = hitList.getDocumentObjects();
            
  if (docs.length == 0) {
   return Collections.emptyList();
  } else {
   return Arrays.asList(docs);
  }
 }
 catch (Exception e){
  log.error("Error: {}", e.getMessage())
  }
 finally {
  if (qParam != null){
   qParam.close()
  }
 }
}

*/






public List<IFolder> myqueryFolders(String whereClause, String databaseName) throws BlueLineException{
	
 IQueryParameter qParam = null

 try {
  qParam = ds.getClassFactory().getQueryParameterInstance(
  s,
  new String[] { databaseName },
  ds.getClassFactory().getExpressionInstance(whereClause),
  null,
  null
  )
        
IDocumentHitList hitList = ds.query(qParam, s);
long hitlistcount = hitList.getTotalHitCount()
IInformationObject[] ios = hitList.getInformationObjects();


   if (ios.length == 0) {
    return Collections.emptyList();
   } else {
    //return ios.findAll { it instanceof IFolder }.collect { it as IFolder }
    
    ios.findAll { it instanceof IFolder } as List<IFolder>
   }
 }

 catch (Exception e){
  log.error("Error: {}", e.getMessage())
  }
 finally {
  if (qParam != null){
   qParam.close()
  }
 }
}


void mysetDescriptorValueS(IInformationObject infoObj, String descIdentifier, String[] vals) {

    IDescriptor desc = ds.getDescriptor(descIdentifier,s)

    IValueDescriptor valDesc = ds.getClassFactory().getValueDescriptorInstance(desc);

    for(String val : vals) {
    	valDesc.addValue(val);
    }
    infoObj.addDescriptor(valDesc); 
}

