import groovy.sql.Sql
import schemacrawler.schema.Schema
import schemacrawler.schema.Database
import schemacrawler.schema.Table
import schemacrawler.schema.Column
import schemacrawler.schemacrawler.InclusionRule
import schemacrawler.schemacrawler.SchemaCrawlerException
import schemacrawler.schemacrawler.SchemaCrawlerOptions
import schemacrawler.schemacrawler.SchemaInfoLevel
import schemacrawler.utility.SchemaCrawlerUtility
import schemacrawler.schema.Index

def JDBC_URL = "jdbc:jtds:sqlserver://localhost/listdb"
def USER_NAME = "dbuser"
def PASSWORD = "archernx01"

def SCHEMA_NAME = "listdb.dbo"

def GENERATED_PATH = "d:/generated/ios/"

//Configure crawler options
def options = new SchemaCrawlerOptions()
options.schemaInfoLevel = SchemaInfoLevel.standard()
options.schemaInclusionRule = new InclusionRule(SCHEMA_NAME, InclusionRule.NONE)

//Configure database connection
def sql = Sql.newInstance(JDBC_URL, USER_NAME, PASSWORD, "net.sourceforge.jtds.jdbc.Driver")

//Get the database
def Database database = SchemaCrawlerUtility.getDatabase(sql.connection, options)

def syncConfigFile = new XmlParser().parse(new File("d:/listdbconfig.xml"))

def syncScopes = syncConfigFile.SyncConfiguration.SyncScopes

def Schema defaultSchema

database.schemas.each { s ->
  println("Schema Name: ${s.name}")
  defaultSchema = s
}

defaultSchema.tables.each { t ->
  println(t.name)
}

syncScopes.SyncScope.each { e ->
  e.SyncTables.SyncTable.each { t ->
    def tableName = t.'@Name'
    def globalName = t.'@GlobalName'
    def allColumns = new Boolean(t.'@IncludeAllColumns')
    def Table table = defaultSchema.tables.find { tm -> tm.name.replace("\"", "").equalsIgnoreCase(tableName) }
    println("Processing table: ${tableName}")
    new File(GENERATED_PATH + globalName + "Entity.h").withWriter { f ->
      f.println("// Generated file. DO NOT MODIFY")
      f.println("// ${globalName}Entity.h")
      f.println()
      f.println("#import <CoreData/CoreData.h>")
      f.println('#import "OfflineEntity.h"')
      f.println()
      f.println("@interface ${globalName}Entity :  OfflineEntity")
      f.println("{")
      f.println("}")
      f.println()
      table.columns.findAll { cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
        def columnName = c.name
        def dataType = c.type.name
        if (dataType.equalsIgnoreCase("int")) dataType = "NSNumber"
        if (dataType.equalsIgnoreCase("nvarchar")) dataType = "NSString"
        if (dataType.equalsIgnoreCase("datetime")) dataType = "NSDate"
        f.println("@property (nonatomic, retain) ${dataType} * ${columnName};")
      }
      f.println()
      f.println("@end")
    }

    new File(GENERATED_PATH + globalName + "Entity.m").withWriter { f ->
      f.println("// Generated file. DO NOT MODIFY")
      f.println("// ${globalName}Entity.m")
      f.println()
      f.println('#import "' + globalName + 'Entity.h"')
      f.println("@implementation ${globalName}Entity")
      f.println()
      table.columns.findAll { cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
        def columnName = c.name
        def dataType = c.type.name
        if (dataType.equalsIgnoreCase("int")) dataType = "NSNumber"
        if (dataType.equalsIgnoreCase("nvarchar")) dataType = "NSString"
        if (dataType.equalsIgnoreCase("datetime")) dataType = "NSDate"
        f.println("@dynamic ${columnName};")
      }
      f.println()
      f.println("-(void) logEntity")
      f.println("{")
      table.columns.findAll { cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
        def columnName = c.name
        f.println('\tNSLog(@"%@ ' + columnName + ': %@", self.EntityType, self.' + columnName + ');')
      }
      f.println('\t[super logEntity];')
      f.println("}")
      f.println()
      f.println("@end")
    }
  }

  new File(GENERATED_PATH + "SyncUtils.h").withWriter { f ->
    f.println("// Generated file. DO NOT MODIFY")
    f.println("//SyncUtils.h")
    f.println()
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      f.println("#import ${globalName}Entity.h")
    }
    f.println()
    f.println("@interface SyncUtils : NSObject {")
    f.println()
    f.println("}")
    f.println()
    f.println("+ (void)saveManagedObjects:(NSManagedObjectContext *)context;")
    f.println()
    f.println("+ (NSDate *)parseDate:(id)str;")
    f.println("+ (id) dateToString:(NSDate*)date;")
    f.println()
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      f.println("+ (void)populate${globalName}:(id)dict withMetadata:(id)metadata withContext:(NSManagedObjectContext*)context;")
    }
    f.println()
    e.SyncTables.SyncTable.findAll { t -> t.FilterColumns.FilterColumn.size() > 0}.each { tt ->
      def globalName = tt.'@GlobalName'
      f.println("+ (void) delete${globalName}:(${globalName}Entity*)list inContext:(NSManagedObjectContext*)context;")
    }
    f.println()
    f.println("+ (bool) ProcessODataJsonChanges:(NSString*)jsonChanges withClientMetadata:(ClientMetadata**)clientMetadata withContext:(NSManagedObjectContext *)context;")
    f.println()
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      def tableName = t.'@Name'
      def Table table = defaultSchema.tables.find { tm -> tm.name.replace("\"", "").equalsIgnoreCase(tableName) }
      def globalName_camelCase = globalName[0].toLowerCase() + globalName.substring(1)
      table.columns.findAll { cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
        def columnName = c.name
        def dataType = c.type.name
        if (dataType.equalsIgnoreCase("int")) dataType = "NSNumber"
        if (dataType.equalsIgnoreCase("nvarchar")) dataType = "NSString"
        if (dataType.equalsIgnoreCase("datetime")) dataType = "NSDate"
        f.println("+ (${dataType} *) get${globalName}${columnName}:(NSNumber *)${globalName_camelCase}ID fromContext:(NSManagedObjectContext*)context;")
        f.println("+ (NSNumber *) get${globalName_camelCase}ID:(${dataType} *)${globalName}${columnName} fromContext:(NSManagedObjectContext*)context;")
      }
      f.println()
      f.println("@end")
    }
  }

  new File(GENERATED_PATH + "SyncUtils.m").withWriter { f ->
    f.println("// Generated file. DO NOT MODIFY")
    f.println("//SyncUtils.m")
    f.println()
    f.println("#import SyncUtils.h")
    f.println("#import SBJSON.h")
    f.println("#import Constants.h")
    f.println("#import <Foundation/Foundation.h>")
    f.println()
    f.println("@implementation SyncUtils")
    f.println()
    f.println("#pragma mark -")
    f.println("#pragma mark SyncController helpers")
    f.println()
    f.println("+ (bool) ProcessODataJsonChanges:(NSString *)jsonChanges withClientMetadata:(ClientMetadata **)clientMetadata withContext:(NSManagedObjectContext *)context")
    f.println("{")
    f.println("\tSBJSON *json = [[SBJSON alloc] init];")
	f.println("\tid myObj = [json objectWithString:jsonChanges];")
    f.println('\tid syncData = [[myObj valueForKey:@"d"] valueForKey:@"__sync"];')
    f.println("\t// get has more changes")
    f.println('\tbool moreChangesAvailable = [[syncData valueForKey:@"moreChangesAvailable"] boolValue]');
	f.println("\t// Update ClientMetada")
    f.println('\t(* clientMetadata).syncBlob = [syncData valueForKey:@"serverBlob"];')
    f.println('\tNSArray * results = [[myObj valueForKey:@"d"] valueForKey:@"results"];')
    f.println('\tfor (id table in results) {')
    f.println('\t\tid metadata = [table valueForKey:@"__metadata"];')
    f.println('\t\tNSString* type = [metadata valueForKey:@"type"];')
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      f.println('\t\tif ([type isEqualToString:@"DefaultScope.' + globalName +'"])')
	  f.println('\t\t{')
	  f.println("\t\t\t[Utils populate${globalName}:table withMetadata:metadata withContext:context];")
      f.println('\t\t}')
    }
    f.println('\t}')
    f.println("\t[json release];")
    f.println("\treturn moreChangesAvailable;")
    f.println("}")
    f.println()
    f.println("+ (bool) hasChanges:(NSManagedObjectContext*) context changes:(NSArray **)listOfChanges")
    f.println("{")
    f.println("\t//// if client has not synced to server UploadChanges is not allowed")
    f.println("if ([Utils clientHasSynced:context])")
    f.println("\t{")
    f.println("\t\tNSError *error = nil;")
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      f.println("\t\tNSFetchRequest *fetch${globalName}s = [[[NSFetchRequest alloc]init]autorelease];")
      f.println("\t\t[fetch${globalName}s setEntity:[NSEntityDescription entityForName:${globalName.toUpperCase()}_TABLE inManagedObjectContext:context]];")
      f.println('\t\t[fetch' +globalName +'s setPredicate:[NSPredicate predicateWithFormat:@"(LocalUpdate == YES)"]];')
      f.println("\t\tNSArray *${globalName.toLowerCase()}s = [context executeFetchRequest:fetch${globalName}s error:&error];")
      f.println('\t\t[Utils processError:error withMessage:@"Error fetching items from ' +globalName +' table. \\n" abort:YES delegate:self];')
      f.println()
    }
    def sBuff_1 = []
    def sBuff_2 = []
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      sBuff_1  << globalName.toLowerCase() +"s.count"
      sBuff_2  << "arrayByAddingObjectsFromArray:" +globalName.toLowerCase()
    }
    f.println("\t\t//TODO: Return the list of changes based on relationships")
    f.println("\t\t*listOfChanges = [" +sBuff_2.join(" ") +"]")
    f.println()
    f.println("\t\t[error release];")
    f.println("\t\treturn (" +sBuff_1.join(" + ") +") > 0;")
    f.println("\t}")
    f.println("\telse")
    f.println("\t{")
    f.println("\t\treturn false;")
    f.println("\t}")
    f.println("}")
    f.println()
    f.println("+ (void) cleanupCache:(NSManagedObjectContext*) context")
    f.println("{")
    f.println("\tNSError *error = nil;")
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      f.println("\t//Cleanup ${globalName}")
      f.println("\tNSFetchRequest *fetch${globalName} = [[[NSFetchRequest alloc] init] autorelease];")
      f.println("\t[fetch${globalName} setEntity:[NSEntityDescription entityForName:${globalName.toUpperCase()}_TABLE inManagedObjectContext:context]];")
      f.println("\tNSArray *${globalName.toLowerCase()}List = [context executeFetchRequest:fetch${globalName} error:&error];")
      f.println('\t[Utils processError:error withMessage:@"Error fetching items from ' +globalName +' table. \\n" abort:YES delegate:self];')
	  f.println('\tfor (id entity in tagList)')
	  f.println('\t{')
      f.println('\t\t[context deleteObject:entity];')
      f.println('\t}')
      f.println()
	}
    f.println()
    f.println("\t//Cleanup ClientMetadata")
	f.println("\tNSFetchRequest *fetchClientMetadata = [[[NSFetchRequest alloc] init] autorelease];")
	f.println("\t[fetchClientMetadata setEntity:[NSEntityDescription entityForName:CLIENT_METADATA_TABLE inManagedObjectContext:context]];")
	f.println("\tNSArray *clientMetadataList = [context executeFetchRequest:fetchClientMetadata error:&error];")
	f.println('\t[Utils processError:error withMessage:@"Error fetching items from Client Metadata table. \n" abort:YES delegate:self];')
	f.println("\tfor (id entity in clientMetadataList)")
	f.println("{")
    f.println("\t\t[context deleteObject:entity];")
	f.println("}")
	f.println()
    f.println("\t[Utils saveManagedObjects:context];")
    f.println("\t[error release];")
    f.println("}")
    f.println()
    f.println()
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      def tableName = t.'@Name'
      def Table table = defaultSchema.tables.find { tm -> tm.name.replace("\"", "").equalsIgnoreCase(tableName) }
      f.println("//Populates the ${globalName}Entity store with the values from the json object dict")
      f.println("+ (void)populate${globalName}: (id)dict withMetadata:(id)metadata withContext:(NSManagedObjectContext*) context;")
      f.println('{')
      f.println("\t${globalName}Entity *${globalName.toLowerCase()} = (${globalName}Entity*)[Utils populateOfflineEntity:dict withMetadata:metadata withContext:context];")
      f.println("\tif (${globalName.toLowerCase()} != nil)")
      f.println('\t{')
      table.columns.findAll { cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
        def columnName = c.name
        f.println("\t\t${globalName.toLowerCase()}.${columnName} = [dict valueForKey:@" +'"' +columnName +'"];')
      }
      f.println('\t}')
      f.println('}')
      f.println()
	}
    f.println()
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      def tableName = t.'@Name'
      def Table table = defaultSchema.tables.find { tm -> tm.name.replace("\"", "").equalsIgnoreCase(tableName) }
      f.println("//Delete the ${globalName}Entity from store")
      f.println("+ (void) delete${globalName}:(${globalName}Entity*)list inContext:(NSManagedObjectContext*)context")
      f.println('{')
      f.println("\tNSError *error = nil;")
      f.println("\tNSFetchRequest *fetch = [[[NSFetchRequest alloc] init]autorelease];")
      f.println("\t[fetch setEntity:[NSEntityDescription entityForName:${globalName.toUpperCase()}_TABLE inManagedObjectContext:context]];")
      f.println('[fetch setPredicate:[NSPredicate predicateWithFormat:@"' +globalName +'ID ==[cd] %@ AND IsTombstone == %@", [' +globalName.toLowerCase() +" ID], [NSNumber numberWithBool:NO]]];")
      f.println("\tNSArray *${globalName.toLowerCase()}Items = [context executeFetchRequest:fetch error:&error];")
      f.println('\t[Utils processError:error withMessage:@"Error fetching items from ' +globalName +'table. \n" abort:YES delegate:self];')
      f.println("\t[error release];")
      f.println("\t//TODO: Delete reference")
      f.println()
      f.println("\t//Save ${globalName.toLowerCase()}ID to create tombstone")
      f.println("\tNSString *${globalName.toLowerCase()}Id = [NSString stringWithString:[${globalName.toLowerCase()} ID]];")
      f.println("\tNSString *syncID = [NSString stringWithString:[list SyncID]];")
      f.println("\tNSString *type = [NSString stringWithString:[list EntityType]];")
      f.println("\tNSString *userID = [NSString stringWithString:[list UserID]];")
      f.println("\t//Delete list to trigger refresh")
      f.println("\t[context deleteObject:${globalName.toLowerCase()}];")
      f.println("""if ([Utils wasSynchronized:${globalName.toLowerCase()}])
	    {
		  // create tombstone to sync up
		  ${globalName}Entity *${globalName.toLowerCase()}Tombstone = [NSEntityDescription insertNewObjectForEntityForName:${globalName.toUpperCase()}_TABLE inManagedObjectContext:context];
		  ${globalName.toLowerCase()}Tombstone.ID = [NSString stringWithString:${globalName.toLowerCase()}Id];
		  ${globalName.toLowerCase()}Tombstone.LocalUpdate = [NSNumber numberWithBool:YES];
		  ${globalName.toLowerCase()}Tombstone.IsTombstone = [NSNumber numberWithBool:YES];""")
      table.columns.findAll { cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
        def columnName = c.name
        f.println("""\t\t${globalName.toLowerCase()}Tombstone.${columnName} = @"";""")
      }
      f.println("\t\t${globalName.toLowerCase()}Tombstone.SyncID = syncID;")
      f.println("\t\t${globalName.toLowerCase()}Tombstone.EntityType = type;")
      f.println("\t\t${globalName.toLowerCase()}Tombstone.UserID = userID;")")"
      f.println('\t}')
      f.println("\t// Save the context.")
      f.println("\t[Utils saveManagedObjects:context];")
      f.println('}')
      f.println()
	}
    f.println()
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      f.println("+ (NSString *) get${globalName}Name:(NSNumber *)${globalName.toLowerCase()}ID fromContext:(NSManagedObjectContext*)context")
      f.println("{")
      f.println("\treturn [Utils getName:${globalName.toLowerCase()}ID fromTable:${globalName.toUpperCase()}_TABLE inContext:context];")
      f.println("}")
      f.println()
    }
    f.println()
    e.SyncTables.SyncTable.each { t ->
      def globalName = t.'@GlobalName'
      f.println("+ (NSNumber *) get${globalName}ID:(NSString *)${globalName.toLowerCase()}Name fromContext:(NSManagedObjectContext*)context")
      f.println("{")
      f.println("\treturn [Utils getId:${globalName.toLowerCase()}Name fromTable:${globalName.toUpperCase()}_TABLE inContext:context];")
      f.println("}")
      f.println()
    }
    f.println()
    f.println("@end")
  }
}


