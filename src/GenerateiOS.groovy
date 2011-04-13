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
    defaultSchema  = s
}

defaultSchema.tables.each { t ->
    println(t.name)
}

syncScopes.SyncScope.each { e ->
    e.SyncTables.SyncTable.each { t ->
        def tableName = t.'@Name'
        def globalName = t.'@GlobalName'
        def allColumns = new Boolean(t.'@IncludeAllColumns')
        def Table table = defaultSchema.tables.find{ tm -> tm.name.replace("\"", "").equalsIgnoreCase(tableName) }
        println("Processing table: ${tableName}")
        new File(GENERATED_PATH +globalName +"Entity.h").withWriter { f ->
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
            table.columns.findAll{ cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
                def columnName = c.name
                def dataType = c.type.name
                if(dataType.equalsIgnoreCase("int")) dataType = "NSNumber"
                if(dataType.equalsIgnoreCase("nvarchar")) dataType = "NSString"
                if(dataType.equalsIgnoreCase("datetime")) dataType = "NSDate"
                f.println("@property (nonatomic, retain) ${dataType} * ${columnName};")
            }
            f.println()
            f.println("@end")
        }

        new File(GENERATED_PATH +globalName +"Entity.m").withWriter { f ->
            f.println("// Generated file. DO NOT MODIFY")
            f.println("// ${globalName}Entity.m")
            f.println()
            f.println('#import "' +globalName +'Entity.h"')
            f.println("@implementation ${globalName}Entity")
            f.println()
            table.columns.findAll{ cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
                def columnName = c.name
                def dataType = c.type.name
                if(dataType.equalsIgnoreCase("int")) dataType = "NSNumber"
                if(dataType.equalsIgnoreCase("nvarchar")) dataType = "NSString"
                if(dataType.equalsIgnoreCase("datetime")) dataType = "NSDate"
                f.println("@dynamic ${columnName};")
            }
            f.println()
            f.println("-(void) logEntity")
            f.println("{")
            table.columns.findAll{ cc -> !cc.type.name.equalsIgnoreCase("uniqueidentifier")}.each { c ->
                def columnName = c.name
                f.println('\tNSLog(@"%@ ' +columnName +': %@", self.EntityType, self.' +columnName +');')
            }
            f.println('\t[super logEntity];')
            f.println("}")
            f.println()
            f.println("@end")
        }
    }
}


