package importer;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import entity.ImportResult;


/**
 * 
 * @author Samarth Bhargav
 *
 */
public class Application
{
    public static void main( String[] args )
    {
        if ( args.length != 5 ) {
            System.err.println( "Usage: <HostName> <UserName> <Password> <DB Name> <Path To mongo_dump folder>" );
            System.exit( -1 );
        }
        String host = args[0];
        String username = args[1];
        String password = args[2];
        String dbName = args[3];
        String mongoDumpPath = args[4];


        File mongoDump = new File( mongoDumpPath );

        if ( !mongoDump.exists() ) {
            System.err.println( mongoDumpPath + " does not exist" );
            System.exit( -1 );
        }

        if ( !mongoDump.isDirectory() ) {
            System.err.println( mongoDumpPath + " is not a directory" );
            System.exit( -1 );
        }

        String[] files = mongoDump.list();
        System.out.println( "Found " + files.length + " files in directory" );

        String uri = "mongodb://";
        uri += username + ":" + password + "@";
        uri += host + ":" + String.valueOf( 27017 );
        MongoClientURI uriClient = new MongoClientURI( uri );
        try {
            MongoClient client = new MongoClient( uriClient );
            DB db = client.getDB( dbName );
            System.out.println( "Connected To Mongo Instance" );
            for ( String file : files ) {
                System.out.println( "Processing " + file );
                MongoImporter importer = new MongoImporter( db, file, mongoDump.getAbsolutePath() + "//" + file );
                ImportResult result = importer.doImport();
                System.out.println( "Processing " + file + " complete" );
                System.out.println( "Found " + result.getProcessed() + " records" );
                System.out.println( "Wrote " + result.getWritten() + " records" );
            }
            System.out.println( "Importing Process Done" );
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        } catch ( IOException e ) {
            e.printStackTrace();
        }

    }
}
