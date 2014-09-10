package importer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;

import entity.ImportResult;


public class MongoImporter
{
    private String path;
    private DBCollection collection;


    public MongoImporter( DB db, String collectionName, String path )
    {
        collection = db.getCollection( collectionName );
        this.path = path;
    }


    public ImportResult doImport() throws IOException
    {
        List<DBObject> objects = getAllDBObjectsFromFile( path );
        int inserted = 0;
        try {
            collection.insert( objects );
            inserted = objects.size();
        } catch ( DuplicateKeyException ex ) {
            for ( DBObject object : objects ) {
                try {
                    collection.insert( object );
                    System.out.print( "." );
                    inserted++;
                } catch ( DuplicateKeyException exception ) {
                    System.err.println( "Found Duplicate Key" );
                }
            }
        }

        ImportResult result = new ImportResult();
        result.setProcessed( objects.size() );
        result.setWritten( inserted );
        return result;
    }


    private List<DBObject> getAllDBObjectsFromFile( String path ) throws IOException
    {
        BufferedReader reader = new BufferedReader( new FileReader( path ) );
        String line = null;
        List<DBObject> objects = new ArrayList<DBObject>();
        while ( ( line = reader.readLine() ) != null ) {
            Object jsonObject = com.mongodb.util.JSON.parse( line );
            objects.add( (DBObject) jsonObject );
        }
        reader.close();
        return objects;
    }

}
