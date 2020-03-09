package main;
//import java.io.*;
import java.sql.*;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
import main.IniFile;

//import org.apache.commons.httpclient.HttpClient;
//import org.apache.commons.httpclient.methods.PostMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

//import com.opencsv.*;

public class MainProtocole {
	private static IniFile ini = new IniFile();
	//private static String urlContrat = "";
	//private static String filenameContrat = "";
	private static String requete = "";
	//private static String urlClient = "https://api-cc.dbcall.fr/v1/defouest/client/";
	
	public static void main(String[] args) {
		//connexion au serveur
		ConnectionManager cm = new ConnectionManager();
		Connection con = cm.getConnection();
		//connexion au serveur dbcall
		ConnectionManager cmDbcall = new ConnectionManager();
		Connection conDbcall = cm.getConnection();
		
		//Boolean includeHeaders = true;
		
		// recuperation des variables dans le fichier .ini
		//urlContrat = ini.getVariable("def","urlApi");
		//filenameContrat = ini.getVariable("def","nom_fichier_csv"); 
		
		
		
		//creation du fichier file
		/*
		File file = null;
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(filenameContrat), '\t');
			file = new File(filenameContrat);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		*/

		try{

			// Execution de la requete
			Statement jRequete = con.createStatement();
			String query = ini.getVariable("base","requete");
			ResultSet rs = jRequete.executeQuery(query);
			
			//creation du json
			JSONArray jsonArray = convert(rs);
			
			//insertion dans la bdd de Dbcall
			PreparedStatement pstmt = conDbcall.prepareStatement("INSERT into public.sites(flux) values (?)");
			pstmt.setObject(1, jsonArray);
			pstmt.executeUpdate();
			
			// ecriture du fichier file
			//writer.writeAll(rs, includeHeaders);
			//writer.close();
			
			//fermeture des connexion
			rs.close();
			jRequete.close();
			con.close();
			conDbcall.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		
		
		//envoie du fichier au serveur
		/*
		try{
			HttpClient client = new HttpClient();
	        PostMethod postMethod = new PostMethod(urlContrat);

	        client.setConnectionTimeout(8000);

	        postMethod.setRequestBody(new FileInputStream(file));
	        postMethod.setRequestHeader("Content-type",
	            "text/csv; charset=ISO-8859-1");

	        int statusCode1 = client.executeMethod(postMethod);

	        System.out.println("statusLine>>>" + postMethod.getStatusLine());
	        postMethod.releaseConnection();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		*/
		
		//FIN
		System.out.println("Fin du traitement du protocole");
		Thread.currentThread().interrupt();
	}
	
	public static JSONArray convert( ResultSet rs )
		    throws SQLException, JSONException
		  {
		    JSONArray json = new JSONArray();
		    ResultSetMetaData rsmd = rs.getMetaData();

		    while(rs.next()) {
		      int numColumns = rsmd.getColumnCount();
		      JSONObject obj = new JSONObject();

		      for (int i=1; i<numColumns+1; i++) {
		        String column_name = rsmd.getColumnName(i);

		        if(rsmd.getColumnType(i)==java.sql.Types.ARRAY){
		         obj.put(column_name, rs.getArray(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.BIGINT){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.BOOLEAN){
		         obj.put(column_name, rs.getBoolean(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.BLOB){
		         obj.put(column_name, rs.getBlob(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.DOUBLE){
		         obj.put(column_name, rs.getDouble(column_name)); 
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.FLOAT){
		         obj.put(column_name, rs.getFloat(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.INTEGER){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.NVARCHAR){
		         obj.put(column_name, rs.getNString(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.VARCHAR){
		         obj.put(column_name, rs.getString(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.TINYINT){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.SMALLINT){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.DATE){
		         obj.put(column_name, rs.getDate(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.TIMESTAMP){
		        obj.put(column_name, rs.getTimestamp(column_name));   
		        }
		        else{
		         obj.put(column_name, rs.getObject(column_name));
		        }
		      }
		      json.put(obj);
		    }
		    return json;
		  }
}