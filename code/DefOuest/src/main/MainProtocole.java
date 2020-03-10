/*package main;
import java.io.*;
import java.sql.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import main.IniFile;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;

import com.opencsv.*;

public class MainProtocole {
	private static IniFile ini = new IniFile();
	private static String urlContrat = "";
	private static String filenameContrat = "";
	private static String requete = "";
	//private static String urlClient = "https://api-cc.dbcall.fr/v1/defouest/client/";
	
	public static void main(String[] args) {
		//connexion au serveur
		ConnectionManager cm = new ConnectionManager();
		Connection con = cm.getConnection();
		Boolean includeHeaders = true;
		
		// recuperation des variables dans le fichier .ini
		urlContrat = ini.getVariable("def","urlApi");
		filenameContrat = ini.getVariable("def","nom_fichier_csv"); 
		requete = ini.getVariable("base","requete");
		
		
		//creation du fichier file
		File file = null;
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(filenameContrat), '\t');
			file = new File(filenameContrat);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try{

			// Execution de la requete
			Statement jRequete = con.createStatement();
			String query = requete;
			ResultSet rs = jRequete.executeQuery(query);
			
			// ecriture du fichier file
			writer.writeAll(rs, includeHeaders);
			writer.close();
			
			//fermeture des connexion
			rs.close();
			jRequete.close();
			con.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		//envoie du fichier au serveur
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
		
		//FIN
		System.out.println("Fin du traitement du protocole");
		Thread.currentThread().interrupt();
	}
}*/


package main;
import java.sql.*;
import main.IniFile;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MainProtocole {
	private static IniFile ini = new IniFile();
	
	public static void main(String[] args) {
		String tableName = "";
		String primarykey = "";
		
		//connexion au serveur dbcall
		ConnectionManagerDbcall cmDbcall = new ConnectionManagerDbcall();
		Connection conDbcall = cmDbcall.getConnection();
	
		//connexion au serveur
		ConnectionManager cm = new ConnectionManager();
		Connection con = cm.getConnection();
				

		try{
			// Execution de la requete
			Statement jRequete = con.createStatement();
			String query = ini.getVariable("base","requete");
			ResultSet rs = jRequete.executeQuery(query);
			
			//Recuperation de la cle primaire
			DatabaseMetaData dbmd = con.getMetaData();
			try (ResultSet tables = dbmd.getTables(null, null, "Contrat", new String[] { "TABLE" })) {
			    while (tables.next()) {
			        String catalog = tables.getString("TABLE_CAT");
			        String schema = tables.getString("TABLE_SCHEM");
			        String table = tables.getString("TABLE_NAME");
			        System.out.println("Table: " + table);
			        tableName=table;
			        try (ResultSet primaryKeys = dbmd.getPrimaryKeys(catalog, schema, table)) {
			            while (primaryKeys.next()) {
			                System.out.println("Primary key: " + primaryKeys.getString("COLUMN_NAME"));
			                primarykey = primaryKeys.getString("COLUMN_NAME");
			            }
			        }
			        catch(Exception e) {
						System.out.println("erreur : getPrimaryKeys");
						System.out.println(e.getMessage());
					}
			    }
			}
			catch(Exception e) {
				System.out.println("erreur : getTables");
				System.out.println(e.getMessage());
			}
			
			//Recuperation de la valeur max de la cle primaire
			try {
				String queryMax = "SELECT MAX("+primarykey+") as max FROM "+tableName;
				ResultSet rsMax = jRequete.executeQuery(queryMax);
				rsMax.next();
				System.out.println("MAX VALUE OF : "+primarykey+" , FROM TABLE : "+tableName+" , IS : "+rsMax.getString("max"));
			}catch(Exception e) {
				System.out.println("erreur : SELECT MAX VALUE");
				System.out.println(e.getMessage());
			}
			
			//envoie de la requete vers dbcall
			sendJsonToDbcall(rs,conDbcall);
			
			//fermeture des connexion
			rs.close();
			jRequete.close();
			con.close();
			conDbcall.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		
		
		//FIN
		System.out.println("Fin du traitement du protocole");
		Thread.currentThread().interrupt();
	}
	
	//Envoie un json vers la bdd Dbcall
	public static void sendJsonToDbcall( ResultSet rs, Connection con )
		    throws SQLException, JSONException
		  {
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
		    //insertion dans la bdd de Dbcall
		    PreparedStatement pstmt = con.prepareStatement("INSERT into public.sites(flux) values (?::JSON)");
			pstmt.setObject(1, obj.toString());
			pstmt.executeUpdate();
			//fermeture
			pstmt.close();
		    }
		  }
	
}